//! Parquet output format

use crate::statistics::WriteStatistics;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use log::debug;
use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnChunk};
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescPtr;
use std::io;
use std::io::Write;
use std::sync::Arc;

#[cfg(feature = "cudf")]
use std::path::Path;
use tokio::sync::mpsc::{Receiver, Sender};
use tpchgen_arrow::RecordBatchIterator;

#[cfg(feature = "cudf")]
use tpchgen_arrow::parquet::create_parquet_writer;

pub trait IntoSize {
    /// Convert the object into a size
    fn into_size(self) -> Result<usize, io::Error>;
}

/// Converts a set of RecordBatchIterators into a Parquet file
///
/// Uses num_threads to generate the data in parallel
///
/// Note the input is an iterator of [`RecordBatchIterator`]; The batches
/// produced by each iterator is encoded as its own row group.
pub async fn generate_parquet<W: Write + Send + IntoSize + 'static, I>(
    writer: W,
    iter_iter: I,
    num_threads: usize,
    parquet_compression: Compression,
) -> Result<(), io::Error>
where
    I: Iterator<Item: RecordBatchIterator> + 'static,
{
    debug!(
        "Generating Parquet with {num_threads} threads, using {parquet_compression} compression"
    );
    // Based on example in https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowColumnWriter.html
    let mut iter_iter = iter_iter.peekable();

    // get schema from the first iterator
    let Some(first_iter) = iter_iter.peek() else {
        return Ok(()); // no data shrug
    };
    let schema = Arc::clone(first_iter.schema());

    // Compute the parquet schema
    let writer_properties = WriterProperties::builder()
        .set_compression(parquet_compression)
        .build();
    let writer_properties = Arc::new(writer_properties);
    let parquet_schema = Arc::new(
        ArrowSchemaConverter::new()
            .with_coerce_types(writer_properties.coerce_types())
            .convert(&schema)
            .unwrap(),
    );

    // create a stream that computes the data for each row group
    let mut row_group_stream = futures::stream::iter(iter_iter)
        .map(async |iter| {
            let parquet_schema = Arc::clone(&parquet_schema);
            let writer_properties = Arc::clone(&writer_properties);
            let schema = Arc::clone(&schema);
            // run on a separate thread
            tokio::task::spawn(async move {
                encode_row_group(parquet_schema, writer_properties, schema, iter)
            })
            .await
            .expect("Inner task panicked")
        })
        .buffered(num_threads); // generate row groups in parallel

    let mut statistics = WriteStatistics::new("row groups");

    // A blocking task that writes the row groups to the file
    // done in a blocking task to avoid having a thread waiting on IO
    // Now, read each completed row group and write it to the file
    let root_schema = parquet_schema.root_schema_ptr();
    let writer_properties_captured = Arc::clone(&writer_properties);
    let (tx, mut rx): (
        Sender<Vec<ArrowColumnChunk>>,
        Receiver<Vec<ArrowColumnChunk>>,
    ) = tokio::sync::mpsc::channel(num_threads);
    let writer_task = tokio::task::spawn_blocking(move || {
        // Create parquet writer
        let mut writer =
            SerializedFileWriter::new(writer, root_schema, writer_properties_captured).unwrap();

        while let Some(chunks) = rx.blocking_recv() {
            // Start row group
            let mut row_group_writer = writer.next_row_group().unwrap();

            // Slap the chunks into the row group
            for chunk in chunks {
                chunk.append_to_row_group(&mut row_group_writer).unwrap();
            }
            row_group_writer.close().unwrap();
            statistics.increment_chunks(1);
        }
        let size = writer.into_inner()?.into_size()?;
        statistics.increment_bytes(size);
        Ok(()) as Result<(), io::Error>
    });

    // now, drive the input stream and send results to the writer task
    while let Some(chunks) = row_group_stream.next().await {
        // send the chunks to the writer task
        if let Err(e) = tx.send(chunks).await {
            debug!("Error sending chunks to writer: {e}");
            break; // stop early
        }
    }
    // signal the writer task that we are done
    drop(tx);

    // Wait for the writer task to finish
    writer_task.await??;

    Ok(())
}

/// Creates the data for a particular row group
///
/// Note at the moment it does not use multiple tasks/threads but it could
/// potentially encode multiple columns with different threads .
///
/// Returns an array of [`ArrowColumnChunk`]
fn encode_row_group<I>(
    parquet_schema: SchemaDescPtr,
    writer_properties: Arc<WriterProperties>,
    schema: SchemaRef,
    iter: I,
) -> Vec<ArrowColumnChunk>
where
    I: RecordBatchIterator,
{
    // Create writers for each of the leaf columns
    #[allow(deprecated)]
    let mut col_writers = parquet::arrow::arrow_writer::get_column_writers(
        &parquet_schema,
        &writer_properties,
        &schema,
    )
    .unwrap();

    // generate the data and send it to the tasks (via the sender channels)
    for batch in iter {
        let columns = batch.columns().iter();
        let col_writers = col_writers.iter_mut();
        let fields = schema.fields().iter();

        for ((col_writer, field), arr) in col_writers.zip(fields).zip(columns) {
            for leaves in compute_leaves(field.as_ref(), arr).unwrap() {
                col_writer.write(&leaves).unwrap();
            }
        }
    }
    // finish the writers and create the column chunks
    col_writers
        .into_iter()
        .map(|col_writer| col_writer.close().unwrap())
        .collect()
}

/// GPU-accelerated Parquet generation using libcudf
///
/// This function writes Parquet files using GPU acceleration via libcudf's
/// chunked writer. It generates data in parallel on CPU (like the CPU version)
/// then streams batches to GPU for writing.
///
/// # Note
/// Only available when the `cudf` feature is enabled.
#[cfg(feature = "cudf")]
pub async fn generate_parquet_gpu<I>(
    path: impl AsRef<Path>,
    iter_iter: I,
    num_threads: usize,
) -> Result<(), io::Error>
where
    I: Iterator<Item: RecordBatchIterator> + 'static,
{
    use arrow::array::RecordBatch;
    use tokio::sync::mpsc::{Receiver, Sender};

    use std::time::Instant;

    debug!("Generating Parquet using GPU (libcudf) with {num_threads} parallel data generation threads");
    let start_total = Instant::now();

    let mut iter_iter = iter_iter.peekable();

    // Get schema from the first iterator
    let Some(first_iter) = iter_iter.peek() else {
        return Ok(()); // no data
    };
    let schema = Arc::clone(first_iter.schema());

    // Create a channel for sending batches to the writer task
    // Use larger buffer to avoid blocking parallel generators
    let (tx, mut rx): (Sender<RecordBatch>, Receiver<RecordBatch>) =
        tokio::sync::mpsc::channel(num_threads * 2);

    // Spawn blocking task that owns the GPU writer
    let path_owned = path.as_ref().to_path_buf();
    let schema_clone = Arc::clone(&schema);
    let writer_task = tokio::task::spawn_blocking(move || {
        use std::time::Instant;

        let start_writer = Instant::now();
        // Create GPU writer with 24GB RMM pool (leave 8GB for system)
        let mut writer = create_parquet_writer(
            &path_owned,
            schema_clone,
            true, // use_gpu
            Some(24 * 1024 * 1024 * 1024),
        )?;
        let writer_init_time = start_writer.elapsed();
        debug!("GPU writer initialized in {writer_init_time:?}");

        // Write all batches we receive
        let mut write_count = 0;
        let start_writing = Instant::now();
        while let Some(batch) = rx.blocking_recv() {
            writer.write(&batch)?;
            write_count += 1;
        }
        let writing_time = start_writing.elapsed();
        debug!("Wrote {write_count} batches in {writing_time:?}");

        // Close the writer
        let start_close = Instant::now();
        Box::new(writer).close()?;
        let close_time = start_close.elapsed();
        debug!("GPU writer closed in {close_time:?}");

        Ok(()) as Result<(), io::Error>
    });

    // Generate batches in parallel and stream to GPU writer
    let start_gen = Instant::now();
    debug!("Starting parallel batch generation with {} threads", num_threads);

    let stream = futures::stream::iter(iter_iter.enumerate())
        .map(|(chunk_idx, iter)| {
            let tx = tx.clone();
            // Generate batches for each chunk in parallel
            tokio::task::spawn_blocking(move || {
                debug!("Chunk {} starting generation", chunk_idx);
                let mut count = 0;
                let mut rows = 0;
                for batch in iter {
                    rows += batch.num_rows();
                    count += 1;
                    if tx.blocking_send(batch).is_err() {
                        debug!("Chunk {} send failed", chunk_idx);
                        break;
                    }
                }
                debug!("Chunk {} completed: {} batches, {} rows", chunk_idx, count, rows);
                (count, rows)
            })
        })
        .buffered(num_threads); // Use buffered, not buffer_unordered

    // Drive the stream to completion
    let mut batch_count = 0;
    let mut total_rows = 0;
    {
        futures::pin_mut!(stream);
        while let Some(result) = stream.next().await {
            let (count, rows) = result.expect("Task panicked");
            batch_count += count;
            total_rows += rows;
        }
        debug!("Stream completed, dropping stream to release tx clones");
    } // stream is dropped here, releasing all tx clones

    let gen_time = start_gen.elapsed();
    debug!("Generated {batch_count} batches ({total_rows} rows) in {gen_time:?}");

    // Explicitly drop tx to signal writer we're done
    debug!("Dropping tx to signal EOF to writer");
    drop(tx);
    debug!("tx dropped, waiting for writer to complete");

    let start_write_wait = Instant::now();
    // Wait for writer task to finish
    writer_task.await??;
    let write_wait_time = start_write_wait.elapsed();

    let total_time = start_total.elapsed();
    debug!("GPU writing: generation={gen_time:?}, write_wait={write_wait_time:?}, total={total_time:?}");

    Ok(())
}
