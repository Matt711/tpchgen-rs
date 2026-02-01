//! Example: Write TPC-H LineItem data to Parquet using GPU acceleration
//!
//! Run with: cargo run --example write_parquet_gpu --features cudf

use tpchgen::generators::LineItemGenerator;
use tpchgen_arrow::{LineItemArrow, RecordBatchIterator};
use tpchgen_arrow::parquet::{create_parquet_writer, ParquetWriter};
use std::time::Instant;

fn main() -> std::io::Result<()> {
    println!("===========================================");
    println!("TPC-H LineItem to Parquet (GPU Example)");
    println!("===========================================\n");

    let scale_factor = 1.0;
    let batch_size = 1_000_000; // 1M rows per batch for optimal GPU performance
    let output_path = "/tmp/lineitem_gpu_example.parquet";

    println!("Generating TPC-H LineItem data (SF={}, batch_size={})...",
             scale_factor, batch_size);

    // Create generator
    let generator = LineItemGenerator::new(scale_factor, 1, 1);
    let mut arrow_generator = LineItemArrow::new(generator)
        .with_batch_size(batch_size);

    // Create GPU writer (or CPU if cudf feature not enabled)
    #[cfg(feature = "cudf")]
    println!("Using GPU-accelerated writer (libcudf)");
    #[cfg(not(feature = "cudf"))]
    println!("Using CPU writer (arrow-rs)");

    let schema = arrow_generator.schema().clone();
    let mut writer = create_parquet_writer(
        output_path,
        schema,
        true, // use_gpu
        Some(2 * 1024 * 1024 * 1024), // 2GB RMM pool
    )?;

    println!("Writer: {}", writer.description());
    println!("Writing to: {}\n", output_path);

    let start = Instant::now();
    let mut batch_count = 0;
    let mut row_count = 0;

    while let Some(batch) = arrow_generator.next() {
        row_count += batch.num_rows();
        batch_count += 1;
        writer.write(&batch)?;

        if batch_count % 5 == 0 {
            println!("  Wrote batch {}", batch_count);
        }
    }

    let write_duration = start.elapsed();

    println!("\nClosing writer...");
    Box::new(writer).close()?;

    let total_duration = start.elapsed();

    println!("\n===========================================");
    println!("RESULTS");
    println!("===========================================");
    println!("Batches written: {}", batch_count);
    println!("Total rows: {}", row_count);
    println!("Write time: {:.2}s", write_duration.as_secs_f64());
    println!("Total time: {:.2}s", total_duration.as_secs_f64());
    println!("Output: {}", output_path);
    println!("===========================================");

    Ok(())
}
