//! Parquet writing support for TPC-H data
//!
//! This module provides both CPU and GPU-accelerated Parquet writing.
//! Enable the `cudf` feature to use GPU acceleration via libcudf.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::path::Path;

#[cfg(feature = "cudf")]
use arrow::array::{Array, StructArray};
#[cfg(feature = "cudf")]
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
#[cfg(feature = "cudf")]
use std::ffi::CString;

/// Trait for writing TPC-H data to Parquet files
pub trait ParquetWriter {
    /// Write a RecordBatch to the Parquet file
    fn write(&mut self, batch: &RecordBatch) -> std::io::Result<()>;

    /// Finalize and close the Parquet file
    fn close(self: Box<Self>) -> std::io::Result<()>;

    /// Get a description of this writer (CPU or GPU)
    fn description(&self) -> &str;
}

/// CPU-based Parquet writer using arrow-rs
pub struct CpuParquetWriter {
    writer: parquet::arrow::ArrowWriter<std::fs::File>,
}

impl CpuParquetWriter {
    /// Create a new CPU-based Parquet writer
    pub fn new(path: impl AsRef<Path>, schema: SchemaRef) -> std::io::Result<Self> {
        use parquet::file::properties::WriterProperties;

        let file = std::fs::File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let writer = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(Self { writer })
    }
}

impl ParquetWriter for CpuParquetWriter {
    fn write(&mut self, batch: &RecordBatch) -> std::io::Result<()> {
        self.writer
            .write(batch)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn close(self: Box<Self>) -> std::io::Result<()> {
        let writer = self.writer;
        writer
            .close()
            .map(|_| ()) // Ignore the ParquetMetaData return value
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn description(&self) -> &str {
        "CPU (arrow-rs)"
    }
}

#[cfg(feature = "cudf")]
mod gpu {
    use super::*;

    // FFI declarations for C++ cuDF functions
    unsafe extern "C" {
        fn cudf_initialize_rmm(pool_size: usize);
        fn cudf_create_chunked_writer(
            output_path: *const std::os::raw::c_char,
        ) -> *mut std::os::raw::c_void;
        fn cudf_write_chunk(
            handle: *mut std::os::raw::c_void,
            arrow_array: *const u8,
            arrow_schema: *const u8,
        );
        fn cudf_close_writer(handle: *mut std::os::raw::c_void);
    }

    /// GPU-accelerated Parquet writer using libcudf
    pub struct GpuParquetWriter {
        handle: *mut std::os::raw::c_void,
        _path: String,
        initialized_rmm: bool,
    }

    impl GpuParquetWriter {
        /// Create a new GPU-accelerated Parquet writer
        ///
        /// # Arguments
        /// * `path` - Output path for the Parquet file
        /// * `rmm_pool_size` - Size of RMM memory pool in bytes (default: 2GB if None)
        pub fn new(
            path: impl AsRef<Path>,
            rmm_pool_size: Option<usize>,
        ) -> std::io::Result<Self> {
            let path_str = path.as_ref().to_string_lossy().to_string();

            // Initialize RMM (only once per process)
            static INIT: std::sync::Once = std::sync::Once::new();
            let mut initialized_rmm = false;
            INIT.call_once(|| {
                let pool_size = rmm_pool_size.unwrap_or(2 * 1024 * 1024 * 1024); // 2GB default
                unsafe {
                    cudf_initialize_rmm(pool_size);
                }
                initialized_rmm = true;
            });

            // Create chunked writer
            let c_path = CString::new(path_str.as_str())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

            let handle = unsafe { cudf_create_chunked_writer(c_path.as_ptr()) };

            if handle.is_null() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to create GPU writer",
                ));
            }

            Ok(Self {
                handle,
                _path: path_str,
                initialized_rmm,
            })
        }
    }

    impl ParquetWriter for GpuParquetWriter {
        fn write(&mut self, batch: &RecordBatch) -> std::io::Result<()> {
            // Convert RecordBatch to StructArray (represents the batch as a single array)
            let struct_array = StructArray::from(batch.clone());

            // Export to Arrow C Data Interface
            let ffi_array = FFI_ArrowArray::new(&struct_array.into_data());
            let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            // Pass to C++ (pointers to the FFI structs)
            unsafe {
                cudf_write_chunk(
                    self.handle,
                    &ffi_array as *const FFI_ArrowArray as *const u8,
                    &ffi_schema as *const FFI_ArrowSchema as *const u8,
                );
            }

            Ok(())
        }

        fn close(mut self: Box<Self>) -> std::io::Result<()> {
            if !self.handle.is_null() {
                unsafe {
                    cudf_close_writer(self.handle);
                }
                self.handle = std::ptr::null_mut(); // Prevent double-free in Drop
            }
            Ok(())
        }

        fn description(&self) -> &str {
            "GPU (libcudf)"
        }
    }

    impl Drop for GpuParquetWriter {
        fn drop(&mut self) {
            // Only close if handle wasn't already closed
            if !self.handle.is_null() {
                unsafe {
                    cudf_close_writer(self.handle);
                }
                self.handle = std::ptr::null_mut();
            }
        }
    }
}

#[cfg(feature = "cudf")]
pub use gpu::GpuParquetWriter;

/// Helper function to create a Parquet writer
///
/// Returns GPU writer if `cudf` feature is enabled and `use_gpu` is true,
/// otherwise returns CPU writer.
pub fn create_parquet_writer(
    path: impl AsRef<Path>,
    schema: SchemaRef,
    #[allow(unused_variables)] use_gpu: bool,
    #[allow(unused_variables)] rmm_pool_size: Option<usize>,
) -> std::io::Result<Box<dyn ParquetWriter>> {
    #[cfg(feature = "cudf")]
    {
        if use_gpu {
            return Ok(Box::new(GpuParquetWriter::new(path, rmm_pool_size)?));
        }
    }

    Ok(Box::new(CpuParquetWriter::new(path, schema)?))
}
