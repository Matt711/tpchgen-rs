#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle to the chunked writer
typedef void* ChunkedWriterHandle;

// Initialize RMM (RAPIDS Memory Manager) with a pool allocator
void cudf_initialize_rmm(size_t pool_size);

// Create a new chunked Parquet writer
// Returns an opaque handle that must be passed to subsequent calls
ChunkedWriterHandle cudf_create_chunked_writer(const char* output_path);

// Write a RecordBatch to the chunked writer using Arrow C Data Interface
// The arrow_array and arrow_schema pointers must point to valid ArrowArray and ArrowSchema
// structures following the Arrow C Data Interface specification
void cudf_write_chunk(
    ChunkedWriterHandle handle,
    const uint8_t* arrow_array,
    const uint8_t* arrow_schema
);

// Finalize and close the chunked writer
void cudf_close_writer(ChunkedWriterHandle handle);

#ifdef __cplusplus
}
#endif
