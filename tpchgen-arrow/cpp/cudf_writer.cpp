#include "cudf_writer.h"
#include <cudf/io/parquet.hpp>
#include <cudf/interop.hpp>
#include <cudf/table/table_view.hpp>
#include <iostream>
#include <memory>
#include <string>

// Arrow C Data Interface structures (standard C ABI)
// https://arrow.apache.org/docs/format/CDataInterface.html
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

// Internal C++ struct to hold writer state
struct WriterState {
    std::unique_ptr<cudf::io::chunked_parquet_writer> writer;
    std::string output_path;
};

extern "C" {

void cudf_initialize_rmm(size_t pool_size) {
    static bool initialized = false;
    if (initialized) {
        return;
    }

    // Use default RMM configuration
    // In a production setting, you would configure RMM pooling here
    // For this benchmark, the default memory resource is sufficient

    initialized = true;
    std::cout << "Using default GPU memory resource (requested pool size: "
              << (pool_size / (1024 * 1024)) << " MB)" << std::endl;
}

ChunkedWriterHandle cudf_create_chunked_writer(const char* output_path) {
    try {
        auto* state = new WriterState();
        state->output_path = std::string(output_path);

        // Create sink info for the output file
        auto sink = cudf::io::sink_info(state->output_path);

        // Create writer options with Snappy compression
        auto options = cudf::io::chunked_parquet_writer_options::builder(sink)
            .compression(cudf::io::compression_type::SNAPPY)
            .build();

        // Create the chunked writer
        state->writer = std::make_unique<cudf::io::chunked_parquet_writer>(options);

        std::cout << "Created chunked Parquet writer for: " << state->output_path << std::endl;

        return static_cast<ChunkedWriterHandle>(state);
    } catch (const std::exception& e) {
        std::cerr << "Failed to create chunked writer: " << e.what() << std::endl;
        return nullptr;
    }
}

void cudf_write_chunk(
    ChunkedWriterHandle handle,
    const uint8_t* arrow_array,
    const uint8_t* arrow_schema
) {
    if (!handle) {
        std::cerr << "Invalid writer handle" << std::endl;
        return;
    }

    try {
        auto* state = static_cast<WriterState*>(handle);

        // Cast to Arrow C Data Interface types
        const auto* c_array = reinterpret_cast<const ArrowArray*>(arrow_array);
        const auto* c_schema = reinterpret_cast<const ArrowSchema*>(arrow_schema);

        // Convert Arrow C Data to cudf table
        auto table = cudf::from_arrow(c_schema, c_array);

        // Write the chunk
        state->writer->write(*table);

    } catch (const std::exception& e) {
        std::cerr << "Failed to write chunk: " << e.what() << std::endl;
    }
}

void cudf_close_writer(ChunkedWriterHandle handle) {
    if (!handle) {
        return;
    }

    try {
        auto* state = static_cast<WriterState*>(handle);

        if (state->writer) {
            state->writer->close();
            std::cout << "Closed chunked writer for: " << state->output_path << std::endl;
        }

        delete state;
    } catch (const std::exception& e) {
        std::cerr << "Failed to close writer: " << e.what() << std::endl;
    }
}

} // extern "C"
