use std::env;

fn main() {
    // Only link cuDF libraries when the cudf feature is enabled
    if env::var("CARGO_FEATURE_CUDF").is_ok() {
        println!("cargo:rerun-if-changed=cpp/cudf_writer.cpp");
        println!("cargo:rerun-if-changed=cpp/cudf_writer.h");

        // Path to pre-built wrapper library in cudf examples
        let wrapper_lib = "/home/coder/cudf/cpp/examples/rust_ffi_wrapper/build";

        // cudf build directory
        let cudf_build_dir = "/home/coder/cudf/cpp/build";

        // Conda environment
        let conda_lib = "/home/coder/.conda/envs/rapids/lib";
        let cuda_lib = "/home/coder/.conda/envs/rapids/targets/x86_64-linux/lib";

        // Add library search paths
        println!("cargo:rustc-link-search=native={}", wrapper_lib);
        println!("cargo:rustc-link-search=native={}", cudf_build_dir);
        println!("cargo:rustc-link-search=native={}", conda_lib);
        println!("cargo:rustc-link-search=native={}", cuda_lib);

        // Link our wrapper library (this will pull in cudf transitively via RPATH)
        println!("cargo:rustc-link-lib=dylib=cudf_parquet_writer");

        // Link cudf and CUDA explicitly
        println!("cargo:rustc-link-lib=dylib=cudf");
        println!("cargo:rustc-link-lib=dylib=cudart");

        // Set rpath for runtime linking
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", wrapper_lib);
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cudf_build_dir);
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", conda_lib);
    }
}
