//! TPCH data generation CLI with a dbgen compatible API.
//!
//! This crate provides a CLI for generating TPCH data and tries to remain close
//! API wise to the original dbgen tool, as in we use the same command line flags
//! and arguments.
//!
//! See the documentation on [`Cli`] for more information on the command line

// Use the library public API
use clap::builder::TypedValueParser;
use clap::Parser;
use log::{info, LevelFilter};
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use tpchgen_cli::{
    Compression, OutputFormat, Table, TpchGenerator, DEFAULT_PARQUET_ROW_GROUP_BYTES,
};

#[derive(Parser)]
#[command(name = "tpchgen")]
#[command(version)]
#[command(
    // -h output
    about = "TPC-H Data Generator",
    // --help output
    long_about = r#"
TPCH Data Generator (https://github.com/clflushopt/tpchgen-rs)

By default each table is written to a single file named <output_dir>/<table>.<format>

If `--part` option is specified, each table is written to a subdirectory in
multiple files named <output_dir>/<table>/<table>.<part>.<format>

Examples

# Generate all tables at scale factor 1 (1GB) in TBL format to /tmp/tpch directory:

tpchgen-cli tbl -s 1 --output-dir=/tmp/tpch

# Generate the lineitem table at scale factor 100 in 10 Apache Parquet files to
# /tmp/tpch/lineitem

tpchgen-cli parquet -s 100 --tables=lineitem --parts=10 --output-dir=/tmp/tpch

# Generate scale factor one in CSV format with tab delimiter

tpchgen-cli csv -s 1 --delimiter='\t'

# Generate scale factor one in current directory, seeing debug output

RUST_LOG=debug tpchgen-cli tbl -s 1
"#
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    deprecated: DeprecatedArgs,
}

#[derive(clap::Subcommand)]
enum Commands {
    Tbl(TblArgs),
    Csv(CsvArgs),
    Parquet(ParquetArgs),
}

#[derive(clap::Args)]
struct Args {
    /// Scale factor to create
    #[arg(short, long, default_value_t = 1.)]
    scale_factor: f64,

    /// Output directory for generated files (default: current directory)
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// Which tables to generate (default: all)
    #[arg(short = 'T', long = "tables", value_delimiter = ',', value_parser = TableValueParser)]
    tables: Option<Vec<Table>>,

    /// Number of part(itions) to generate. If not specified creates a single file per table
    #[arg(short, long)]
    parts: Option<i32>,

    /// Which part(ition) to generate (1-based). If not specified, generates all parts
    #[arg(long)]
    part: Option<i32>,

    /// The number of threads for parallel generation, defaults to the number of CPUs
    #[arg(short, long, default_value_t = num_cpus::get())]
    num_threads: usize,

    /// Verbose output
    ///
    /// When specified, sets the log level to `info` and ignores the `RUST_LOG`
    /// environment variable. When not specified, uses `RUST_LOG`
    #[arg(short, long, default_value_t = false, conflicts_with = "quiet")]
    verbose: bool,

    /// Quiet mode - only show error-level logs
    #[arg(short, long, default_value_t = false, conflicts_with = "verbose")]
    quiet: bool,

    /// Write the output to stdout instead of a file.
    #[arg(long, default_value_t = false)]
    stdout: bool,
}

#[derive(clap::Args)]
struct TblArgs {
    #[command(flatten)]
    common: Args,
}

#[derive(clap::Args)]
struct CsvArgs {
    #[command(flatten)]
    common: Args,

    /// CSV delimiter character (default: ',')
    ///
    /// Specifies the delimiter character to use when generating CSV files.
    ///
    /// Supports escape sequences: \t (tab), \n (newline), \r (carriage return), \\ (backslash)
    /// Common delimiters: ',' (comma), '|' (pipe), '\t' (tab), ';' (semicolon)
    #[arg(long, default_value = ",", value_parser = parse_delimiter)]
    delimiter: char,
}

#[derive(clap::Args)]
struct ParquetArgs {
    #[command(flatten)]
    common: Args,

    /// Parquet block compression format.
    ///
    /// Supported values: UNCOMPRESSED, ZSTD(N), SNAPPY, GZIP, LZO, BROTLI, LZ4
    ///
    /// Note to use zstd you must supply the "compression" level (1-22)
    /// as a number in parentheses, e.g. `ZSTD(1)` for level 1 compression.
    ///
    /// Using `ZSTD` results in the best compression, but is about 2x slower than
    /// UNCOMPRESSED. For example, for the lineitem table at SF=10
    ///
    ///   ZSTD(1):      1.9G  (0.52 GB/sec)
    ///   SNAPPY:       2.4G  (0.75 GB/sec)
    ///   UNCOMPRESSED: 3.8G  (1.41 GB/sec)
    #[arg(short = 'c', long, default_value = "SNAPPY")]
    compression: Compression,

    /// Target size in row group bytes in Parquet files
    ///
    /// Row groups are the typical unit of parallel processing and compression
    /// with many query engines. Therefore, smaller row groups enable better
    /// parallelism and lower peak memory use but may reduce compression
    /// efficiency.
    ///
    /// Note: Parquet files are limited to 32k row groups, so at high scale
    /// factors, the row group size may be increased to keep the number of row
    /// groups under this limit.
    ///
    /// Typical values range from 10MB to 100MB.
    #[arg(long, default_value_t = DEFAULT_PARQUET_ROW_GROUP_BYTES)]
    row_group_bytes: i64,
}

#[derive(clap::Args)]
struct DeprecatedArgs {
    /// Scale factor to create
    #[arg(short, long)]
    scale_factor: Option<f64>,

    /// Output directory for generated files (default: current directory)
    #[arg(short, long)]
    output_dir: Option<PathBuf>,

    /// Which tables to generate (default: all)
    #[arg(short = 'T', long = "tables", value_delimiter = ',', value_parser = TableValueParser)]
    tables: Option<Vec<Table>>,

    /// Number of part(itions) to generate. If not specified creates a single file per table
    #[arg(short, long)]
    parts: Option<i32>,

    /// Which part(ition) to generate (1-based). If not specified, generates all parts
    #[arg(long)]
    part: Option<i32>,

    /// Output format: tbl, csv, parquet (deprecated: use subcommands instead e.g. 'tpchgen-cli parquet ...')
    #[arg(short, long)]
    #[deprecated]
    format: Option<OutputFormat>,

    /// The number of threads for parallel generation, defaults to the number of CPUs
    #[arg(short, long)]
    num_threads: Option<usize>,

    /// Parquet block compression format (deprecated: use 'parquet' subcommand instead)
    #[arg(short = 'c', long)]
    #[deprecated]
    parquet_compression: Option<Compression>,

    /// Verbose output
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// Quiet mode - only show error-level logs
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// Write the output to stdout instead of a file.
    #[arg(long, default_value_t = false)]
    stdout: bool,

    /// Target row group size in bytes (deprecated: use 'parquet' subcommand instead)
    #[arg(long)]
    #[deprecated]
    parquet_row_group_bytes: Option<i64>,

    /// CSV delimiter character (deprecated: use 'csv' subcommand instead)
    #[arg(long, value_parser = parse_delimiter)]
    #[deprecated]
    delimiter: Option<char>,
}

/// Parse a delimiter string, handling escape sequences
fn parse_delimiter(s: &str) -> Result<char, String> {
    // Handle common escape sequences
    let parsed = match s {
        "\\t" => '\t',
        "\\n" => '\n',
        "\\r" => '\r',
        "\\\\" => '\\',
        _ => {
            // If it's not an escape sequence, it should be a single character
            let chars: Vec<char> = s.chars().collect();
            if chars.len() != 1 {
                return Err(format!(
                    "Delimiter must be a single character or escape sequence (\\t, \\n, \\r, \\\\), got: '{}'",
                    s
                ));
            }
            chars[0]
        }
    };
    Ok(parsed)
}

// TableValueParser is CLI-specific and uses the Table type from the library
#[derive(Debug, Clone)]
struct TableValueParser;

impl TypedValueParser for TableValueParser {
    type Value = Table;

    /// Parse the value into a Table enum.
    fn parse_ref(
        &self,
        cmd: &clap::Command,
        _: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let value = value
            .to_str()
            .ok_or_else(|| clap::Error::new(clap::error::ErrorKind::InvalidValue).with_cmd(cmd))?;
        Table::from_str(value)
            .map_err(|_| clap::Error::new(clap::error::ErrorKind::InvalidValue).with_cmd(cmd))
    }

    fn possible_values(
        &self,
    ) -> Option<Box<dyn Iterator<Item = clap::builder::PossibleValue> + '_>> {
        Some(Box::new(
            [
                clap::builder::PossibleValue::new("region").help("Region table (alias: r)"),
                clap::builder::PossibleValue::new("nation").help("Nation table (alias: n)"),
                clap::builder::PossibleValue::new("supplier").help("Supplier table (alias: s)"),
                clap::builder::PossibleValue::new("customer").help("Customer table (alias: c)"),
                clap::builder::PossibleValue::new("part").help("Part table (alias: P)"),
                clap::builder::PossibleValue::new("partsupp").help("PartSupp table (alias: S)"),
                clap::builder::PossibleValue::new("orders").help("Orders table (alias: O)"),
                clap::builder::PossibleValue::new("lineitem").help("LineItem table (alias: L)"),
            ]
            .into_iter(),
        ))
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();
    cli.main().await
}

impl Cli {
    async fn main(self) -> io::Result<()> {
        match self.command {
            Some(Commands::Tbl(args)) => args.run().await,
            Some(Commands::Csv(args)) => args.run().await,
            Some(Commands::Parquet(args)) => args.run().await,
            None => self.run().await,
        }
    }

    #[allow(deprecated)]
    async fn run(self) -> io::Result<()> {
        let format = self.deprecated.format.unwrap_or(OutputFormat::Tbl);
        let scale_factor = self.deprecated.scale_factor.unwrap_or(1.);
        let output_dir = self
            .deprecated
            .output_dir
            .unwrap_or_else(|| PathBuf::from("."));
        let num_threads = self.deprecated.num_threads.unwrap_or_else(num_cpus::get);
        let verbose = self.deprecated.verbose;
        let quiet = self.deprecated.quiet;
        let stdout = self.deprecated.stdout;
        let parquet_compression = self
            .deprecated
            .parquet_compression
            .unwrap_or(Compression::SNAPPY);
        let parquet_row_group_bytes = self
            .deprecated
            .parquet_row_group_bytes
            .unwrap_or(DEFAULT_PARQUET_ROW_GROUP_BYTES);
        let delimiter = self.deprecated.delimiter.unwrap_or(',');

        configure_logging(verbose, quiet);

        if self.deprecated.format.is_some() {
            eprintln!(
                "Warning: The --format flag is deprecated. Use subcommands instead: 'tpchgen {}' instead of 'tpchgen --format={}'",
                format, format
            );
        }

        if self.deprecated.parquet_compression.is_some() && format != OutputFormat::Parquet {
            log::warn!("Parquet compression option set but not generating Parquet files");
        } else if self.deprecated.parquet_compression.is_some() {
            eprintln!(
                "Warning: The --parquet-compression flag is deprecated. Use 'tpchgen parquet --compression=...' instead"
            );
        }

        if self.deprecated.parquet_row_group_bytes.is_some() && format != OutputFormat::Parquet {
            log::warn!("Parquet row group size option set but not generating Parquet files");
        } else if self.deprecated.parquet_row_group_bytes.is_some() {
            eprintln!(
                "Warning: The --parquet-row-group-bytes flag is deprecated. Use 'tpchgen parquet --row-group-bytes=...' instead"
            );
        }

        if format == OutputFormat::Tbl && delimiter != ',' {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The --delimiter option cannot be used with --format=tbl. TBL format uses the TPC-H standard pipe delimiter."
            ));
        }

        if self.deprecated.delimiter.is_some() && format != OutputFormat::Csv {
            eprintln!("Warning: Delimiter option set but not generating CSV files");
        } else if self.deprecated.delimiter.is_some() {
            eprintln!(
                "Warning: The --delimiter flag is deprecated. Use 'tpchgen csv --delimiter=...' instead"
            );
        }

        let mut builder = TpchGenerator::builder()
            .with_scale_factor(scale_factor)
            .with_output_dir(output_dir)
            .with_format(format)
            .with_num_threads(num_threads)
            .with_parquet_compression(parquet_compression)
            .with_parquet_row_group_bytes(parquet_row_group_bytes)
            .with_stdout(stdout)
            .with_csv_delimiter(delimiter);

        if let Some(tables) = self.deprecated.tables {
            builder = builder.with_tables(tables);
        }

        if let Some(parts) = self.deprecated.parts {
            builder = builder.with_parts(parts);
        }
        if let Some(part) = self.deprecated.part {
            builder = builder.with_part(part);
        }

        builder.build().generate().await
    }
}

impl TblArgs {
    async fn run(self) -> io::Result<()> {
        configure_logging(self.common.verbose, self.common.quiet);

        let mut builder = TpchGenerator::builder()
            .with_scale_factor(self.common.scale_factor)
            .with_output_dir(self.common.output_dir)
            .with_format(OutputFormat::Tbl)
            .with_num_threads(self.common.num_threads)
            .with_stdout(self.common.stdout);

        if let Some(tables) = self.common.tables {
            builder = builder.with_tables(tables);
        }

        if let Some(parts) = self.common.parts {
            builder = builder.with_parts(parts);
        }
        if let Some(part) = self.common.part {
            builder = builder.with_part(part);
        }

        builder.build().generate().await
    }
}

impl CsvArgs {
    async fn run(self) -> io::Result<()> {
        configure_logging(self.common.verbose, self.common.quiet);

        let mut builder = TpchGenerator::builder()
            .with_scale_factor(self.common.scale_factor)
            .with_output_dir(self.common.output_dir)
            .with_format(OutputFormat::Csv)
            .with_num_threads(self.common.num_threads)
            .with_stdout(self.common.stdout)
            .with_csv_delimiter(self.delimiter);

        if let Some(tables) = self.common.tables {
            builder = builder.with_tables(tables);
        }

        if let Some(parts) = self.common.parts {
            builder = builder.with_parts(parts);
        }
        if let Some(part) = self.common.part {
            builder = builder.with_part(part);
        }

        builder.build().generate().await
    }
}

impl ParquetArgs {
    async fn run(self) -> io::Result<()> {
        configure_logging(self.common.verbose, self.common.quiet);

        let mut builder = TpchGenerator::builder()
            .with_scale_factor(self.common.scale_factor)
            .with_output_dir(self.common.output_dir)
            .with_format(OutputFormat::Parquet)
            .with_num_threads(self.common.num_threads)
            .with_stdout(self.common.stdout)
            .with_parquet_compression(self.compression)
            .with_parquet_row_group_bytes(self.row_group_bytes);

        if let Some(tables) = self.common.tables {
            builder = builder.with_tables(tables);
        }

        if let Some(parts) = self.common.parts {
            builder = builder.with_parts(parts);
        }
        if let Some(part) = self.common.part {
            builder = builder.with_part(part);
        }

        builder.build().generate().await
    }
}

fn configure_logging(verbose: bool, quiet: bool) {
    if quiet {
        env_logger::builder()
            .filter_level(LevelFilter::Error)
            .init();
    } else if verbose {
        env_logger::builder().filter_level(LevelFilter::Info).init();
        info!("Verbose output enabled (ignoring RUST_LOG environment variable)");
    } else {
        env_logger::builder()
            .filter_level(LevelFilter::Warn)
            .parse_default_env()
            .init();
    }
}
