//! Random sub-sampling of CSV files and PostgreSQL databases.
//!
//! `remnant` reads data from a CSV file or a PostgreSQL database, samples a
//! percentage of rows, and writes the result as CSV or Parquet. When sampling
//! a PostgreSQL database it also generates a `rebuild.sql` script that
//! recreates the schema and loads the sampled data.
//!
//! # Library usage
//!
//! ## CSV sampling
//!
//! ```no_run
//! remnant::csv::run("input.csv", "output.csv", 10.0, Some(10_000), Some(42)).unwrap();
//! ```
//!
//! ## PostgreSQL sampling
//!
//! ```no_run
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use remnant::pg::OutputFormat;
//! remnant::pg::run(
//!     "postgres://user:pass@localhost/mydb",
//!     "./output",
//!     10.0,
//!     Some(42),
//!     OutputFormat::Csv,
//! ).await.unwrap();
//! # });
//! ```
//!
//! # Modules
//!
//! - [`csv`] — Sample rows from a CSV file and write a CSV output.
//! - [`pg`] — Sample every table in a PostgreSQL database, write CSV or Parquet
//!   files, and generate a SQL rebuild script.
//! - [`sampling`] — Core sampling logic operating on Polars DataFrames.
//!
//! # CLI
//!
//! `remnant` is also a command-line tool with `csv` and `pg` subcommands.
//! See the [README](https://github.com/ecolumix/csv-sampler) for CLI usage.

pub mod csv;
pub mod pg;
pub mod sampling;
