//! CSV file sampling.
//!
//! Reads a CSV file into a Polars DataFrame, samples a percentage of rows via
//! [`sampling::sample_df`], and writes the result
//! as a new CSV file.

use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

use crate::sampling;

/// Sample a percentage of rows from a CSV file and write the result to a new CSV.
///
/// Reads `file` into a Polars DataFrame, samples `percent`% of the rows
/// (optionally with a reproducible `seed`), and writes the result to `outfile`.
///
/// # Arguments
///
/// * `file` — Path to the input CSV file.
/// * `outfile` — Path for the output CSV file (created or overwritten).
/// * `percent` — Percentage of rows to keep (e.g. `10.0` for 10%).
/// * `max_records` — Maximum rows Polars examines to infer column types.
///   This does **not** limit the number of rows read from the file.
/// * `seed` — Optional random seed for reproducible sampling.
///
/// # Errors
///
/// Returns an error if the input file cannot be read or the CSV is malformed.
pub fn run(
    file: &str,
    outfile: &str,
    percent: f64,
    max_records: Option<usize>,
    seed: Option<u64>,
) -> Result<()> {
    let df = CsvReader::from_path(file)?
        .infer_schema(max_records)
        .has_header(true)
        .finish()?;

    let mut sampled_df = sampling::sample_df(&df, percent, seed)?;

    let mut out = File::create(outfile).expect("Could not create output file");
    let _ = CsvWriter::new(&mut out)
        .has_header(true)
        .with_separator(b',')
        .finish(&mut sampled_df);

    println!("sample_df dimensions: {:?}", sampled_df.shape());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_csv_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Write a small CSV
        std::fs::write(&input_path, "id,name\n1,alice\n2,bob\n3,charlie\n4,dave\n5,eve\n6,frank\n7,grace\n8,heidi\n9,ivan\n10,judy\n").unwrap();

        run(
            input_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
            50.0,
            Some(100),
            Some(42),
        )
        .unwrap();

        let mut contents = String::new();
        File::open(&output_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        // Header + 5 data rows (50% of 10)
        let line_count = contents.lines().count();
        assert_eq!(line_count, 6); // 1 header + 5 rows
    }
}
