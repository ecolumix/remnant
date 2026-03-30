# remnant

## Summary

A command line tool for creating random sub-samples of data. remnant can sample rows from CSV files or from all tables in a PostgreSQL database, outputting the results as CSV or Parquet files. When sampling a database, it also generates a SQL rebuild script to recreate the schema and load the sampled data.

## Installation

```
cargo build --release
```

## Usage

remnant has two subcommands: `csv` for sampling CSV files and `pg` for sampling PostgreSQL databases.

### CSV Sampling

Sample a percentage of rows from a CSV file:

```
remnant csv --file <FILE> --outfile <OUTFILE> [OPTIONS]
```

**Options:**

```
-f, --file <FILE>                Input CSV file (required)
-o, --outfile <OUTFILE>          Output CSV file (required)
-p, --percent <PERCENT>          Percentage of rows to sample [default: 10]
-m, --max-records <MAX_RECORDS>  Records for schema inference [default: 10000]
-s, --seed <SEED>                Random seed for reproducibility
```

**Example:**

```bash
remnant csv -f data/large_dataset.csv -o data/sample.csv -p 5 -s 42
```

### PostgreSQL Database Sampling

Sample a percentage of rows from every table in a PostgreSQL database:

```
remnant pg --connection-string <CONNECTION_STRING> --outdir <OUTDIR> [OPTIONS]
```

**Options:**

```
-c, --connection-string <CONNECTION_STRING>  PostgreSQL connection string (required)
-o, --outdir <OUTDIR>                        Output directory for data files (required)
-p, --percent <PERCENT>                      Percentage of rows to sample [default: 10]
-s, --seed <SEED>                            Random seed for reproducibility
-f, --format <FORMAT>                        Output format: csv or parquet [default: csv]
```

**Example:**

```bash
# Sample 10% of each table as CSV files
remnant pg -c "host=localhost user=postgres password=secret dbname=mydb" -o ./sample_output -p 10

# Sample 5% as Parquet files with a fixed seed
remnant pg -c "postgres://postgres:secret@localhost/mydb" -o ./sample_output -p 5 -s 42 -f parquet
```

**Output:** The output directory will contain one data file per table (e.g., `users.csv`, `orders.csv`) plus a `rebuild.sql` script.

### Rebuild Script

When using the `pg` subcommand, remnant automatically generates a `rebuild.sql` file in the output directory. This script contains everything needed to recreate the sampled database:

1. `CREATE TABLE` statements with original column types, constraints, and defaults
2. `\COPY` commands to load data from the CSV files
3. Foreign key constraints (added after data load to avoid ordering issues)
4. Index creation statements

To rebuild the database from the sampled data:

```bash
psql -d target_database -f sample_output/rebuild.sql
```

Note: Parquet format output includes comments in place of `\COPY` commands, since PostgreSQL does not natively support Parquet import.

## Project Structure

```
src/
  main.rs         -- CLI argument parsing and subcommand dispatch
  lib.rs          -- Library crate root
  sampling.rs     -- Shared sampling logic (sample_df)
  csv.rs          -- CSV file subsetting
  pg.rs           -- PostgreSQL subsetting, schema introspection, and SQL generation
```

## Running Tests

### Unit Tests

```
cargo test
```

Unit tests cover sampling logic, CSV round-trips, type mapping, SQL generation,
seed conversion, row count calculation, and file output (CSV/Parquet). No database
required.

### Integration Tests (PostgreSQL)

Integration tests require a running PostgreSQL instance. Set `TEST_DATABASE_URL`
and enable the `integration` feature:

```bash
export TEST_DATABASE_URL="postgres://user:pass@localhost/remnant_test"
cargo test --features integration
```

These tests create and drop temporary tables (prefixed with `remnant_test_`)
automatically. Use a dedicated test database to avoid interfering with other data.
