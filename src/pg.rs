//! PostgreSQL database sampling and schema introspection.
//!
//! Connects to a PostgreSQL database, enumerates all non-system schemas and
//! their tables, and samples a configurable percentage of rows from each table
//! concurrently. Sampled data is written as CSV or Parquet files (one per
//! table), organised into per-schema subdirectories.
//!
//! In addition to the data files, a `rebuild.sql` script is generated that
//! contains `CREATE TABLE` statements, `\COPY` commands to reload the data,
//! foreign key constraints (added after data load), and index definitions —
//! all wrapped in a transaction.

use anyhow::{Context, Result};
use polars::prelude::*;
use sqlx::postgres::PgPool;
use sqlx::{Column, Executor, Row, TypeInfo};
use std::collections::BTreeMap;
use std::fs::{self, File};
use tokio::task::JoinSet;

/// Output file format for sampled database tables.
///
/// Determines the file extension and serialisation format used when writing
/// sampled data. The generated `rebuild.sql` script adjusts its load commands
/// based on this choice.
#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    /// Comma-separated values — compatible with PostgreSQL `\COPY … FROM` for
    /// rebuild.
    Csv,
    /// Apache Parquet columnar format. The rebuild script includes a comment
    /// noting that Parquet import requires an external tool.
    Parquet,
}

struct ColumnInfo {
    column_name: String,
    udt_name: String,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    is_nullable: bool,
    column_default: Option<String>,
}

struct ForeignKey {
    constraint_name: String,
    columns: Vec<String>,
    foreign_schema: String,
    foreign_table: String,
    foreign_columns: Vec<String>,
}

struct IndexInfo {
    #[allow(dead_code)]
    index_name: String,
    index_def: String,
}

struct TableSchema {
    schema_name: String,
    table_name: String,
    columns: Vec<ColumnInfo>,
    primary_key_columns: Vec<String>,
    foreign_keys: Vec<ForeignKey>,
    indexes: Vec<IndexInfo>,
}

async fn fetch_columns(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
    let rows = sqlx::query(
        "SELECT column_name, udt_name, character_maximum_length, \
         numeric_precision, numeric_scale, is_nullable, column_default \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await?;

    let columns = rows
        .iter()
        .map(|r| {
            let is_nullable_str: String = r.get("is_nullable");
            ColumnInfo {
                column_name: r.get("column_name"),
                udt_name: r.get("udt_name"),
                character_maximum_length: r.get("character_maximum_length"),
                numeric_precision: r.get("numeric_precision"),
                numeric_scale: r.get("numeric_scale"),
                is_nullable: is_nullable_str == "YES",
                column_default: r.get("column_default"),
            }
        })
        .collect();

    Ok(columns)
}

async fn fetch_primary_key(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT kcu.column_name \
         FROM information_schema.table_constraints tc \
         JOIN information_schema.key_column_usage kcu \
           ON tc.constraint_name = kcu.constraint_name \
           AND tc.table_schema = kcu.table_schema \
         WHERE tc.table_schema = $1 \
           AND tc.table_name = $2 \
           AND tc.constraint_type = 'PRIMARY KEY' \
         ORDER BY kcu.ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await?;

    let columns: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    Ok(columns)
}

async fn fetch_foreign_keys(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<ForeignKey>> {
    let rows = sqlx::query(
        "SELECT tc.constraint_name, kcu.column_name, \
                ccu.table_schema AS foreign_table_schema, \
                ccu.table_name AS foreign_table_name, \
                ccu.column_name AS foreign_column_name \
         FROM information_schema.table_constraints tc \
         JOIN information_schema.key_column_usage kcu \
           ON tc.constraint_name = kcu.constraint_name \
           AND tc.table_schema = kcu.table_schema \
         JOIN information_schema.constraint_column_usage ccu \
           ON tc.constraint_name = ccu.constraint_name \
         WHERE tc.table_schema = $1 \
           AND tc.table_name = $2 \
           AND tc.constraint_type = 'FOREIGN KEY' \
         ORDER BY tc.constraint_name, kcu.ordinal_position",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await?;

    let mut fk_map: BTreeMap<String, ForeignKey> = BTreeMap::new();
    for row in &rows {
        let constraint_name: String = row.get("constraint_name");
        let column_name: String = row.get("column_name");
        let foreign_schema: String = row.get("foreign_table_schema");
        let foreign_table: String = row.get("foreign_table_name");
        let foreign_column: String = row.get("foreign_column_name");

        let fk = fk_map
            .entry(constraint_name.clone())
            .or_insert_with(|| ForeignKey {
                constraint_name,
                columns: Vec::new(),
                foreign_schema,
                foreign_table,
                foreign_columns: Vec::new(),
            });
        fk.columns.push(column_name);
        fk.foreign_columns.push(foreign_column);
    }

    Ok(fk_map.into_values().collect())
}

async fn fetch_indexes(pool: &PgPool, schema: &str, table: &str) -> Result<Vec<IndexInfo>> {
    let rows = sqlx::query(
        "SELECT indexname, indexdef \
         FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 \
           AND indexname NOT IN ( \
             SELECT constraint_name FROM information_schema.table_constraints \
             WHERE table_schema = $1 AND constraint_type = 'PRIMARY KEY' \
           )",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await?;

    let indexes = rows
        .iter()
        .map(|r| IndexInfo {
            index_name: r.get("indexname"),
            index_def: r.get("indexdef"),
        })
        .collect();

    Ok(indexes)
}

async fn fetch_table_schema(pool: &PgPool, schema: &str, table: &str) -> Result<TableSchema> {
    let columns = fetch_columns(pool, schema, table).await?;
    let primary_key_columns = fetch_primary_key(pool, schema, table).await?;
    let foreign_keys = fetch_foreign_keys(pool, schema, table).await?;
    let indexes = fetch_indexes(pool, schema, table).await?;

    Ok(TableSchema {
        schema_name: schema.to_string(),
        table_name: table.to_string(),
        columns,
        primary_key_columns,
        foreign_keys,
        indexes,
    })
}

/// Sample every table in a PostgreSQL database and write the results to files.
///
/// Connects to the database at `connection_string`, discovers all non-system
/// schemas and their tables, and samples `percent`% of each table's rows
/// concurrently. Each table is written as a separate file (CSV or Parquet) in
/// a per-schema subdirectory under `outdir`. A `rebuild.sql` script is also
/// generated that can recreate the schemas and reload the sampled data.
///
/// # Arguments
///
/// * `connection_string` — PostgreSQL connection string
///   (e.g. `"postgres://user:pass@localhost/mydb"`).
/// * `outdir` — Directory for output files (created if it does not exist).
/// * `percent` — Percentage of rows to sample per table (e.g. `10.0` for 10%).
/// * `seed` — Optional seed for reproducible sampling via PostgreSQL's
///   `setseed()`.
/// * `format` — [`OutputFormat::Csv`] or [`OutputFormat::Parquet`].
/// * `schema_filter` — Optional list of schema names to include. When `None`,
///   all non-system schemas are sampled.
///
/// # Errors
///
/// Returns an error if the database connection fails or the output directory
/// cannot be created.
pub async fn run(
    connection_string: &str,
    outdir: &str,
    percent: f64,
    seed: Option<u64>,
    format: OutputFormat,
    schema_filter: Option<Vec<String>>,
) -> Result<()> {
    let pool = PgPool::connect(connection_string)
        .await
        .context("Failed to connect to PostgreSQL")?;

    fs::create_dir_all(outdir).context("Failed to create output directory")?;

    let db_schemas = enumerate_schemas(&pool, schema_filter.as_deref()).await?;

    if db_schemas.is_empty() {
        println!("No schemas found.");
        return Ok(());
    }

    let mut all_tables: Vec<(String, String)> = Vec::new();
    for schema in &db_schemas {
        let tables = enumerate_tables(&pool, schema).await?;
        for table in tables {
            all_tables.push((schema.clone(), table));
        }
    }

    if all_tables.is_empty() {
        println!("No tables found in schemas: {:?}", db_schemas);
        return Ok(());
    }

    println!(
        "Found {} table(s) across {} schema(s).",
        all_tables.len(),
        db_schemas.len()
    );

    let mut set = JoinSet::new();

    for (schema, table) in all_tables {
        let pool = pool.clone();
        let outdir = outdir.to_string();
        let format = format.clone();

        set.spawn(async move {
            match process_table(&pool, &schema, &table, &outdir, percent, seed, &format).await {
                Ok(()) => match fetch_table_schema(&pool, &schema, &table).await {
                    Ok(ts) => Some(ts),
                    Err(e) => {
                        eprintln!(
                            "Warning: could not fetch schema for '{}'.'{}':{}",
                            schema, table, e
                        );
                        None
                    }
                },
                Err(e) => {
                    eprintln!("Warning: skipping table '{}'.'{}':{}", schema, table, e);
                    None
                }
            }
        });
    }

    let mut schemas: Vec<TableSchema> = Vec::new();
    while let Some(result) = set.join_next().await {
        if let Ok(Some(schema)) = result {
            schemas.push(schema);
        }
    }

    schemas.sort_by(|a, b| (&a.schema_name, &a.table_name).cmp(&(&b.schema_name, &b.table_name)));

    if !schemas.is_empty() {
        let sql = generate_rebuild_sql(&schemas, &format);
        let sql_path = format!("{}/rebuild.sql", outdir);
        fs::write(&sql_path, &sql).context("Failed to write rebuild.sql")?;
        println!("Wrote rebuild script to {}", sql_path);
    }

    Ok(())
}

async fn count_rows(pool: &PgPool, schema: &str, table: &str) -> Result<i64> {
    let query = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
    let row: (i64,) = sqlx::query_as(&query).fetch_one(pool).await?;
    Ok(row.0)
}

async fn enumerate_schemas(pool: &PgPool, filter: Option<&[String]>) -> Result<Vec<String>> {
    let schemas = if let Some(names) = filter {
        sqlx::query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
               AND schema_name NOT LIKE 'pg_temp_%' \
               AND schema_name = ANY($1::text[]) \
             ORDER BY schema_name",
        )
        .bind(names)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
               AND schema_name NOT LIKE 'pg_temp_%' \
             ORDER BY schema_name",
        )
        .fetch_all(pool)
        .await?
    };

    let names: Vec<String> = schemas.iter().map(|r| r.get(0)).collect();
    Ok(names)
}

async fn enumerate_tables(pool: &PgPool, schema: &str) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    let tables: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    Ok(tables)
}

fn calculate_sample_count(percent: f64, total_rows: i64) -> i64 {
    ((percent / 100.0) * total_rows as f64).floor() as i64
}

fn map_seed_to_pg(s: u64) -> f64 {
    (s as f64 / u64::MAX as f64) * 2.0 - 1.0
}

async fn process_table(
    pool: &PgPool,
    schema: &str,
    table: &str,
    outdir: &str,
    percent: f64,
    seed: Option<u64>,
    format: &OutputFormat,
) -> Result<()> {
    println!("Processing table '{}'.'{}'...", schema, table);

    let total_rows = count_rows(pool, schema, table).await?;
    let n = calculate_sample_count(percent, total_rows);

    let mut df = if n == 0 || total_rows == 0 {
        read_table_to_df(pool, schema, table, None, None).await?
    } else {
        read_table_to_df(pool, schema, table, Some(n), seed).await?
    };
    let sampled_rows = df.shape().0;

    let ext = match format {
        OutputFormat::Csv => "csv",
        OutputFormat::Parquet => "parquet",
    };
    let schema_dir = format!("{}/{}", outdir, schema);
    fs::create_dir_all(&schema_dir).context("Failed to create schema output directory")?;
    let outpath = format!("{}/{}.{}", schema_dir, table, ext);

    write_output(&mut df, &outpath, format)?;

    println!(
        "  {} rows -> {} rows -> {}",
        total_rows, sampled_rows, outpath
    );

    Ok(())
}

async fn read_table_to_df(
    pool: &PgPool,
    schema: &str,
    table: &str,
    limit: Option<i64>,
    seed: Option<u64>,
) -> Result<DataFrame> {
    let qualified = format!("\"{}\".\"{}\"", schema, table);
    let rows = if let Some(n) = limit {
        // Acquire a dedicated connection so setseed() and the query share the same session
        let mut conn = pool.acquire().await?;
        if let Some(s) = seed {
            let seed_float = map_seed_to_pg(s);
            sqlx::query(&format!("SELECT setseed({})", seed_float))
                .execute(&mut *conn)
                .await?;
        }
        let query = format!("SELECT * FROM {} ORDER BY RANDOM() LIMIT {}", qualified, n);
        sqlx::query(&query).fetch_all(&mut *conn).await?
    } else {
        let query = format!("SELECT * FROM {}", qualified);
        sqlx::query(&query).fetch_all(pool).await?
    };

    if rows.is_empty() {
        let describe_query = format!("SELECT * FROM {}", qualified);
        let describe = pool.describe(describe_query.as_str()).await?;
        let columns: Vec<Series> = describe
            .columns()
            .iter()
            .map(|col| {
                Series::new_empty(
                    col.name(),
                    &type_name_to_polars_dtype(col.type_info().name()),
                )
            })
            .collect();
        return Ok(DataFrame::new(columns)?);
    }

    let pg_columns = rows[0].columns();
    let num_cols = pg_columns.len();

    let mut accumulators: Vec<ColumnAccumulator> = pg_columns
        .iter()
        .map(|col| ColumnAccumulator::new(col.type_info().name()))
        .collect();

    for row in &rows {
        for i in 0..num_cols {
            accumulators[i].push(row, i);
        }
    }

    let series: Vec<Series> = accumulators
        .into_iter()
        .enumerate()
        .map(|(i, acc)| acc.into_series(pg_columns[i].name()))
        .collect();

    Ok(DataFrame::new(series)?)
}

fn write_output(df: &mut DataFrame, path: &str, format: &OutputFormat) -> Result<()> {
    let file = File::create(path).context("Could not create output file")?;
    match format {
        OutputFormat::Csv => {
            CsvWriter::new(file)
                .has_header(true)
                .with_separator(b',')
                .finish(df)?;
        }
        OutputFormat::Parquet => {
            ParquetWriter::new(file).finish(df)?;
        }
    }
    Ok(())
}

fn is_serial(col: &ColumnInfo) -> bool {
    col.column_default
        .as_ref()
        .map(|d| d.contains("nextval("))
        .unwrap_or(false)
}

fn udt_to_sql_type(col: &ColumnInfo) -> String {
    // Detect SERIAL/BIGSERIAL
    if is_serial(col) {
        return match col.udt_name.as_str() {
            "int8" => "BIGSERIAL".to_string(),
            _ => "SERIAL".to_string(),
        };
    }

    match col.udt_name.as_str() {
        "int2" => "SMALLINT".to_string(),
        "int4" => "INTEGER".to_string(),
        "int8" => "BIGINT".to_string(),
        "float4" => "REAL".to_string(),
        "float8" => "DOUBLE PRECISION".to_string(),
        "numeric" => match (col.numeric_precision, col.numeric_scale) {
            (Some(p), Some(s)) => format!("NUMERIC({},{})", p, s),
            (Some(p), None) => format!("NUMERIC({})", p),
            _ => "NUMERIC".to_string(),
        },
        "varchar" => match col.character_maximum_length {
            Some(n) => format!("VARCHAR({})", n),
            None => "VARCHAR".to_string(),
        },
        "bpchar" => match col.character_maximum_length {
            Some(n) => format!("CHAR({})", n),
            None => "CHAR".to_string(),
        },
        "text" => "TEXT".to_string(),
        "bool" => "BOOLEAN".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" => "TIMESTAMP".to_string(),
        "timestamptz" => "TIMESTAMPTZ".to_string(),
        "uuid" => "UUID".to_string(),
        "jsonb" => "JSONB".to_string(),
        "json" => "JSON".to_string(),
        "bytea" => "BYTEA".to_string(),
        other => other.to_uppercase(),
    }
}

fn generate_create_table(schema: &TableSchema) -> String {
    let mut sql = format!(
        "CREATE TABLE \"{}\".\"{}\" (\n",
        schema.schema_name, schema.table_name
    );

    let mut col_defs: Vec<String> = Vec::new();
    for col in &schema.columns {
        let sql_type = udt_to_sql_type(col);
        let mut def = format!("    \"{}\" {}", col.column_name, sql_type);

        if !col.is_nullable {
            def.push_str(" NOT NULL");
        }

        // Add DEFAULT unless it's a SERIAL (default is implicit)
        if !is_serial(col) {
            if let Some(ref default) = col.column_default {
                def.push_str(&format!(" DEFAULT {}", default));
            }
        }

        col_defs.push(def);
    }

    if !schema.primary_key_columns.is_empty() {
        let pk_cols: Vec<String> = schema
            .primary_key_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect();
        col_defs.push(format!("    PRIMARY KEY ({})", pk_cols.join(", ")));
    }

    sql.push_str(&col_defs.join(",\n"));
    sql.push_str("\n);\n");
    sql
}

fn generate_foreign_keys(schema: &TableSchema) -> String {
    let mut sql = String::new();
    for fk in &schema.foreign_keys {
        let local_cols: Vec<String> = fk.columns.iter().map(|c| format!("\"{}\"", c)).collect();
        let foreign_cols: Vec<String> = fk
            .foreign_columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect();

        sql.push_str(&format!(
            "ALTER TABLE \"{}\".\"{}\" ADD CONSTRAINT \"{}\" FOREIGN KEY ({}) REFERENCES \"{}\".\"{}\" ({});\n",
            schema.schema_name,
            schema.table_name,
            fk.constraint_name,
            local_cols.join(", "),
            fk.foreign_schema,
            fk.foreign_table,
            foreign_cols.join(", "),
        ));
    }
    sql
}

fn generate_copy_command(schema: &TableSchema, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Csv => {
            format!(
                "\\COPY \"{}\".\"{}\" FROM '{}/{}.csv' WITH (FORMAT csv, HEADER true);\n",
                schema.schema_name, schema.table_name, schema.schema_name, schema.table_name
            )
        }
        OutputFormat::Parquet => {
            format!(
                "-- Parquet loading requires an external tool (e.g., pgloader, or convert to CSV first)\n\
                 -- Data file: {}/{}.parquet\n",
                schema.schema_name, schema.table_name
            )
        }
    }
}

fn generate_rebuild_sql(schemas: &[TableSchema], format: &OutputFormat) -> String {
    let mut sql = String::new();

    sql.push_str("-- Generated by remnant\n");
    sql.push_str("-- Rebuild script for sampled database\n\n");
    sql.push_str("BEGIN;\n\n");

    // 0. CREATE SCHEMA statements
    let schema_names: std::collections::BTreeSet<&str> =
        schemas.iter().map(|s| s.schema_name.as_str()).collect();
    for name in &schema_names {
        sql.push_str(&format!("CREATE SCHEMA IF NOT EXISTS \"{}\";\n", name));
    }
    sql.push('\n');

    // 1. CREATE TABLE statements
    for schema in schemas {
        sql.push_str(&generate_create_table(schema));
        sql.push('\n');
    }

    // 2. COPY commands (after all tables exist)
    for schema in schemas {
        sql.push_str(&generate_copy_command(schema, format));
    }
    sql.push('\n');

    // 3. Foreign key constraints (after data is loaded)
    let mut has_fks = false;
    for schema in schemas {
        let fk_sql = generate_foreign_keys(schema);
        if !fk_sql.is_empty() {
            has_fks = true;
            sql.push_str(&fk_sql);
        }
    }
    if has_fks {
        sql.push('\n');
    }

    // 4. Indexes (after data and constraints)
    let mut has_indexes = false;
    for schema in schemas {
        for idx in &schema.indexes {
            has_indexes = true;
            sql.push_str(&idx.index_def);
            sql.push_str(";\n");
        }
    }
    if has_indexes {
        sql.push('\n');
    }

    sql.push_str("COMMIT;\n");
    sql
}

fn type_name_to_polars_dtype(type_name: &str) -> DataType {
    match type_name {
        "INT2" | "INT4" => DataType::Int32,
        "INT8" => DataType::Int64,
        "FLOAT4" => DataType::Float32,
        "FLOAT8" | "NUMERIC" => DataType::Float64,
        "BOOL" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

enum ColumnAccumulator {
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
}

impl ColumnAccumulator {
    fn new(type_name: &str) -> Self {
        match type_name {
            "INT2" => ColumnAccumulator::Int32(Vec::new()),
            "INT4" => ColumnAccumulator::Int32(Vec::new()),
            "INT8" => ColumnAccumulator::Int64(Vec::new()),
            "FLOAT4" => ColumnAccumulator::Float32(Vec::new()),
            "FLOAT8" | "NUMERIC" => ColumnAccumulator::Float64(Vec::new()),
            "BOOL" => ColumnAccumulator::Bool(Vec::new()),
            _ => ColumnAccumulator::Utf8(Vec::new()),
        }
    }

    fn push(&mut self, row: &sqlx::postgres::PgRow, idx: usize) {
        match self {
            ColumnAccumulator::Int32(v) => {
                // INT2 comes as i16, INT4 as i32
                if let Ok(val) = row.try_get::<Option<i32>, _>(idx) {
                    v.push(val);
                } else if let Ok(val) = row.try_get::<Option<i16>, _>(idx) {
                    v.push(val.map(|x| x as i32));
                } else {
                    v.push(None);
                }
            }
            ColumnAccumulator::Int64(v) => {
                v.push(row.try_get::<Option<i64>, _>(idx).unwrap_or(None));
            }
            ColumnAccumulator::Float32(v) => {
                v.push(row.try_get::<Option<f32>, _>(idx).unwrap_or(None));
            }
            ColumnAccumulator::Float64(v) => {
                if let Ok(val) = row.try_get::<Option<f64>, _>(idx) {
                    v.push(val);
                } else {
                    // NUMERIC fallback: try to get as string and parse
                    let str_val: Option<String> = row.try_get(idx).unwrap_or(None);
                    v.push(str_val.and_then(|s| s.parse::<f64>().ok()));
                }
            }
            ColumnAccumulator::Bool(v) => {
                v.push(row.try_get::<Option<bool>, _>(idx).unwrap_or(None));
            }
            ColumnAccumulator::Utf8(v) => {
                if let Ok(val) = row.try_get::<Option<String>, _>(idx) {
                    v.push(val);
                } else if let Ok(val) = row.try_get::<Option<chrono::NaiveDateTime>, _>(idx) {
                    v.push(val.map(|dt| dt.to_string()));
                } else if let Ok(val) = row.try_get::<Option<chrono::NaiveDate>, _>(idx) {
                    v.push(val.map(|d| d.to_string()));
                } else {
                    v.push(None);
                }
            }
        }
    }

    fn into_series(self, name: &str) -> Series {
        match self {
            ColumnAccumulator::Int32(v) => Series::new(name, &v),
            ColumnAccumulator::Int64(v) => Series::new(name, &v),
            ColumnAccumulator::Float32(v) => Series::new(name, &v),
            ColumnAccumulator::Float64(v) => Series::new(name, &v),
            ColumnAccumulator::Bool(v) => Series::new(name, &v),
            ColumnAccumulator::Utf8(v) => Series::new(name, &v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_name_to_polars_dtype() {
        assert_eq!(type_name_to_polars_dtype("INT4"), DataType::Int32);
        assert_eq!(type_name_to_polars_dtype("INT2"), DataType::Int32);
        assert_eq!(type_name_to_polars_dtype("INT8"), DataType::Int64);
        assert_eq!(type_name_to_polars_dtype("FLOAT4"), DataType::Float32);
        assert_eq!(type_name_to_polars_dtype("FLOAT8"), DataType::Float64);
        assert_eq!(type_name_to_polars_dtype("NUMERIC"), DataType::Float64);
        assert_eq!(type_name_to_polars_dtype("BOOL"), DataType::Boolean);
        assert_eq!(type_name_to_polars_dtype("TEXT"), DataType::Utf8);
        assert_eq!(type_name_to_polars_dtype("VARCHAR"), DataType::Utf8);
    }

    #[test]
    fn test_accumulator_int32() {
        let mut acc = ColumnAccumulator::Int32(Vec::new());
        if let ColumnAccumulator::Int32(ref mut v) = acc {
            v.push(Some(1));
            v.push(None);
            v.push(Some(3));
        }
        let series = acc.into_series("test");
        assert_eq!(series.len(), 3);
        assert_eq!(series.name(), "test");
        assert_eq!(series.null_count(), 1);
    }

    #[test]
    fn test_accumulator_utf8() {
        let mut acc = ColumnAccumulator::Utf8(Vec::new());
        if let ColumnAccumulator::Utf8(ref mut v) = acc {
            v.push(Some("hello".to_string()));
            v.push(None);
            v.push(Some("world".to_string()));
        }
        let series = acc.into_series("text_col");
        assert_eq!(series.len(), 3);
        assert_eq!(series.null_count(), 1);
    }

    #[test]
    fn test_accumulator_float64() {
        let mut acc = ColumnAccumulator::Float64(Vec::new());
        if let ColumnAccumulator::Float64(ref mut v) = acc {
            v.push(Some(1.5));
            v.push(Some(2.7));
            v.push(None);
        }
        let series = acc.into_series("floats");
        assert_eq!(series.len(), 3);
        assert_eq!(series.null_count(), 1);
    }

    #[test]
    fn test_accumulator_bool() {
        let mut acc = ColumnAccumulator::Bool(Vec::new());
        if let ColumnAccumulator::Bool(ref mut v) = acc {
            v.push(Some(true));
            v.push(Some(false));
            v.push(None);
        }
        let series = acc.into_series("flags");
        assert_eq!(series.len(), 3);
        assert_eq!(series.null_count(), 1);
    }

    // Helper to build a ColumnInfo for tests
    fn col(
        name: &str,
        udt: &str,
        nullable: bool,
        default: Option<&str>,
        char_len: Option<i32>,
        num_prec: Option<i32>,
        num_scale: Option<i32>,
    ) -> ColumnInfo {
        ColumnInfo {
            column_name: name.to_string(),
            udt_name: udt.to_string(),
            character_maximum_length: char_len,
            numeric_precision: num_prec,
            numeric_scale: num_scale,
            is_nullable: nullable,
            column_default: default.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_udt_to_sql_type_basic() {
        assert_eq!(
            udt_to_sql_type(&col("x", "int2", true, None, None, None, None)),
            "SMALLINT"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "int4", true, None, None, None, None)),
            "INTEGER"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "int8", true, None, None, None, None)),
            "BIGINT"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "float4", true, None, None, None, None)),
            "REAL"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "float8", true, None, None, None, None)),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "bool", true, None, None, None, None)),
            "BOOLEAN"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "text", true, None, None, None, None)),
            "TEXT"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "date", true, None, None, None, None)),
            "DATE"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "timestamptz", true, None, None, None, None)),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "uuid", true, None, None, None, None)),
            "UUID"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "jsonb", true, None, None, None, None)),
            "JSONB"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "varchar", true, None, Some(255), None, None)),
            "VARCHAR(255)"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "numeric", true, None, None, Some(10), Some(2))),
            "NUMERIC(10,2)"
        );
        assert_eq!(
            udt_to_sql_type(&col("x", "bpchar", true, None, Some(1), None, None)),
            "CHAR(1)"
        );
    }

    #[test]
    fn test_udt_to_sql_type_serial() {
        let c = col(
            "id",
            "int4",
            false,
            Some("nextval('users_id_seq'::regclass)"),
            None,
            None,
            None,
        );
        assert_eq!(udt_to_sql_type(&c), "SERIAL");

        let c = col(
            "id",
            "int8",
            false,
            Some("nextval('big_id_seq'::regclass)"),
            None,
            None,
            None,
        );
        assert_eq!(udt_to_sql_type(&c), "BIGSERIAL");
    }

    #[test]
    fn test_generate_create_table() {
        let schema = TableSchema {
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                col(
                    "id",
                    "int4",
                    false,
                    Some("nextval('users_id_seq'::regclass)"),
                    None,
                    None,
                    None,
                ),
                col("name", "varchar", false, None, Some(100), None, None),
                col("email", "text", true, None, None, None, None),
                col("score", "numeric", true, None, None, Some(10), Some(2)),
            ],
            primary_key_columns: vec!["id".to_string()],
            foreign_keys: vec![],
            indexes: vec![],
        };

        let sql = generate_create_table(&schema);
        assert!(sql.contains("CREATE TABLE \"public\".\"users\""));
        assert!(sql.contains("\"id\" SERIAL NOT NULL"));
        assert!(sql.contains("\"name\" VARCHAR(100) NOT NULL"));
        assert!(sql.contains("\"email\" TEXT"));
        assert!(sql.contains("\"score\" NUMERIC(10,2)"));
        assert!(sql.contains("PRIMARY KEY (\"id\")"));
        // SERIAL should not have a DEFAULT clause
        assert!(!sql.contains("nextval"));
    }

    #[test]
    fn test_generate_foreign_keys() {
        let schema = TableSchema {
            schema_name: "public".to_string(),
            table_name: "orders".to_string(),
            columns: vec![],
            primary_key_columns: vec![],
            foreign_keys: vec![ForeignKey {
                constraint_name: "orders_customer_id_fkey".to_string(),
                columns: vec!["customer_id".to_string()],
                foreign_schema: "public".to_string(),
                foreign_table: "customers".to_string(),
                foreign_columns: vec!["id".to_string()],
            }],
            indexes: vec![],
        };

        let sql = generate_foreign_keys(&schema);
        assert!(sql.contains("ALTER TABLE \"public\".\"orders\""));
        assert!(sql.contains("ADD CONSTRAINT \"orders_customer_id_fkey\""));
        assert!(sql.contains("FOREIGN KEY (\"customer_id\")"));
        assert!(sql.contains("REFERENCES \"public\".\"customers\" (\"id\")"));
    }

    #[test]
    fn test_generate_copy_command_csv() {
        let schema = TableSchema {
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            columns: vec![],
            primary_key_columns: vec![],
            foreign_keys: vec![],
            indexes: vec![],
        };
        let sql = generate_copy_command(&schema, &OutputFormat::Csv);
        assert!(sql.contains("\\COPY \"public\".\"users\" FROM 'public/users.csv'"));
        assert!(sql.contains("FORMAT csv, HEADER true"));
    }

    #[test]
    fn test_generate_copy_command_parquet() {
        let schema = TableSchema {
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            columns: vec![],
            primary_key_columns: vec![],
            foreign_keys: vec![],
            indexes: vec![],
        };
        let sql = generate_copy_command(&schema, &OutputFormat::Parquet);
        assert!(sql.contains("Parquet loading requires"));
        assert!(sql.contains("public/users.parquet"));
    }

    #[test]
    fn test_generate_rebuild_sql_ordering() {
        let schemas = vec![
            TableSchema {
                schema_name: "public".to_string(),
                table_name: "customers".to_string(),
                columns: vec![
                    col("id", "int4", false, None, None, None, None),
                    col("name", "text", false, None, None, None, None),
                ],
                primary_key_columns: vec!["id".to_string()],
                foreign_keys: vec![],
                indexes: vec![],
            },
            TableSchema {
                schema_name: "public".to_string(),
                table_name: "orders".to_string(),
                columns: vec![
                    col("id", "int4", false, None, None, None, None),
                    col("customer_id", "int4", false, None, None, None, None),
                ],
                primary_key_columns: vec!["id".to_string()],
                foreign_keys: vec![ForeignKey {
                    constraint_name: "orders_customer_id_fkey".to_string(),
                    columns: vec!["customer_id".to_string()],
                    foreign_schema: "public".to_string(),
                    foreign_table: "customers".to_string(),
                    foreign_columns: vec!["id".to_string()],
                }],
                indexes: vec![IndexInfo {
                    index_name: "idx_orders_customer".to_string(),
                    index_def: "CREATE INDEX idx_orders_customer ON public.orders USING btree (customer_id)".to_string(),
                }],
            },
        ];

        let sql = generate_rebuild_sql(&schemas, &OutputFormat::Csv);

        // Verify CREATE SCHEMA comes first
        let schema_pos = sql.find("CREATE SCHEMA IF NOT EXISTS").unwrap();
        let create_pos = sql.find("CREATE TABLE").unwrap();
        let copy_pos = sql.find("\\COPY").unwrap();
        let fk_pos = sql.find("ALTER TABLE").unwrap();
        let idx_pos = sql.find("CREATE INDEX").unwrap();

        assert!(
            schema_pos < create_pos,
            "CREATE SCHEMA should come before CREATE TABLE"
        );
        assert!(
            create_pos < copy_pos,
            "CREATE TABLE should come before COPY"
        );
        assert!(copy_pos < fk_pos, "COPY should come before FOREIGN KEY");
        assert!(fk_pos < idx_pos, "FOREIGN KEY should come before INDEX");

        // Verify wrapping
        assert!(sql.starts_with("-- Generated by remnant"));
        assert!(sql.contains("BEGIN;"));
        assert!(sql.contains("COMMIT;"));
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"public\""));
    }

    // -- Seed conversion tests --

    #[test]
    fn test_map_seed_zero() {
        assert_eq!(map_seed_to_pg(0), -1.0);
    }

    #[test]
    fn test_map_seed_max() {
        let result = map_seed_to_pg(u64::MAX);
        assert!(
            (result - 1.0).abs() < 1e-10,
            "expected ~1.0, got {}",
            result
        );
    }

    #[test]
    fn test_map_seed_midpoint() {
        let result = map_seed_to_pg(u64::MAX / 2);
        assert!(result.abs() < 0.01, "expected ~0.0, got {}", result);
    }

    #[test]
    fn test_map_seed_always_in_range() {
        for s in [0, 1, 1000, u64::MAX / 4, u64::MAX / 2, u64::MAX] {
            let result = map_seed_to_pg(s);
            assert!(
                (-1.0..=1.0).contains(&result),
                "seed {} mapped to {} which is outside [-1, 1]",
                s,
                result
            );
        }
    }

    // -- Row count calculation tests --

    #[test]
    fn test_calc_sample_10_pct_of_100() {
        assert_eq!(calculate_sample_count(10.0, 100), 10);
    }

    #[test]
    fn test_calc_sample_33_pct_of_10() {
        assert_eq!(calculate_sample_count(33.0, 10), 3);
    }

    #[test]
    fn test_calc_sample_0_percent() {
        assert_eq!(calculate_sample_count(0.0, 1000), 0);
    }

    #[test]
    fn test_calc_sample_100_percent() {
        assert_eq!(calculate_sample_count(100.0, 500), 500);
    }

    #[test]
    fn test_calc_sample_zero_rows() {
        assert_eq!(calculate_sample_count(50.0, 0), 0);
    }

    #[test]
    fn test_calc_sample_large_table() {
        assert_eq!(calculate_sample_count(0.1, 10_000_000), 10_000);
    }

    // -- write_output tests --

    #[test]
    fn test_write_output_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        let path_str = path.to_str().unwrap();

        let mut df = DataFrame::new(vec![
            Series::new("id", &[1i32, 2, 3]),
            Series::new("name", &["a", "b", "c"]),
        ])
        .unwrap();

        write_output(&mut df, path_str, &OutputFormat::Csv).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines.len(), 4); // header + 3 rows
    }

    #[test]
    fn test_write_output_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let path_str = path.to_str().unwrap();

        let mut df = DataFrame::new(vec![
            Series::new("id", &[1i32, 2, 3]),
            Series::new("name", &["a", "b", "c"]),
        ])
        .unwrap();

        write_output(&mut df, path_str, &OutputFormat::Parquet).unwrap();

        let file = File::open(&path).unwrap();
        let read_df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(read_df.shape(), (3, 2));
        let col_names: Vec<&str> = read_df.get_column_names();
        assert_eq!(col_names, vec!["id", "name"]);
    }

    #[test]
    fn test_write_output_empty_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.csv");
        let path_str = path.to_str().unwrap();

        let mut df = DataFrame::new(vec![
            Series::new_empty("id", &DataType::Int32),
            Series::new_empty("name", &DataType::Utf8),
        ])
        .unwrap();

        write_output(&mut df, path_str, &OutputFormat::Csv).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1); // header only
        assert_eq!(lines[0], "id,name");
    }

    // -- Missing ColumnAccumulator variant tests --

    #[test]
    fn test_accumulator_int64() {
        let mut acc = ColumnAccumulator::Int64(Vec::new());
        if let ColumnAccumulator::Int64(ref mut v) = acc {
            v.push(Some(1i64));
            v.push(None);
            v.push(Some(i64::MAX));
        }
        let series = acc.into_series("big_ids");
        assert_eq!(series.len(), 3);
        assert_eq!(series.null_count(), 1);
    }

    #[test]
    fn test_accumulator_float32() {
        let mut acc = ColumnAccumulator::Float32(Vec::new());
        if let ColumnAccumulator::Float32(ref mut v) = acc {
            v.push(Some(1.5f32));
            v.push(Some(2.7f32));
            v.push(None);
        }
        let series = acc.into_series("small_floats");
        assert_eq!(series.len(), 3);
        assert_eq!(series.null_count(), 1);
    }
}

#[cfg(all(test, feature = "integration"))]
mod integration_tests {
    use super::*;

    fn database_url() -> String {
        std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for integration tests")
    }

    async fn setup_test_table(pool: &PgPool, name: &str, n_rows: i64) -> String {
        let table_name = format!("remnant_test_{}", name);
        sqlx::query(&format!("DROP TABLE IF EXISTS \"{}\"", table_name))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!(
            "CREATE TABLE \"{}\" (id SERIAL PRIMARY KEY, name TEXT NOT NULL, score DOUBLE PRECISION)",
            table_name
        ))
        .execute(pool)
        .await
        .unwrap();
        for i in 0..n_rows {
            sqlx::query(&format!(
                "INSERT INTO \"{}\" (name, score) VALUES ('item_{}', {})",
                table_name,
                i,
                i as f64 * 1.1
            ))
            .execute(pool)
            .await
            .unwrap();
        }
        table_name
    }

    async fn teardown_test_table(pool: &PgPool, table_name: &str) {
        sqlx::query(&format!("DROP TABLE IF EXISTS \"{}\"", table_name))
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_count_rows() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "count", 100).await;
        let count = count_rows(&pool, "public", &table).await.unwrap();
        assert_eq!(count, 100);
        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_enumerate_tables() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "enumerate", 1).await;
        let tables = enumerate_tables(&pool, "public").await.unwrap();
        assert!(
            tables.contains(&table),
            "expected {} in {:?}",
            table,
            tables
        );
        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_read_table_full() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "read_full", 50).await;
        let df = read_table_to_df(&pool, "public", &table, None, None)
            .await
            .unwrap();
        assert_eq!(df.shape().0, 50);
        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_read_table_with_limit() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "read_limit", 100).await;
        let df = read_table_to_df(&pool, "public", &table, Some(20), None)
            .await
            .unwrap();
        assert_eq!(df.shape().0, 20);
        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_read_table_with_seed_determinism() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "read_seed", 100).await;
        let df1 = read_table_to_df(&pool, "public", &table, Some(30), Some(42))
            .await
            .unwrap();
        let df2 = read_table_to_df(&pool, "public", &table, Some(30), Some(42))
            .await
            .unwrap();
        assert!(
            df1.frame_equal(&df2),
            "same seed should produce identical results"
        );
        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_read_table_empty() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "read_empty", 0).await;
        let df = read_table_to_df(&pool, "public", &table, None, None)
            .await
            .unwrap();
        assert_eq!(df.shape().0, 0);
        assert!(df.width() > 0, "empty table should still have columns");
        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_process_table_csv() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table = setup_test_table(&pool, "process_csv", 200).await;
        let dir = tempfile::tempdir().unwrap();
        let outdir = dir.path().to_str().unwrap();

        process_table(
            &pool,
            "public",
            &table,
            outdir,
            25.0,
            Some(42),
            &OutputFormat::Csv,
        )
        .await
        .unwrap();

        let outpath = format!("{}/public/{}.csv", outdir, table);
        let content = std::fs::read_to_string(&outpath).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 51, "header + 50 rows (25% of 200)");

        teardown_test_table(&pool, &table).await;
    }

    #[tokio::test]
    async fn test_full_pipeline() {
        let pool = PgPool::connect(&database_url()).await.unwrap();
        let table1 = setup_test_table(&pool, "pipeline_a", 100).await;
        let table2 = setup_test_table(&pool, "pipeline_b", 50).await;
        let dir = tempfile::tempdir().unwrap();
        let outdir = dir.path().to_str().unwrap();

        run(
            &database_url(),
            outdir,
            10.0,
            Some(42),
            OutputFormat::Csv,
            Some(vec!["public".to_string()]),
        )
        .await
        .unwrap();

        // Verify output files exist in schema subdirectory
        let path1 = dir.path().join("public").join(format!("{}.csv", table1));
        let path2 = dir.path().join("public").join(format!("{}.csv", table2));
        assert!(path1.exists(), "output file for {} should exist", table1);
        assert!(path2.exists(), "output file for {} should exist", table2);

        // Verify rebuild.sql exists and references our tables with schema qualification
        let rebuild_path = dir.path().join("rebuild.sql");
        assert!(rebuild_path.exists(), "rebuild.sql should exist");
        let sql = std::fs::read_to_string(&rebuild_path).unwrap();
        assert!(sql.contains(&format!("CREATE TABLE \"public\".\"{}\"", table1)));
        assert!(sql.contains(&format!("CREATE TABLE \"public\".\"{}\"", table2)));
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS \"public\""));

        teardown_test_table(&pool, &table1).await;
        teardown_test_table(&pool, &table2).await;
    }
}
