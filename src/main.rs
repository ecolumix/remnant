use anyhow::Result;
use clap::{Parser, Subcommand};
use remnant::pg::OutputFormat;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Sample a percentage of rows from a CSV file
    Csv {
        #[arg(short, long)]
        file: String,

        #[arg(short, long, default_value_t = 10.0)]
        percent: f64,

        #[arg(short, long, default_value_t = 10_000)]
        max_records: i32,

        #[arg(short, long)]
        outfile: String,

        #[arg(short, long)]
        seed: Option<u64>,
    },
    /// Sample a percentage of rows from each table in a PostgreSQL database
    Pg {
        #[arg(short, long)]
        connection_string: String,

        #[arg(short, long)]
        outdir: String,

        #[arg(short, long, default_value_t = 10.0)]
        percent: f64,

        #[arg(short = 'S', long)]
        seed: Option<u64>,

        #[arg(short, long, value_enum, default_value_t = OutputFormat::Csv)]
        format: OutputFormat,

        /// Comma-separated list of schemas to include (default: all non-system schemas)
        #[arg(long, value_delimiter = ',')]
        schemas: Option<Vec<String>>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let t1 = Instant::now();
    let cli = Cli::parse();

    match cli.command {
        Commands::Csv {
            file,
            percent,
            max_records,
            outfile,
            seed,
        } => {
            remnant::csv::run(&file, &outfile, percent, Some(max_records as usize), seed)?;
        }
        Commands::Pg {
            connection_string,
            outdir,
            percent,
            seed,
            format,
            schemas,
        } => {
            remnant::pg::run(&connection_string, &outdir, percent, seed, format, schemas).await?;
        }
    }

    let t2 = Instant::now();
    println!("Completed in: {:?}", t2 - t1);

    Ok(())
}
