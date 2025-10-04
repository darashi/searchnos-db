mod cmd;

use crate::cmd::CliError;
use clap::{CommandFactory, Parser, Subcommand, ValueHint};
use std::path::PathBuf;

const DEFAULT_DB_PATH: &str = "./data";

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, global = true, default_value = DEFAULT_DB_PATH)]
    db_path: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Display database statistics
    Stat,
    /// Print events as JSON
    Query {
        /// Filter(s) as JSON: accepts an object for a single filter or an array for multiple
        filters: Option<String>,
    },
    /// Import events from a JSONL file
    Import {
        /// Path to input JSONL file
        #[arg(value_name = "FILE", value_hint = ValueHint::FilePath, num_args = 1..)]
        import_paths: Vec<PathBuf>,
        /// Maximum number of events per batch write
        #[arg(long, default_value_t = 1024)]
        batch_size: usize,
        /// Flush interval in milliseconds
        #[arg(long, default_value_t = 100)]
        flush_interval_ms: u64,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), CliError> {
    let Cli { db_path, command } = Cli::parse();
    let Some(command) = command else {
        println!("{}", Cli::command().render_help());
        return Ok(());
    };

    match command {
        Command::Stat => cmd::stat::run(&db_path),
        Command::Query { filters } => cmd::query::run(&db_path, filters),
        Command::Import {
            import_paths,
            batch_size,
            flush_interval_ms,
        } => {
            let flush_interval = std::time::Duration::from_millis(flush_interval_ms);
            cmd::import::run(&db_path, &import_paths, batch_size, flush_interval)
        }
    }
}
