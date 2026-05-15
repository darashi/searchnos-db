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
    /// Dump stored ndb notes as length-prefixed binary records
    Dump {
        /// Path to the output dump file
        #[arg(value_name = "FILE", value_hint = ValueHint::FilePath)]
        output_path: PathBuf,
    },
    /// Load ndb notes from a length-prefixed binary dump
    Load {
        /// Path to the input dump file
        #[arg(value_name = "FILE", value_hint = ValueHint::FilePath)]
        input_path: PathBuf,
    },
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
        Command::Dump { output_path } => cmd::dump::run(&db_path, &output_path),
        Command::Load { input_path } => cmd::load::run(&db_path, &input_path),
        Command::Query { filters } => cmd::query::run(&db_path, filters),
        Command::Import { import_paths } => cmd::import::run(&db_path, &import_paths),
    }
}
