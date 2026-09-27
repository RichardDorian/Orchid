use std::process::ExitCode;

use clap::Parser;
use orchidctl::Cli;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut stdout = std::io::stdout();
    match orchidctl::run(cli, &mut stdout).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
