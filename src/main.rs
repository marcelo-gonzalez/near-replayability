use anyhow::Context;
use sqlx::postgres::PgPool;
use std::path::PathBuf;

mod apply_chunks;
mod epoch_info;

#[derive(clap::Parser)]
struct RunCmd {
    #[clap(long)]
    neard_path: PathBuf,
    #[clap(long)]
    home_dir: PathBuf,
}

#[derive(clap::Parser)]
enum SubCmd {
    Run(RunCmd),
    PopulateEpochs,
    // TODO: TestParallelism(TestParallelismCmd),
}

#[derive(clap::Parser)]
struct Cmd {
    #[clap(subcommand)]
    subcommand: SubCmd,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("sqlx::query", log::LevelFilter::Warn)
        .env()
        .init()
        .unwrap();

    dotenv::dotenv().ok();

    let cmd: Cmd = clap::Parser::parse();

    let database_url =
        std::env::var("DATABASE_URL").context("error reading DATABASE_URL environment variable")?;
    let db = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("Error connecting to {}", database_url))?;

    match cmd.subcommand {
        SubCmd::Run(cmd) => apply_chunks::apply_chunks(&db, &cmd.neard_path, &cmd.home_dir).await,
        SubCmd::PopulateEpochs => {
            epoch_info::populate(&db, "https://archival-rpc.mainnet.near.org", 85372640).await
        }
    }
}
