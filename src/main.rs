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
    #[clap(long)]
    num_processes: Option<usize>,
}

#[derive(clap::Parser)]
struct TestParallelismCmd {
    #[clap(long)]
    neard_path: PathBuf,
    #[clap(long)]
    home_dir: PathBuf,
    #[clap(long)]
    height: u64,
    #[clap(long)]
    shard_id: u32,
    #[clap(long)]
    total_applies: usize,
    #[clap(long)]
    num_processes: usize,
}

#[derive(clap::Parser)]
enum SubCmd {
    Run(RunCmd),
    PopulateEpochs,
    TestParallelism(TestParallelismCmd),
}

#[derive(clap::Parser)]
struct Cmd {
    #[clap(subcommand)]
    subcommand: SubCmd,
}

async fn db_connect() -> anyhow::Result<PgPool> {
    let database_url =
        std::env::var("DATABASE_URL").context("error reading DATABASE_URL environment variable")?;
    PgPool::connect(&database_url)
        .await
        .with_context(|| format!("Error connecting to {}", database_url))
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

    match cmd.subcommand {
        SubCmd::Run(cmd) => {
            let num_processes = cmd.num_processes.unwrap_or_else(num_cpus::get);
            apply_chunks::apply_chunks(
                &db_connect().await?,
                &cmd.neard_path,
                &cmd.home_dir,
                num_processes,
            )
            .await
        }
        SubCmd::PopulateEpochs => {
            epoch_info::populate(
                &db_connect().await?,
                "https://archival-rpc.mainnet.near.org",
                85372640,
            )
            .await
        }
        SubCmd::TestParallelism(cmd) => {
            apply_chunks::test_parallelism(
                &cmd.neard_path,
                &cmd.home_dir,
                cmd.height,
                cmd.shard_id,
                cmd.total_applies,
                cmd.num_processes,
            )
            .await
        }
    }
}
