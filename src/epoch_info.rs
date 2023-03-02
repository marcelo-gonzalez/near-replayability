use anyhow::Context;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_primitives::types::{BlockId, BlockReference, EpochId, EpochReference};
use near_primitives::views::BlockView;
use sqlx::postgres::PgPool;
use std::time::Duration;

async fn find_first_block(
    client: &JsonRpcClient,
    start_height: u64,
    max_height: u64,
) -> anyhow::Result<BlockView> {
    let mut height = start_height;
    loop {
        match client
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(height)),
            })
            .await
        {
            Ok(b) => return Ok(b),
            Err(err) => match err.handler_error() {
                Some(methods::block::RpcBlockError::UnknownBlock { .. }) => {
                    if height >= max_height {
                        anyhow::bail!(
                            "can't find any block between {} and {}",
                            start_height,
                            max_height
                        );
                    }
                    height += 1;
                    continue;
                }
                _ => anyhow::bail!("error fetching block #{}: {:?}", height, err),
            },
        };
    }
}

pub async fn populate(db: &PgPool, rpc_url: &str, head_height: u64) -> anyhow::Result<()> {
    let client = JsonRpcClient::connect(rpc_url);
    let mut start_block =
        match sqlx::query!("SELECT height, start_height FROM epochs ORDER BY height ASC LIMIT 1")
            .fetch_one(db)
            .await
        {
            Ok(row) => {
                // TODO: handle missing rows if they were skipped/removed for some reason
                if row.height == 1 {
                    return Ok(());
                }
                let block_reference =
                    BlockReference::BlockId(BlockId::Height(row.start_height.try_into().unwrap()));
                client
                    .call(methods::block::RpcBlockRequest { block_reference })
                    .await
                    .with_context(|| format!("error fetching block #{}", row.start_height))?
            }
            Err(sqlx::Error::RowNotFound) => {
                let block_reference =
                    BlockReference::BlockId(BlockId::Height(head_height.try_into().unwrap()));
                let block = client
                    .call(methods::block::RpcBlockRequest { block_reference })
                    .await
                    .with_context(|| format!("error fetching block #{}", head_height))?;
                let epoch_info = client
                    .call(methods::validators::RpcValidatorRequest {
                        epoch_reference: EpochReference::EpochId(EpochId(block.header.epoch_id)),
                    })
                    .await
                    .with_context(|| {
                        format!("failed fetching epoch info for block #{}", head_height)
                    })?;

                find_first_block(&client, epoch_info.epoch_start_height, head_height).await?
            }
            Err(e) => return Err(e).context("failed fetching last indexed epoch"),
        };

    while start_block.header.prev_hash != Default::default() {
        // don't spam too hard
        tokio::time::sleep(Duration::from_millis(300)).await;

        let prev_epoch_end = client
            .call(methods::block::RpcBlockRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(
                    start_block.header.prev_hash,
                )),
            })
            .await
            .with_context(|| format!("error fetching block {}", &start_block.header.prev_hash))?;

        let epoch_info = client
            .call(methods::validators::RpcValidatorRequest {
                epoch_reference: EpochReference::BlockId(BlockId::Height(
                    prev_epoch_end.header.height,
                )),
            })
            .await
            .with_context(|| {
                format!(
                    "failed fetching epoch info for block #{}",
                    prev_epoch_end.header.height
                )
            })?;

        start_block = find_first_block(
            &client,
            epoch_info.epoch_start_height,
            prev_epoch_end.header.height,
        )
        .await?;

        tracing::info!(target: "near-replayability", "inserting epoch info #{}", &epoch_info.epoch_height);

        sqlx::query!(
            "INSERT INTO epochs (height, id, start_height, end_height, chunks_applied, chunk_application_failures) \
            VALUES ($1, $2, $3, $4, $5, $6)
            ",
            i64::try_from(epoch_info.epoch_height).unwrap(),
            prev_epoch_end.header.epoch_id.to_string(),
            i64::try_from(start_block.header.height).unwrap(),
            i64::try_from(prev_epoch_end.header.height).unwrap(),
            0,
            0,
        )
        .execute(db)
        .await.context("error inserting epoch info")?;
    }

    Ok(())
}
