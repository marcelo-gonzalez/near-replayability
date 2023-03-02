use anyhow::Context;
use sqlx::postgres::PgPool;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

struct NeardPath {
    path: PathBuf,
    modified: SystemTime,
    version: String,
    version_hash: i32,
}

impl NeardPath {
    fn new(path: &Path) -> anyhow::Result<Self> {
        let m = std::fs::metadata(path)
            .with_context(|| format!("failed getting file status for {}", path.display()))?;

        let mut ret = Self {
            path: path.into(),
            modified: m.modified().context("no system time in file status?")?,
            version: String::new(),
            version_hash: 0,
        };
        ret.load_version()?;
        Ok(ret)
    }

    fn load_version(&mut self) -> anyhow::Result<()> {
        let output = Command::new(&self.path)
            .arg("--version")
            .output()
            .with_context(|| format!("failed executing {}", self.path.display()))?;
        anyhow::ensure!(
            output.status.success(),
            "nonzero exit code from {} --version",
            self.path.display()
        );
        let hash = md5::compute(&output.stdout);
        let version = String::from_utf8(output.stdout)
            .with_context(|| format!("non utf-8 data from {} --version", self.path.display()))?;
        self.version = version;
        self.version_hash = i32::from_le_bytes(hash[..4].try_into().unwrap());
        Ok(())
    }

    async fn insert_version(&self, db: &PgPool) -> anyhow::Result<()> {
        sqlx::query!(
            "INSERT INTO neard_versions (hash, version) \
            VALUES ($1, $2) ON CONFLICT DO NOTHING
            ",
            self.version_hash,
            self.version.clone()
        )
        .execute(db)
        .await
        .context("failed inserting version information to the DB")?;
        Ok(())
    }

    async fn reload_and_insert(&mut self, db: &PgPool) -> anyhow::Result<()> {
        let m = std::fs::metadata(&self.path)
            .with_context(|| format!("failed getting file status for {}", self.path.display()))?;
        let modified = m.modified().context("no system time in file status?")?;
        if modified != self.modified {
            tracing::warn!(target: "near-replayability", "neard version changed from under us!");
            self.load_version()?;
            self.insert_version(db).await?;
        }
        Ok(())
    }
}

#[allow(non_camel_case_types)]
#[derive(PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(type_name = "apply_status")]
enum ApplyStatus {
    ok,
    failed,
    pending,
    height_missing,
}

#[derive(Debug)]
struct Job {
    epoch_id: String,
    height: i64,
    shard_id: i32,
    status: ApplyStatus,
}

async fn next_jobs(db: &PgPool, neard_version_hash: i32) -> anyhow::Result<Vec<Job>> {
    let rows = sqlx::query!(
        "
WITH next_epoch AS
(
   SELECT
      *
   FROM
      epochs 
   ORDER BY
      chunks_applied LIMIT 1 
)
,
epoch_heights AS 
(
   SELECT
      id AS epoch_id,
      generate_series(start_height, end_height) AS block_height 
   FROM
      next_epoch 
)
,
epoch_chunks AS 
(
   SELECT
      *,
      generate_series(0, 3) AS shard_id 
   FROM
      epoch_heights 
)
,
chunks_to_apply AS 
(
   INSERT INTO
      applied_chunks (height, shard_id, epoch_id, neard_version_hash, status, job_queued_at) 
      SELECT
         block_height,
         epoch_chunks.shard_id,
         epoch_chunks.epoch_id,
         $1,
         'pending',
         'now'
      FROM
         epoch_chunks 
         LEFT JOIN
            applied_chunks 
            ON epoch_chunks.block_height = applied_chunks.height 
            AND epoch_chunks.shard_id = applied_chunks.shard_id 
      WHERE
         applied_chunks.height IS NULL LIMIT 50
      ON CONFLICT DO NOTHING RETURNING height, shard_id, epoch_id
)
,
epoch_count AS 
(
   UPDATE
      epochs 
   SET
      chunks_applied = new_count.count 
   FROM
      (
         SELECT
            next_epoch.height,
            next_epoch.chunks_applied + c.count AS count 
         FROM
            next_epoch 
            CROSS JOIN
               (
                  SELECT
                     COUNT(*) 
                  FROM
                     chunks_to_apply
               )
               c
      )
      new_count 
   WHERE
      epochs.height = new_count.height
)
SELECT * FROM chunks_to_apply;
        ",
        neard_version_hash
    )
    .fetch_all(db)
    .await
    .context("failed getting new apply jobs")?;
    Ok(rows
        .into_iter()
        .map(|r| Job {
            height: r.height,
            shard_id: r.shard_id,
            epoch_id: r.epoch_id,
            status: ApplyStatus::pending,
        })
        .collect())
}

async fn update_jobs(
    db: &PgPool,
    finished: Vec<Job>,
    neard_version_hash: i32,
) -> anyhow::Result<()> {
    let mut epoch_failures = HashMap::<_, i64>::new();
    for j in finished.iter() {
        if j.status == ApplyStatus::failed {
            *epoch_failures.entry(j.epoch_id.clone()).or_default() += 1;
        }
    }
    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "UPDATE applied_chunks AS a SET
        status = b.status
        FROM (",
    );
    builder.push_values(finished, |mut b, job| {
        b.push_bind(job.height)
            .push_bind(job.shard_id)
            .push_bind(neard_version_hash)
            .push_bind(job.status);
    });
    builder.push("
    ) AS b(height, shard_id, neard_version_hash, status)
    WHERE a.height = b.height AND a.shard_id = b.shard_id AND a.neard_version_hash = b.neard_version_hash;
    ");
    let query = builder.build();
    query
        .execute(db)
        .await
        .context("failed updating applied_chunks rows")?;
    // there will only be one of these for now
    for (epoch_id, failures) in epoch_failures {
        sqlx::query!(
            "UPDATE epochs SET chunk_application_failures = chunk_application_failures + $1 WHERE id = $2;",
            failures,
            epoch_id,
        )
        .execute(db)
        .await
        .context("failed decrementing epoch table chunks_applied row")?;
    }
    Ok(())
}

async fn delete_jobs(
    db: &PgPool,
    still_pending: Vec<Job>,
    neard_version_hash: i32,
) -> anyhow::Result<()> {
    let mut epoch_decrements = HashMap::<_, i64>::new();
    for j in still_pending.iter() {
        *epoch_decrements.entry(j.epoch_id.clone()).or_default() += 1;
    }
    let mut builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "WITH p (height, shard_id, neard_version_hash) AS ( ",
    );
    builder.push_values(still_pending, |mut b, job| {
        b.push_bind(job.height)
            .push_bind(job.shard_id)
            .push_bind(neard_version_hash);
    });
    builder.push("
    )
    DELETE FROM applied_chunks AS a USING p
    WHERE a.height = p.height AND a.shard_id = p.shard_id AND p.neard_version_hash = p.neard_version_hash;
    ");
    let query = builder.build();
    query
        .execute(db)
        .await
        .context("failed updating applied_chunks rows")?;

    // there will only be one of these for now
    for (epoch_id, dec) in epoch_decrements {
        sqlx::query!(
            "UPDATE epochs SET chunks_applied = chunks_applied - $1 WHERE id = $2;",
            dec,
            epoch_id,
        )
        .execute(db)
        .await
        .context("failed decrementing epoch table chunks_applied row")?;
    }
    Ok(())
}

pub(crate) async fn apply_chunks(
    db: &PgPool,
    neard_path: &Path,
    home_dir: &Path,
) -> anyhow::Result<()> {
    let mut neard = NeardPath::new(neard_path)?;
    neard.insert_version(db).await?;

    let quit = Arc::new(AtomicBool::new(false));
    let quit2 = quit.clone();
    let _sig = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        quit2.store(true, Ordering::Relaxed);
    });

    // TODO: parallelize
    loop {
        // hopefully nobody will update it without stopping this program first, but check it
        // to at least catch it earlyish if it happens
        // TODO: right now we just continue taking undone work with the new neard version.
        // Should redo old chunks with the new one
        neard.reload_and_insert(db).await?;
        let mut jobs = next_jobs(db, neard.version_hash).await?;
        if jobs.is_empty() {
            tracing::info!(target: "near-replayability", "no more jobs left to do");
            return Ok(());
        }

        for job in jobs.iter_mut() {
            let output = Command::new(neard_path)
                .arg("--unsafe-fast-startup")
                .arg("--home")
                .arg(home_dir)
                .arg("view-state")
                .arg("apply")
                .arg("--height")
                .arg(job.height.to_string())
                .arg("--shard-id")
                .arg(job.shard_id.to_string())
                .output()
                .with_context(|| {
                    format!("failed executing {} view-state apply", neard.path.display())
                })?;

            if output.status.success() {
                job.status = ApplyStatus::ok;
            } else {
                match String::from_utf8(output.stderr) {
                    Ok(stderr) => {
                        // this is pretty ugly but idk how else to do it for now
                        if stderr.contains("DBNotFoundErr(\"BLOCK HEIGHT:") {
                            job.status = ApplyStatus::height_missing;
                        } else {
                            job.status = ApplyStatus::failed;
                        }
                    }
                    Err(_) => job.status = ApplyStatus::failed,
                }
            }

            if quit.load(Ordering::Relaxed) {
                let mut still_pending = Vec::new();
                let mut finished = Vec::new();
                for j in jobs {
                    if j.status == ApplyStatus::pending {
                        still_pending.push(j);
                    } else {
                        finished.push(j);
                    }
                }
                if !still_pending.is_empty() {
                    delete_jobs(db, still_pending, neard.version_hash).await?;
                }
                if !finished.is_empty() {
                    update_jobs(db, finished, neard.version_hash).await?;
                }
                return Ok(());
            }
        }
        update_jobs(db, jobs, neard.version_hash).await?;
    }
}
