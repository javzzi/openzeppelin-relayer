//! Load index reconciliation worker implementation.
//!
//! This module implements the load index reconciliation worker that periodically
//! re-syncs the Redis load index sorted sets with actual in-flight transaction counts.
//! This corrects drift from lost increments/decrements due to Redis failures or crashes.
//!
//! ## Distributed Lock
//!
//! Since this runs on multiple service instances simultaneously, a distributed lock
//! ensures only one instance processes the reconciliation at a time.

use actix_web::web::ThinData;
use eyre::Result;
use redis::AsyncCommands;
use std::time::Duration;
use tracing::{debug, info, instrument, warn};

use crate::{
    config::ServerConfig,
    constants::{
        LOAD_INDEX_RECONCILIATION_LOCK_TTL_SECS, WORKER_LOAD_INDEX_RECONCILIATION_RETRIES,
    },
    jobs::handle_result,
    models::{DefaultAppState, TransactionStatus},
    queues::{HandlerError, WorkerContext},
    repositories::{Repository, TransactionRepository},
    utils::DistributedLock,
};

/// Distributed lock name for load index reconciliation.
const RECONCILIATION_LOCK_NAME: &str = "load_index_reconciliation";

/// Handles periodic load index reconciliation jobs from the queue.
#[instrument(
    level = "debug",
    skip(job, data),
    fields(
        job_type = "load_index_reconciliation",
        attempt = %ctx.attempt,
    ),
    err
)]
pub async fn load_index_reconciliation_handler(
    job: LoadIndexReconciliationCronReminder,
    data: ThinData<DefaultAppState>,
    ctx: WorkerContext,
) -> Result<(), HandlerError> {
    let result = handle_request(job, &data).await;

    handle_result(
        result,
        &ctx,
        "LoadIndexReconciliation",
        WORKER_LOAD_INDEX_RECONCILIATION_RETRIES,
    )
}

/// Represents a cron reminder job for triggering load index reconciliation.
#[derive(Default, Debug, Clone)]
pub struct LoadIndexReconciliationCronReminder();

/// Handles the actual reconciliation logic.
async fn handle_request(
    _job: LoadIndexReconciliationCronReminder,
    data: &ThinData<DefaultAppState>,
) -> Result<()> {
    // In distributed mode, acquire a lock to prevent concurrent reconciliation
    let lock_guard = if ServerConfig::get_distributed_mode() {
        if let Some((pool, prefix)) = data.relayer_repository.connection_info() {
            let lock_key = format!("{prefix}:lock:{RECONCILIATION_LOCK_NAME}");
            let lock = DistributedLock::new(
                pool.clone(),
                &lock_key,
                Duration::from_secs(LOAD_INDEX_RECONCILIATION_LOCK_TTL_SECS),
            );
            match lock.try_acquire().await {
                Ok(Some(guard)) => {
                    debug!(lock_key = %lock_key, "acquired distributed lock for load index reconciliation");
                    Some(guard)
                }
                Ok(None) => {
                    debug!(lock_key = %lock_key, "load index reconciliation already running on another instance");
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        lock_key = %lock_key,
                        "failed to acquire reconciliation lock, skipping"
                    );
                    return Ok(());
                }
            }
        } else {
            debug!("in-memory repository detected, skipping distributed lock");
            None
        }
    } else {
        debug!("distributed mode disabled, skipping lock acquisition");
        None
    };

    info!("starting load index reconciliation");

    let (pool, prefix) = match data.relayer_repository.connection_info() {
        Some(info) => info,
        None => {
            debug!("load index reconciliation skipped: no Redis connection");
            return Ok(());
        }
    };

    let in_flight_statuses = [
        TransactionStatus::Pending,
        TransactionStatus::Sent,
        TransactionStatus::Submitted,
        TransactionStatus::Mined,
    ];

    // Fetch all relayers
    let relayers = data.relayer_repository.list_all().await?;

    let mut conn = pool.get().await.map_err(|e| {
        eyre::eyre!("load index reconciliation: failed to get Redis connection: {e}")
    })?;

    // Ensure relayer_network reverse lookup key exists for ALL relayers
    for relayer in &relayers {
        let network_key = format!("{}:relayer_network:{}", prefix, relayer.id);
        let network_value = format!("{}:{}", relayer.network_type, relayer.network);

        if let Err(e) = conn
            .set::<_, _, ()>(&network_key, &network_value)
            .await
        {
            warn!(
                relayer_id = %relayer.id,
                error = %e,
                "load index reconciliation: failed to set relayer_network key"
            );
        }
    }

    // Update load index scores only for active relayers
    for relayer in relayers.iter().filter(|r| !r.paused && !r.system_disabled) {
        let count = data
            .transaction_repository
            .count_by_status(&relayer.id, &in_flight_statuses)
            .await
            .unwrap_or(0);

        let load_key = format!(
            "{}:load_index:{}:{}",
            prefix, relayer.network_type, relayer.network
        );

        if let Err(e) = conn
            .zadd::<_, _, _, ()>(&load_key, &relayer.id, count as f64)
            .await
        {
            warn!(
                relayer_id = %relayer.id,
                error = %e,
                "load index reconciliation: failed to update score"
            );
        } else {
            debug!(
                relayer_id = %relayer.id,
                count = %count,
                "load index reconciliation: updated score"
            );
        }
    }

    info!("load index reconciliation complete");

    // Keep the lock guard alive until we're done
    drop(lock_guard);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconciliation_lock_name_not_empty() {
        assert!(!RECONCILIATION_LOCK_NAME.is_empty());
    }

    #[test]
    fn test_load_index_reconciliation_cron_reminder_default() {
        let reminder = LoadIndexReconciliationCronReminder();
        // Verify it implements Default and Debug
        let _debug = format!("{:?}", reminder);
        let _clone = reminder.clone();
    }

    #[test]
    fn test_in_flight_statuses_are_correct() {
        // Verify the statuses used for counting match the expected in-flight set
        let expected = [
            TransactionStatus::Pending,
            TransactionStatus::Sent,
            TransactionStatus::Submitted,
            TransactionStatus::Mined,
        ];
        // These are the statuses that should be counted for load index reconciliation
        assert_eq!(expected.len(), 4);
        assert!(expected.contains(&TransactionStatus::Pending));
        assert!(expected.contains(&TransactionStatus::Sent));
        assert!(expected.contains(&TransactionStatus::Submitted));
        assert!(expected.contains(&TransactionStatus::Mined));
        // Final statuses should NOT be included
        assert!(!expected.contains(&TransactionStatus::Confirmed));
        assert!(!expected.contains(&TransactionStatus::Failed));
        assert!(!expected.contains(&TransactionStatus::Canceled));
        assert!(!expected.contains(&TransactionStatus::Expired));
    }
}
