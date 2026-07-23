mod api;
pub mod capacity;
pub mod config;
mod scheduler;
pub mod store;

use std::path::Path;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

use anyhow::{Context, Result};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::UnixListener;
use tokio::sync::{Notify, RwLock, watch};
use tower::ServiceExt;

use capacity::CapacityManager;
use config::{Config, SANDBOX_AGENT_SHA256, SANDBOX_AGENT_URL};
use scheduler::CancellationRegistry;
use store::Store;

#[derive(Clone)]
pub struct ServiceState {
    pub config: Arc<Config>,
    pub store: Store,
    pub notify: Arc<Notify>,
    pub cancellations: CancellationRegistry,
}

pub async fn run(config: Config) -> Result<()> {
    prepare(&config).await?;
    let database_url = format!("sqlite://{}", config.daemon.database_path.display());
    let store = Store::open(&database_url).await?;
    let config = Arc::new(config);
    let state = ServiceState {
        config: config.clone(),
        store: store.clone(),
        notify: Arc::new(Notify::new()),
        cancellations: Arc::new(RwLock::new(std::collections::HashMap::new())),
    };
    let capacity = CapacityManager::new(config.sources.clone(), store.clone());
    let scheduler_context = scheduler::SchedulerContext {
        config: config.clone(),
        store,
        capacity,
        notify: state.notify.clone(),
        cancellations: state.cancellations.clone(),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut scheduler_task = tokio::spawn(scheduler::run(scheduler_context, shutdown_rx));
    let app = api::router(state);
    let listener = bind_socket(&config.daemon.socket_path).await?;
    tracing::info!(socket = %config.daemon.socket_path.display(), "thieving-eyesd ready");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.context("accept UDS client")?;
                let service = app.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let hyper_service = service_fn(move |request| service.clone().oneshot(request));
                    if let Err(error) = http1::Builder::new().serve_connection(io, hyper_service).await {
                        tracing::debug!(%error, "UDS HTTP connection ended");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("wait for shutdown signal")?;
                break;
            }
            result = &mut scheduler_task => {
                result.context("scheduler task join failed")??;
                anyhow::bail!("scheduler stopped unexpectedly");
            }
        }
    }
    let _ = shutdown_tx.send(true);
    scheduler_task
        .await
        .context("scheduler shutdown join failed")??;
    Ok(())
}

pub async fn prepare(config: &Config) -> Result<()> {
    config.validate()?;
    if let Some(parent) = config.daemon.database_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = config.daemon.socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    thieving_eyes_runtime_sandbox_agent::ensure_binary(
        &config.sandbox_agent_path(),
        SANDBOX_AGENT_URL,
        SANDBOX_AGENT_SHA256,
        config.runtime.download_if_missing,
    )
    .await?;
    Ok(())
}

async fn bind_socket(path: &Path) -> Result<UnixListener> {
    if tokio::fs::try_exists(path).await? {
        let metadata = tokio::fs::symlink_metadata(path).await?;
        if !metadata.file_type().is_socket() {
            anyhow::bail!("refusing to replace non-socket path {}", path.display());
        }
        tokio::fs::remove_file(path).await?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("bind daemon socket {}", path.display()))?;
    set_socket_permissions(path).await?;
    Ok(listener)
}

#[cfg(unix)]
async fn set_socket_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if !metadata.file_type().is_socket() {
        anyhow::bail!("daemon socket path is not a socket");
    }
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o600);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}
