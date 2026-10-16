//! Length-prefixed TCP writes to bots.

use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub(crate) async fn write_prefixed(writer: &mut OwnedWriteHalf, payload: &[u8]) -> std::io::Result<()> {
    writer.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    writer.write_all(payload).await
}

pub(crate) async fn write_to_bots(
    writers: impl IntoIterator<Item = Arc<Mutex<OwnedWriteHalf>>>,
    payload: &[u8],
) {
    for writer in writers {
        let mut writer_guard = writer.lock().await;
        if let Err(e) = write_prefixed(&mut *writer_guard, payload).await {
            tracing::error!(?e, "Failed to write to socket");
        }
    }
}

pub(crate) async fn write_to_bot(writer: &Arc<Mutex<OwnedWriteHalf>>, payload: &[u8]) {
    let mut writer_guard = writer.lock().await;
    if let Err(e) = write_prefixed(&mut *writer_guard, payload).await {
        tracing::error!(?e, "Failed to write to socket");
    }
}
