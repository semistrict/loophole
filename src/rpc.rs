use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::store::{S3Access, Store};

// ---------------------------------------------------------------------------
// Protocol: JSON request written to .loophole/rpc, JSON response read back.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Snapshot {
        new_store_id: String,
    },
    Clone {
        continuation_id: String,
        clone_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    fn success() -> Self {
        Self {
            ok: true,
            error: None,
        }
    }

    fn err(e: anyhow::Error) -> Self {
        Self {
            ok: false,
            error: Some(format!("{e:#}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Server side: dispatch a request against a running Store.
// ---------------------------------------------------------------------------

pub async fn dispatch<S: S3Access>(store: &Store<S>, req: &[u8]) -> Vec<u8> {
    let resp = match serde_json::from_slice::<Request>(req) {
        Ok(req) => execute(store, req).await,
        Err(e) => Response::err(e.into()),
    };
    serde_json::to_vec(&resp).expect("Response serialization cannot fail")
}

async fn execute<S: S3Access>(store: &Store<S>, req: Request) -> Response {
    match req {
        Request::Snapshot { new_store_id } => match store.snapshot(new_store_id).await {
            Ok(()) => Response::success(),
            Err(e) => Response::err(e),
        },
        Request::Clone {
            continuation_id,
            clone_id,
        } => match store.clone_store(continuation_id, clone_id).await {
            Ok(()) => Response::success(),
            Err(e) => Response::err(e),
        },
    }
}

// ---------------------------------------------------------------------------
// Client side: open .loophole/rpc, write request, read response.
// ---------------------------------------------------------------------------

pub fn call(mountpoint: &Path, req: &Request) -> Result<Response> {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, Write};

    let rpc_path = mountpoint.join(".loophole").join("rpc");
    let req_bytes = serde_json::to_vec(req).context("serializing request")?;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&rpc_path)
        .with_context(|| format!("opening {}", rpc_path.display()))?;

    file.write_all(&req_bytes).context("writing request")?;

    // Seek back to 0 — the FUSE write handler stored the response at offset 0,
    // and the kernel tracks the file offset which has advanced past the write.
    file.seek(std::io::SeekFrom::Start(0))
        .context("seeking to start for response read")?;

    let mut resp_bytes = Vec::new();
    file.read_to_end(&mut resp_bytes)
        .context("reading response")?;

    serde_json::from_slice(&resp_bytes).context("parsing response")
}
