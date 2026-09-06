// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Minimal Beryl Rust Client CRUD roundtrip.

use std::error::Error;
use std::io;
use std::path::PathBuf;

use beryl_client::{ClientConfig, FsClient};
use bytes::Bytes;

const DIRECTORY: &str = "/examples";
const FILE: &str = "/examples/rust-client-crud.bin";
const BLOCK_SIZE: u32 = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("conf/client.yaml"));
    let client = FsClient::new(ClientConfig::load(config_path)?)?;

    client.mkdirs(DIRECTORY).await?;

    let payload = Bytes::from(
        (0..2 * BLOCK_SIZE as usize + 17)
            .map(|index| ((index * 31 + 17) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let mut writer = client.create(FILE).await?;
    writer.write_all(payload.clone()).await?;
    writer.close().await?;

    let status = client.get_status(FILE).await?;
    if status.len != payload.len() as u64 {
        return Err(io::Error::other(format!(
            "stat size mismatch: expected {}, got {}",
            payload.len(),
            status.len
        ))
        .into());
    }

    let actual = client.open(FILE).await?.read_to_end().await?;
    if actual != payload {
        return Err(io::Error::other("read content mismatch").into());
    }

    client.delete(FILE).await?;
    println!("Rust Client CRUD roundtrip succeeded: {FILE}");
    Ok(())
}
