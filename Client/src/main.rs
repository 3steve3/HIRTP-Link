use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io::SeekFrom;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use prost::Message;
use sha2::{Digest, Sha256};

// Import generated gRPC code
pub mod ingestionsystem {
    tonic::include_proto!("ingestionsystem");
}
use ingestionsystem::{
    data_exchange_service_client::DataExchangeServiceClient,
    NegotiateRequest, NegotiateRequestPayload, DataTransmit, DataTransmitPayload
};

// --- CONFIGURATION ---
const CHUNK_SIZE: usize = 128 * 1024; // 128KB
const IDLE_POLLING_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\x1b[1;36m--- HIRTP.Link Production Client (Rust) ---\x1b[0m");

    // 1. Establish gRPC Channel (Docker-friendly)
    let addr = std::env::var("SERVER_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    println!("[System] Connecting to Base Station at {}...", addr);
    
    let channel = Channel::from_shared(addr)?.connect().await?;
    let mut client = DataExchangeServiceClient::new(channel);
    
    let pending_dir = "./pending";
    if !Path::new(pending_dir).exists() {
        tokio::fs::create_dir_all(pending_dir).await?;
    }

    // 2. Identity Cache: Maps Path -> (SHA256, Size, Last Modified)
    // Prevents re-hashing large files after every network disconnect
    let mut identity_cache: HashMap<PathBuf, (Vec<u8>, u64, SystemTime)> = HashMap::new();

    loop {
        let mut entries = tokio::fs::read_dir(pending_dir).await?;
        let mut files_found = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() { continue; }
            files_found += 1;

            let metadata = entry.metadata().await?;
            let mtime = metadata.modified().unwrap_or(SystemTime::now());
            let size = metadata.len();

            // 3. Resolve Identity (Cache or Compute)
            let (file_hash, total_size) = if let Some((h, s, t)) = identity_cache.get(&path) {
                if *t == mtime && *s == size {
                    (h.clone(), *s) // Cache hit
                } else {
                    compute_and_cache(&path, &mut identity_cache, mtime).await?
                }
            } else {
                compute_and_cache(&path, &mut identity_cache, mtime).await?
            };

            // 4. Manage Transfer
            match manage_file_transfer(&mut client, path.clone(), file_hash, total_size).await {
                Ok(_) => {
                    identity_cache.remove(&path); // Clean up cache on success
                }
                Err(e) => {
                    eprintln!("\x1b[1;33m[!] Session paused for {:?}: {}\x1b[0m", path.file_name(), e);
                    // Keep in cache to resume instantly on next loop
                }
            }
        }

        if files_found == 0 {
            sleep(IDLE_POLLING_DELAY).await; 
        }
    }
}

async fn compute_and_cache(
    path: &PathBuf, 
    cache: &mut HashMap<PathBuf, (Vec<u8>, u64, SystemTime)>,
    mtime: SystemTime
) -> Result<(Vec<u8>, u64), Box<dyn std::error::Error>> {
    println!("\x1b[1;35m[Cache-Miss]\x1b[0m Calculating SHA256 identity for {:?}", path.file_name());
    let (hash, size) = compute_file_info(path).await?;
    cache.insert(path.clone(), (hash.clone(), size, mtime));
    Ok((hash, size))
}

async fn manage_file_transfer(
    client: &mut DataExchangeServiceClient<Channel>,
    path: PathBuf,
    file_hash: Vec<u8>,
    total_size: u64
) -> Result<(), Box<dyn std::error::Error>> {
    let filename = path.file_name().unwrap().to_string_lossy().to_string();

    // App-level handshake
    let handshake = negotiate(client, &file_hash, total_size).await?;
    
    if handshake.transfer_complete {
        println!("\x1b[1;32m[{}] Verified by Server. Deleting local copy.\x1b[0m", filename);
        tokio::fs::remove_file(&path).await?;
        return Ok(());
    }

    println!("\x1b[1;34m[{}]\x1b[0m Resuming session {} at chunk {}", 
        filename, handshake.session_id, handshake.start_at_chunk);

    // Stream data
    transmit_data(client, &path, handshake.session_id, handshake.start_at_chunk).await?;

    // In a prod system, we loop back to handshake one last time to confirm completion
    Err("Stream cycle finished. Re-verifying...".into())
}

async fn negotiate(
    client: &mut DataExchangeServiceClient<Channel>,
    file_hash: &[u8],
    total_size: u64
) -> Result<ingestionsystem::NegotiateResponsePayload, Box<dyn std::error::Error>> {
    let payload = NegotiateRequestPayload {
        sha256_file_hash: file_hash.to_vec(),
        total_file_size_bytes: total_size,
        chunk_size: CHUNK_SIZE as u32,
    };

    let request = NegotiateRequest {
        hash: compute_crc(&payload),
        payload: Some(payload),
    };

    let response = client.negotiate_start(request).await?.into_inner();
    response.payload.ok_or_else(|| "Server sent empty response".into())
}

async fn transmit_data(
    client: &mut DataExchangeServiceClient<Channel>,
    path: &Path,
    session_id: u32,
    start_chunk: u32
) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = mpsc::channel(10);
    let path_buf = path.to_path_buf();

    // --- SENDER TASK (with Chaos Monkey) ---
    tokio::spawn(async move {
        let mut file = tokio::fs::File::open(&path_buf).await.expect("File access lost");
        let _ = file.seek(SeekFrom::Start((start_chunk as u64) * (CHUNK_SIZE as u64))).await;
        
        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut chunk_num = start_chunk;

        loop {
            // THE CHAOS MONKEY (2% failure rate per chunk for demo visibility)
            if rand::random::<f64>() < 0.02 {
                println!("\x1b[1;31m[!] CHAOS MONKEY: Severing gRPC stream intentionally...\x1b[0m");
                return; 
            }

            let n = match file.read(&mut buffer).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            };

            let payload = DataTransmitPayload {
                session_id,
                chunk_number: chunk_num,
                data: buffer[..n].to_vec(),
            };

            let msg = DataTransmit {
                hash: compute_crc(&payload),
                payload: Some(payload),
            };

            if tx.send(msg).await.is_err() { break; }
            chunk_num += 1;
        }
    });

    // --- RECEIVER TASK (ACKs) ---
    let request_stream = ReceiverStream::new(rx);
    let mut response_stream = client.transmit_data(request_stream).await?.into_inner();

    while let Some(resp) = response_stream.message().await? {
        if let Some(payload) = resp.payload {
            if !payload.ack {
                return Err(format!("Server NACK for chunk {}", payload.chunk_number).into());
            }
            if payload.chunk_number % 5 == 0 {
                println!("\x1b[1;32m[ACK]\x1b[0m Chunk {} verified by base station", payload.chunk_number);
            }
        }
    }

    Ok(())
}

async fn compute_file_info(path: &Path) -> Result<(Vec<u8>, u64), Box<dyn std::error::Error>> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut total_size = 0;

    while let Ok(n) = file.read(&mut buffer).await {
        if n == 0 { break; }
        hasher.update(&buffer[..n]);
        total_size += n as u64;
    }
    Ok((hasher.finalize().to_vec(), total_size))
}

fn compute_crc<T: Message>(msg: &T) -> Vec<u8> {
    let mut hasher = crc32fast::Hasher::new();
    let bytes = msg.encode_to_vec();
    hasher.update(&bytes);
    hasher.finalize().to_le_bytes().to_vec()
}