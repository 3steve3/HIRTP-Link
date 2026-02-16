# High-Integrity Resumable Transfer Protocol (HIRTP-Link)

[![Rust Client](https://img.shields.io/badge/Client-Rust-orange?logo=rust)](./Client)
[![C# Server](https://img.shields.io/badge/Server-C%23-blue?logo=dotnet)](./Server)
[![gRPC](https://img.shields.io/badge/Protocol-gRPC-green)](./ingestionsystem.proto)
[![Docker](https://img.shields.io/badge/Deploy-Docker_Compose-blue?logo=docker)](./docker-compose.yml)

HIRTP-Link is a data transfer protocol and proof-of-concept implementation designed for unreliable networks. It prioritizes data integrity and transfer resumption over raw throughput, making it suitable for scenarios where data loss is unacceptable, such as satellite downlinks or remote sensor data collection.

The system uses a dual-layer verification strategy: CRC32C for per-packet transport integrity and SHA256 for end-to-end file identity verification.

## Demonstration

The repository includes a Docker Compose configuration to simulate a large file transfer over an unstable link.

### Running the Simulation

Ensure Docker is installed and running.  
From the project root, run:

```bash
docker-compose up --build
```

This command will:

- Start the C# gRPC server.
- Start the Rust client.
- The client will automatically download a 120MB test image (NASA/Webb JADES TIFF) if it is not present.
- The client will begin streaming the file while a "Chaos Monkey" introduces random connection failures to test the protocol's recovery logic.

### Simulation Log Output

The logs demonstrate the protocol's ability to survive stream termination and resume without re-calculating file hashes.

```text
remote-client  | [System] Connecting to Base Station at http://base-station:50051...
remote-client  | [Cache-Miss] Calculating SHA256 identity for "jades4.tif"
remote-client  | [jades4.tif] Resuming session 2149021 at chunk 0
remote-client  | [ACK] Chunk 5 verified by base station
remote-client  | [ACK] Chunk 10 verified by base station
remote-client  | [!] CHAOS MONKEY: Severing gRPC stream intentionally...
remote-client  | [jades4.tif] Handshake attempt 2/50
base-station   | [RESUME] Client re-connected for file 8A3F21BC. Resuming from chunk 12
remote-client  | [jades4.tif] Resuming session 2149021 at chunk 12
...
base-station   | [COMPLETE] All parts received for 8A3F21BC. Reassembling...
base-station   | [VERIFIED] File 8A3F21BC is valid. Cleaning up parts.
remote-client  | [jades4.tif] Verified by Server. Deleting local copy.
```

## Technical Features

- **Client-side Identity Caching**: The file's SHA256 identity is cached by the client in memory. On reconnection, this cached identity is used for the handshake, avoiding expensive disk I/O for large files.  
- **Transactional Reassembly**: The server stores incoming data as discrete chunks. A final reassembly and SHA256 verification are performed only after all chunks have been received successfully.  
- **Resilience Testing**: The Rust client includes an optional "Chaos Monkey" that introduces random connection failures to validate the protocol's state machine and recovery mechanisms.  
- **Cross-Platform Implementation**: A resource-efficient Rust client suitable for edge devices and a high-throughput C# server compiled with Native AOT for minimal footprint.  

## Tech Stack

**Remote Client (Rust)**

- Framework: Tokio and Tonic for asynchronous gRPC streaming.  
- Integrity: sha2 for SHA256 identity and crc32fast for transport checksums.  
- Resilience: Contains the state machine for reconnection and resumption logic.  

**Base Station (C# Server)**

- Framework: ASP.NET Core 9.0, compiled with Native AOT for a small, self-contained native binary.  
- Storage: Manages chunk-based storage and atomic reassembly of final files.  
- State: Uses a ConcurrentDictionary to manage active transfer sessions in memory.  

## Protocol Definition

The core contract is defined in `ingestionsystem.proto`. It uses a wrapper message pattern to separate the transport checksum from the data payload.

```protobuf
syntax = "proto3";

package ingestionsystem;

option csharp_namespace = "HIRTP_Link.Protos";

service DataExchangeService {
  // Client introduces the file. Server returns a session-handle.
  rpc NegotiateStart (NegotiateRequest) returns (NegotiateResponse);
  
  // Bidirectional stream for high-integrity chunked transfer.
  rpc TransmitData (stream DataTransmit) returns (stream DataTransmitResponse);
}

// --- Negotiation Phase ---

message NegotiateRequest {
    bytes hash = 1; // CRC32C of NegotiateRequestPayload
    NegotiateRequestPayload payload = 2;
}

message NegotiateRequestPayload {
    bytes sha256_file_hash = 1;      // The Immutable Identity of the full file
    uint64 total_file_size_bytes = 2; 
    uint32 chunk_size = 3;         
}

message NegotiateResponse {
    bytes hash = 1; // CRC32C of NegotiateResponsePayload
    NegotiateResponsePayload payload = 2;
}

message NegotiateResponsePayload {
    bool transfer_complete = 1;   
    uint32 session_id = 2;        
    repeated int32 missing_chunks = 3; 
    uint32 start_at_chunk = 4;    
}

// --- Transmission Phase ---

message DataTransmit {
    bytes hash = 1; // CRC32C of DataTransmitPayload
    DataTransmitPayload payload = 2;
}

message DataTransmitPayload {
    uint32 session_id = 1;        
    uint32 chunk_number = 2;      
    bytes data = 3;              
}

message DataTransmitResponse {
    bytes hash = 1; // CRC32C of DataTransmitResponsePayload
    DataTransmitResponsePayload payload = 2;
}

message DataTransmitResponsePayload {
    uint32 chunk_number = 1;      
    bool ack = 2;                
    uint32 session_id = 3; 
}
```

## Project Structure

- `/Client`: The Rust implementation of the remote producer.  
- `/Server`: The C# implementation of the base station consumer.  
- `/ingestionsystem.proto`: Shared Protocol Buffer definition file (located in project root).  
- `docker-compose.yml`: Orchestration for the resilience demonstration.  
- `.dockerignore`: Configured to exclude build artifacts for fast container builds.  

## Future Enhancements

- Implement Mutual TLS (mTLS) for transport-layer security.  
- Add logic for dynamic adjustment of chunk size based on link quality metrics.  
- Support reassembly directly to cloud object storage (e.g., S3, Azure Blob).  
