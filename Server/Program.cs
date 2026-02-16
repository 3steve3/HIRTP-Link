using Google.Protobuf;
using Grpc.Core;
using HIRTP_Link.Protos;
using System.Collections.Concurrent;
using System.IO.Hashing;
using System.Security.Cryptography;

var builder = WebApplication.CreateSlimBuilder(args);
builder.Services.AddGrpc();
builder.Services.AddSingleton<SessionManager>(); // State helper

var app = builder.Build();

app.MapGrpcService<DataExchangeServiceImpl>();
app.MapGet("/", () => "Communication must be via gRPC protocol.");

app.Run();

// --- Data Models ---

public record TransferSession(
    string FileHashHex,
    ulong TotalSizeBytes,
    uint ChunkSize,
    string IncompleteDirPath,
    int ExpectedChunks
);

public class SessionManager
{
    public ConcurrentDictionary<uint, TransferSession> ActiveSessions { get; } = new();
}

// --- Service Implementation ---

public class DataExchangeServiceImpl : DataExchangeService.DataExchangeServiceBase
{
    private readonly string _baseStoragePath = Path.Combine(AppContext.BaseDirectory, "Storage");
    private readonly ILogger<DataExchangeServiceImpl> _logger;
    private readonly SessionManager _sessions;

    public DataExchangeServiceImpl(ILogger<DataExchangeServiceImpl> logger, SessionManager sessions)
    {
        _logger = logger;
        _sessions = sessions;
        Directory.CreateDirectory(Path.Combine(_baseStoragePath, "Incomplete"));
        Directory.CreateDirectory(Path.Combine(_baseStoragePath, "Final"));
    }

    public override Task<NegotiateResponse> NegotiateStart(NegotiateRequest request, ServerCallContext context)
    {
        if (!VerifyCrc32(request.Payload, request.Hash.ToByteArray()))
            throw new RpcException(new Status(StatusCode.DataLoss, "Negotiation CRC Mismatch"));

        var fileHashHex = Convert.ToHexString(request.Payload.Sha256FileHash.ToByteArray());
        var finalPath = Path.Combine(_baseStoragePath, "Final", fileHashHex);
        var incompleteDirPath = Path.Combine(_baseStoragePath, "Incomplete", fileHashHex);

        var responsePayload = new NegotiateResponsePayload();

        if (File.Exists(finalPath))
        {
            responsePayload.TransferComplete = true;
            _logger.LogInformation("\x1b[1;32m[ALREADY-EXISTS]\x1b[0m {hash}", fileHashHex[..8]);
        }
        else
        {
            Directory.CreateDirectory(incompleteDirPath);

            // Resume Logic: Count existing parts
            var existingParts = Directory.GetFiles(incompleteDirPath, "part_*").Length;

            uint sessionId = (uint)fileHashHex.GetHashCode(); // Stable for this run
            int expectedChunks = (int)Math.Ceiling((double)request.Payload.TotalFileSizeBytes / request.Payload.ChunkSize);

            // Register session in memory so TransmitData knows what to do
            _sessions.ActiveSessions[sessionId] = new TransferSession(
                fileHashHex,
                request.Payload.TotalFileSizeBytes,
                request.Payload.ChunkSize,
                incompleteDirPath,
                expectedChunks
            );

            responsePayload.SessionId = sessionId;
            responsePayload.StartAtChunk = (uint)existingParts;
            responsePayload.TransferComplete = false;

            _logger.LogInformation("\x1b[1;34m[NEGOTIATE]\x1b[0m Session {id} for {hash}. Resuming from {chunk}",
                sessionId, fileHashHex[..8], existingParts);
        }

        return Task.FromResult(new NegotiateResponse
        {
            Payload = responsePayload,
            Hash = ComputeCrc32(responsePayload)
        });
    }

    public override async Task TransmitData(
        IAsyncStreamReader<DataTransmit> requestStream,
        IServerStreamWriter<DataTransmitResponse> responseStream,
        ServerCallContext context)
    {
        while (await requestStream.MoveNext())
        {
            var message = requestStream.Current;

            // 1. Verify CRC
            if (!VerifyCrc32(message.Payload, message.Hash.ToByteArray()))
            {
                await responseStream.WriteAsync(CreateAck(message.Payload.ChunkNumber, message.Payload.SessionId, false));
                continue;
            }

            // 2. Locate Session
            if (!_sessions.ActiveSessions.TryGetValue(message.Payload.SessionId, out var session))
            {
                _logger.LogWarning("Unknown SessionId {id}", message.Payload.SessionId);
                return;
            }

            // 3. Write Part to Disk
            string partPath = Path.Combine(session.IncompleteDirPath, $"part_{message.Payload.ChunkNumber}");
            await File.WriteAllBytesAsync(partPath, message.Payload.Data.ToByteArray());

            // 4. Send ACK
            await responseStream.WriteAsync(CreateAck(message.Payload.ChunkNumber, message.Payload.SessionId, true));

            // 5. Check for Completion
            var currentPartsCount = Directory.GetFiles(session.IncompleteDirPath, "part_*").Length;
            if (currentPartsCount >= session.ExpectedChunks)
            {
                _logger.LogInformation("\x1b[1;35m[COMPLETE]\x1b[0m All parts received for {hash}. Reassembling...", session.FileHashHex[..8]);
                await TryReassembleFile(session);
            }
        }
    }

    private async Task TryReassembleFile(TransferSession session)
    {
        string finalPath = Path.Combine(_baseStoragePath, "Final", session.FileHashHex);

        try
        {
            using (var finalFile = File.Create(finalPath))
            {
                for (int i = 0; i < session.ExpectedChunks; i++)
                {
                    string partPath = Path.Combine(session.IncompleteDirPath, $"part_{i}");
                    using (var partFile = File.OpenRead(partPath))
                    {
                        await partFile.CopyToAsync(finalFile);
                    }
                }
            }

            // Final Integrity Check
            byte[] finalData = await File.ReadAllBytesAsync(finalPath);
            byte[] actualHash = SHA256.HashData(finalData);
            string actualHashHex = Convert.ToHexString(actualHash);

            if (actualHashHex.Equals(session.FileHashHex, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("\x1b[1;32m[VERIFIED]\x1b[0m File {hash} is valid. Cleaning up parts.", session.FileHashHex[..8]);
                Directory.Delete(session.IncompleteDirPath, true);
                _sessions.ActiveSessions.TryRemove((uint)session.FileHashHex.GetHashCode(), out _);
            }
            else
            {
                _logger.LogError("\x1b[1;31m[CORRUPTED]\x1b[0m SHA256 mismatch for {hash}!", session.FileHashHex[..8]);
                File.Delete(finalPath); // Remove the bad file
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Reassembly failed");
        }
    }

    #region Integrity Helpers

    private bool VerifyCrc32(IMessage payload, byte[] providedHash)
    {
        var actualHash = new byte[4];
        Crc32.Hash(payload.ToByteArray(), actualHash);
        if (!BitConverter.IsLittleEndian) Array.Reverse(actualHash);
        return providedHash.SequenceEqual(actualHash);
    }

    private ByteString ComputeCrc32(IMessage payload)
    {
        var hash = new byte[4];
        Crc32.Hash(payload.ToByteArray(), hash);
        if (!BitConverter.IsLittleEndian) Array.Reverse(hash);
        return ByteString.CopyFrom(hash);
    }

    private DataTransmitResponse CreateAck(uint chunkNum, uint sessionId, bool success)
    {
        var payload = new DataTransmitResponsePayload { ChunkNumber = chunkNum, SessionId = sessionId, Ack = success };
        return new DataTransmitResponse { Payload = payload, Hash = ComputeCrc32(payload) };
    }

    #endregion
}