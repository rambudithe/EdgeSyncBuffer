# EdgeSyncBuffer

**Offline-first IoT data pipeline for .NET** — guaranteed ordered delivery despite connectivity interruptions.

[![NuGet](https://img.shields.io/nuget/v/EdgeSync.Buffer.svg)](https://www.nuget.org/packages/EdgeSync.Buffer)
[![NuGet Downloads](https://img.shields.io/nuget/dt/EdgeSync.Buffer.svg)](https://www.nuget.org/packages/EdgeSync.Buffer)
[![CI](https://github.com/rambudithe/EdgeSyncBuffer/actions/workflows/ci.yml/badge.svg)](https://github.com/rambudithe/EdgeSyncBuffer/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## The Problem

When an IoT device at a branch location loses connectivity to the cloud, what happens to the telemetry it generates?

In most .NET IoT deployments — **it is silently dropped.**

`MQTTnet`'s `ManagedClient` reconnects automatically, but there is no built-in mechanism to:
- Buffer messages locally during outages
- Persist them to disk across process restarts
- Replay them in **guaranteed chronological order** when connectivity returns

In compliance-sensitive environments — financial institution surveillance, healthcare monitoring, industrial safety — that silent loss has real regulatory and operational consequences.

---

## The Solution

`EdgeSyncBuffer` provides an offline-first write guarantee:

```
Write always succeeds locally
         ↓
Entries buffered in-memory + persisted to disk
         ↓
Connectivity restored → all entries delivered to cloud
         ↓
Guaranteed sequence-number ordering — not FIFO, not timestamp order
```

### Why sequence numbers instead of timestamps?

Clock drift at edge devices during connectivity outages is common. A device that reconnects after an NTP correction may have timestamps that appear out of sequence. **Sequence numbers are assigned at write time and are immune to clock drift** — ensuring correct ordering regardless of device clock state.

This is the key insight missing from most IoT buffer implementations.

---

## Installation

```bash
dotnet add package EdgeSync.Buffer
```

Requires **.NET 8.0** or later.

---

## Quick Start

```csharp
using EdgeSync;

// 1. Define your telemetry type
public record CameraEvent(
    string CameraId,
    string EventType,
    DateTime Timestamp,
    float Confidence) : ITimestamped;

// 2. Define your cloud upload function
Task<SyncResult> UploadToCloud(IEnumerable<CameraEvent> batch, CancellationToken ct)
{
    // Your Azure IoT Hub, AWS IoT Core, or REST API call here
    await myCloudService.SendBatchAsync(batch, ct);
    return SyncResult.Ok();
}

// 3. Create the buffer
await using var buffer = new EdgeSyncBuffer<CameraEvent>(
    options: new EdgeSyncOptions
    {
        MaxCapacity = 100_000,
        BatchSize   = 500,
        PersistPath = "/var/iot/camera_events.jsonl", // survives restarts
    },
    uploadFn: UploadToCloud,
    logger:   new ConsoleEdgeSyncLogger());

// 4. Write — always succeeds, even when offline
await buffer.WriteAsync(new CameraEvent("CAM001", "MotionDetected", DateTime.UtcNow, 0.95f));

// 5. When connectivity returns — ordered sync happens automatically
await buffer.OnConnectivityRestoredAsync();

// 6. When connectivity is lost — subsequent writes buffer locally
buffer.OnConnectivityLost();
```

---

## Configuration

```csharp
var options = new EdgeSyncOptions
{
    // Maximum entries in memory before oldest are evicted
    MaxCapacity = 100_000,          // default

    // Entries per cloud upload batch
    BatchSize = 500,                // default

    // JSON Lines file for disk persistence (null = memory-only)
    PersistPath = null,             // default — set a path to enable

    // Evictions before dead-letter alert fires
    DeadLetterThreshold = 1_000,    // default
};

// Convenience factory methods
var memoryOnly   = EdgeSyncOptions.Default;
var withDisk     = EdgeSyncOptions.WithPersistence("/var/iot/buffer.jsonl");
```

---

## Dead-Letter Alerting

```csharp
// Raise an alert when entries are being evicted (buffer pressure)
buffer.OnDeadLetterThresholdExceeded += async stats =>
{
    await alertService.SendAsync(
        $"IoT buffer under pressure: {stats.EvictionRate}% eviction rate. " +
        $"Pending: {stats.CurrentPending}");
};
```

---

## Statistics

```csharp
var stats = buffer.Stats;

Console.WriteLine($"Written:  {stats.TotalWritten}");
Console.WriteLine($"Synced:   {stats.TotalSynced}");
Console.WriteLine($"Evicted:  {stats.TotalEvicted}");
Console.WriteLine($"Pending:  {stats.CurrentPending}");
Console.WriteLine($"SyncRate: {stats.SyncRate}%");
```

---

## Custom Logging

Implement `IEdgeSyncLogger` to route to your existing logging infrastructure:

```csharp
// With Microsoft.Extensions.Logging
public class MsExtLogger<T> : IEdgeSyncLogger
{
    private readonly ILogger<T> _inner;
    public MsExtLogger(ILogger<T> logger) => _inner = logger;
    public void LogDebug(string msg)       => _inner.LogDebug(msg);
    public void LogInformation(string msg) => _inner.LogInformation(msg);
    public void LogWarning(string msg)     => _inner.LogWarning(msg);
    public void LogError(string msg)       => _inner.LogError(msg);
}
```

---

## Design Decisions

### Why JSON Lines for persistence?

- Zero dependencies beyond `System.Text.Json` (already in .NET)
- Append-only — each write is a single file operation
- Human-readable for debugging
- Simple compaction — rewrite file minus synced entries

For high-throughput scenarios, replace with SQLite via `Microsoft.Data.Sqlite`.

### Why `ConcurrentQueue<T>` not `Channel<T>`?

`Channel<T>` is excellent for producer/consumer pipelines but requires a dedicated consumer task. `ConcurrentQueue<T>` allows the buffer to be written from any thread without a background consumer — matching IoT scenarios where writes come from device event callbacks on arbitrary threads.

### Why not use the cloud SDK's built-in retry?

Cloud SDK retries address transient HTTP failures. EdgeSyncBuffer addresses **connectivity outages** — periods where no network path exists. These are different failure modes requiring different solutions.

---

## Contributing

Issues and pull requests are welcome. Please open an issue describing the problem before submitting a PR for significant changes.

```bash
# Run tests
dotnet test

# Build package locally
dotnet pack src/EdgeSyncBuffer/EdgeSync.Buffer.csproj --configuration Release
```

---

## Related Articles

- [Offline-First IoT Data Pipelines in .NET](https://infoq.com) — InfoQ
- [Building Resilient IoT Infrastructure for Enterprise Deployments](https://dzone.com) — DZone

---

## License

MIT — see [LICENSE](LICENSE)

---

## Author

**Ram Budithe** — Lead Software Engineer, Global Security Technology  
[LinkedIn](https://linkedin.com/in/rambudithe) · [GitHub](https://github.com/rambudithe)
