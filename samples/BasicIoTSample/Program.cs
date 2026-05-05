using EdgeSync;

// ── Sample: Basic IoT Camera Event Pipeline ─────────────────────────────────
// Demonstrates EdgeSyncBuffer for a physical security camera system.
// Shows offline-first write, connectivity simulation, and ordered delivery.

Console.WriteLine("EdgeSyncBuffer — Basic IoT Sample");
Console.WriteLine("===================================\n");

// 1. Define your telemetry type
// Any record/class that implements ITimestamped works
var options = new EdgeSyncOptions
{
    MaxCapacity = 10_000,
    BatchSize = 100,
    PersistPath = Path.Combine(Path.GetTempPath(), "camera_events.jsonl"),
};

// 2. Define your cloud upload function
// Replace this with your actual Azure IoT Hub, AWS IoT Core, or REST API call
Task<SyncResult> UploadToCloud(IEnumerable<CameraEvent> batch, CancellationToken ct)
{
    var events = batch.ToList();
    Console.WriteLine($"  [CLOUD] Uploading {events.Count} events " +
                      $"(seq #{events.Min(e => e.SequenceId)}–#{events.Max(e => e.SequenceId)})");
    // Simulate network call
    return Task.FromResult(SyncResult.Ok());
}

// 3. Create the buffer with optional console logger
await using var buffer = new EdgeSyncBuffer<CameraEvent>(
    options,
    UploadToCloud,
    logger: new ConsoleEdgeSyncLogger());

// Subscribe to dead-letter alerts
buffer.OnDeadLetterThresholdExceeded += stats =>
{
    Console.WriteLine($"  [ALERT] Dead-letter threshold exceeded! " +
                      $"Eviction rate: {stats.EvictionRate}%");
    return Task.CompletedTask;
};

// 4. Simulate devices writing while offline ──────────────────────────────────
Console.WriteLine("Phase 1: Writing events while OFFLINE");
Console.WriteLine("--------------------------------------");

for (int i = 1; i <= 25; i++)
{
    await buffer.WriteAsync(new CameraEvent(
        CameraId: $"CAM{(i % 5) + 1:000}",
        EventType: i % 3 == 0 ? "MotionDetected" : "Heartbeat",
        Timestamp: DateTime.UtcNow,
        Confidence: Random.Shared.NextSingle(),
        SequenceId: i));

    if (i % 5 == 0)
        Console.WriteLine($"  Written {i} events. Pending: {buffer.PendingCount}");
}

Console.WriteLine($"\nAll 25 events buffered. Buffer size: {buffer.PendingCount}");
Console.WriteLine($"Stats: {buffer.Stats}\n");

// 5. Simulate connectivity restored ─────────────────────────────────────────
Console.WriteLine("Phase 2: Connectivity RESTORED — syncing");
Console.WriteLine("----------------------------------------");
await buffer.OnConnectivityRestoredAsync();

Console.WriteLine($"\nSync complete. Pending: {buffer.PendingCount}");
Console.WriteLine($"Final stats: {buffer.Stats}");

// 6. Back online — writes sync immediately ──────────────────────────────────
Console.WriteLine("\nPhase 3: Writing while ONLINE (immediate sync)");
Console.WriteLine("----------------------------------------------");

for (int i = 26; i <= 30; i++)
{
    await buffer.WriteAsync(new CameraEvent(
        $"CAM{(i % 5) + 1:000}", "Motion", DateTime.UtcNow, 0.95f, i));
}

await Task.Delay(100); // Allow async sync to complete
Console.WriteLine($"Pending after online writes: {buffer.PendingCount}");
Console.WriteLine($"\nFinal stats: {buffer.Stats}");

// Cleanup temp file
if (File.Exists(options.PersistPath))
    File.Delete(options.PersistPath);

Console.WriteLine("\nSample complete.");

// ── Camera event model ───────────────────────────────────────────────────────
public record CameraEvent(
    string CameraId,
    string EventType,
    DateTime Timestamp,
    float Confidence,
    int SequenceId) : ITimestamped;
