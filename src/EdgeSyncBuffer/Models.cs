namespace EdgeSync;

// ── Core interface ──────────────────────────────────────────────────────────

/// <summary>
/// All telemetry types must implement this interface.
/// The timestamp is stored for context but ordering uses sequence numbers.
/// </summary>
public interface ITimestamped
{
    /// <summary>UTC time the event occurred at the device.</summary>
    DateTime Timestamp { get; }
}

// ── Internal entry wrapper ──────────────────────────────────────────────────

/// <summary>
/// Internal wrapper that adds a monotonically increasing sequence number
/// to each entry. Sequence numbers — not timestamps — drive delivery order.
///
/// Why not timestamps? Clock drift at edge devices during connectivity outages
/// is common. A device that reconnects after an NTP correction may have
/// timestamps that appear out of sequence. Sequence numbers are assigned
/// server-side at write time and are immune to clock drift.
/// </summary>
public record BufferedEntry<T>(
    T Data,
    DateTime BufferedAt,
    long SequenceNumber);

// ── Configuration ───────────────────────────────────────────────────────────

/// <summary>
/// Configuration options for EdgeSyncBuffer.
/// </summary>
public class EdgeSyncOptions
{
    /// <summary>
    /// Maximum number of entries to hold in memory before evicting oldest.
    /// Default: 100,000 entries.
    /// </summary>
    public int MaxCapacity { get; init; } = 100_000;

    /// <summary>
    /// Number of entries to upload to the cloud in each sync batch.
    /// Larger batches are more efficient but increase retry cost on failure.
    /// Default: 500 entries per batch.
    /// </summary>
    public int BatchSize { get; init; } = 500;

    /// <summary>
    /// Optional path to a JSON Lines file for disk persistence.
    /// When set, entries survive process restarts.
    /// Set to null to disable disk persistence (memory-only mode).
    /// Default: null (memory-only).
    /// </summary>
    public string? PersistPath { get; init; }

    /// <summary>
    /// Number of evictions after which OnDeadLetterThresholdExceeded is raised.
    /// Set to 0 to disable dead-letter alerting.
    /// Default: 1000.
    /// </summary>
    public int DeadLetterThreshold { get; init; } = 1_000;

    /// <summary>Default in-memory only configuration.</summary>
    public static EdgeSyncOptions Default => new();

    /// <summary>Configuration with disk persistence enabled.</summary>
    public static EdgeSyncOptions WithPersistence(string path) =>
        new() { PersistPath = path };
}

// ── Upload result ───────────────────────────────────────────────────────────

/// <summary>
/// Result returned by your cloud upload function.
/// Return Success = true when the cloud has durably accepted the batch.
/// Return Success = false on any transient failure — entries will be re-queued.
/// </summary>
public record SyncResult(bool Success, string? ErrorMessage = null)
{
    /// <summary>Creates a successful sync result.</summary>
    public static SyncResult Ok() => new(true);

    /// <summary>Creates a failed sync result with an error message.</summary>
    public static SyncResult Fail(string reason) => new(false, reason);
}

// ── Statistics ──────────────────────────────────────────────────────────────

/// <summary>Lifetime statistics for a buffer instance.</summary>
public record BufferStats(
    long TotalWritten,
    long TotalSynced,
    long TotalEvicted,
    int CurrentPending)
{
    /// <summary>Percentage of written entries successfully synced.</summary>
    public double SyncRate => TotalWritten == 0 ? 0 :
        Math.Round((double)TotalSynced / TotalWritten * 100, 1);

    /// <summary>Percentage of written entries lost due to eviction.</summary>
    public double EvictionRate => TotalWritten == 0 ? 0 :
        Math.Round((double)TotalEvicted / TotalWritten * 100, 1);
}

// ── Logger interface ────────────────────────────────────────────────────────

/// <summary>
/// Optional logging interface. Implement this to route EdgeSyncBuffer
/// log messages to your existing logging infrastructure (Serilog,
/// Microsoft.Extensions.Logging, etc.)
/// </summary>
public interface IEdgeSyncLogger
{
    void LogDebug(string message);
    void LogInformation(string message);
    void LogWarning(string message);
    void LogError(string message);
}

/// <summary>
/// Simple console logger for development and testing.
/// </summary>
public sealed class ConsoleEdgeSyncLogger : IEdgeSyncLogger
{
    public void LogDebug(string message) =>
        Console.WriteLine($"[EdgeSync DBG] {DateTime.UtcNow:HH:mm:ss} {message}");
    public void LogInformation(string message) =>
        Console.WriteLine($"[EdgeSync INF] {DateTime.UtcNow:HH:mm:ss} {message}");
    public void LogWarning(string message) =>
        Console.WriteLine($"[EdgeSync WRN] {DateTime.UtcNow:HH:mm:ss} {message}");
    public void LogError(string message) =>
        Console.WriteLine($"[EdgeSync ERR] {DateTime.UtcNow:HH:mm:ss} {message}");
}
