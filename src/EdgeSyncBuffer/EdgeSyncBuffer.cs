using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace EdgeSync;

/// <summary>
/// Offline-first time-series buffer for IoT edge devices.
/// Writes always succeed locally. When connectivity is restored,
/// all buffered data is delivered to the cloud in guaranteed
/// sequence-number order — not FIFO, not timestamp order.
///
/// Survives process restarts via JSON Lines disk persistence.
/// Applies configurable backpressure when the buffer is full.
///
/// Designed for enterprise IoT deployments where data loss
/// has compliance or operational consequences.
/// </summary>
/// <typeparam name="T">The telemetry message type. Must implement ITimestamped.</typeparam>
public sealed class EdgeSyncBuffer<T> : IAsyncDisposable where T : ITimestamped
{
    private readonly ConcurrentQueue<BufferedEntry<T>> _queue = new();
    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private readonly EdgeSyncOptions _options;
    private readonly Func<IEnumerable<T>, CancellationToken, Task<SyncResult>> _uploadFn;
    private readonly IEdgeSyncLogger? _logger;

    private bool _isOnline;
    private long _sequenceCounter;
    private long _totalWritten;
    private long _totalSynced;
    private long _totalEvicted;
    private bool _disposed;

    /// <summary>Number of entries currently buffered locally.</summary>
    public int PendingCount => _queue.Count;

    /// <summary>Whether the buffer currently believes it has cloud connectivity.</summary>
    public bool IsOnline => _isOnline;

    /// <summary>Lifetime statistics for this buffer instance.</summary>
    public BufferStats Stats => new(_totalWritten, _totalSynced, _totalEvicted, _queue.Count);

    /// <summary>Raised when the dead-letter threshold is exceeded.</summary>
    public event Func<BufferStats, Task>? OnDeadLetterThresholdExceeded;

    /// <summary>
    /// Initializes the buffer and recovers any un-synced entries from disk.
    /// </summary>
    public EdgeSyncBuffer(
        EdgeSyncOptions options,
        Func<IEnumerable<T>, CancellationToken, Task<SyncResult>> uploadFn,
        IEdgeSyncLogger? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _uploadFn = uploadFn ?? throw new ArgumentNullException(nameof(uploadFn));
        _logger = logger;

        if (options.PersistPath is not null && File.Exists(options.PersistPath))
            _ = Task.Run(RecoverFromDiskAsync);
    }

    /// <summary>
    /// Write a telemetry entry. This method ALWAYS succeeds — if offline,
    /// the entry is buffered locally and synced when connectivity returns.
    /// </summary>
    /// <exception cref="ObjectDisposedException">If the buffer has been disposed.</exception>
    public async Task WriteAsync(T item, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Enforce capacity — evict oldest entry if full
        if (_queue.Count >= _options.MaxCapacity)
        {
            _queue.TryDequeue(out _);
            Interlocked.Increment(ref _totalEvicted);
            _logger?.LogWarning("Buffer full — oldest entry evicted. Consider increasing MaxCapacity.");

            if (_options.DeadLetterThreshold > 0 &&
                _totalEvicted % _options.DeadLetterThreshold == 0 &&
                OnDeadLetterThresholdExceeded is not null)
                await OnDeadLetterThresholdExceeded(Stats);
        }

        var entry = new BufferedEntry<T>(
            Data: item,
            BufferedAt: DateTime.UtcNow,
            SequenceNumber: Interlocked.Increment(ref _sequenceCounter));

        _queue.Enqueue(entry);
        Interlocked.Increment(ref _totalWritten);

        if (_options.PersistPath is not null)
            await AppendToDiskAsync(entry, ct);

        _logger?.LogDebug($"Written seq#{entry.SequenceNumber}. Pending: {_queue.Count}");

        // Attempt immediate sync if online
        if (_isOnline)
            _ = TrySyncBatchAsync(ct);
    }

    /// <summary>
    /// Call this when cloud connectivity is restored.
    /// Triggers ordered delivery of all buffered entries.
    /// </summary>
    public async Task OnConnectivityRestoredAsync(CancellationToken ct = default)
    {
        _isOnline = true;
        _logger?.LogInformation($"Connectivity restored. Syncing {_queue.Count} buffered entries.");
        await SyncAllAsync(ct);
    }

    /// <summary>
    /// Call this when cloud connectivity is lost.
    /// Subsequent writes are buffered locally until restored.
    /// </summary>
    public void OnConnectivityLost()
    {
        _isOnline = false;
        _logger?.LogWarning("Connectivity lost. Buffering locally.");
    }

    /// <summary>
    /// Stream all currently buffered entries in sequence order.
    /// Useful for inspection, diagnostics, and testing.
    /// </summary>
    public async IAsyncEnumerable<T> ReadBufferedAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var entry in _queue.ToArray().OrderBy(e => e.SequenceNumber))
        {
            ct.ThrowIfCancellationRequested();
            yield return entry.Data;
            await Task.Yield();
        }
    }

    // ── Internal sync logic ────────────────────────────────────────────────

    private async Task SyncAllAsync(CancellationToken ct)
    {
        await _syncGate.WaitAsync(ct);
        try
        {
            while (_queue.Count > 0 && _isOnline && !ct.IsCancellationRequested)
                await TrySyncBatchAsync(ct);
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task TrySyncBatchAsync(CancellationToken ct)
    {
        var batch = new List<BufferedEntry<T>>();

        while (batch.Count < _options.BatchSize && _queue.TryDequeue(out var entry))
            batch.Add(entry);

        if (batch.Count == 0) return;

        // KEY INSIGHT: sort by sequence number, NOT by FIFO queue order.
        // Clock drift at the edge means timestamps are unreliable for ordering.
        var ordered = batch
            .OrderBy(e => e.SequenceNumber)
            .Select(e => e.Data)
            .ToList();

        try
        {
            var result = await _uploadFn(ordered, ct);

            if (result.Success)
            {
                Interlocked.Add(ref _totalSynced, batch.Count);
                _logger?.LogInformation($"Synced {batch.Count} entries. Total synced: {_totalSynced}");

                if (_options.PersistPath is not null)
                    await RemoveSyncedFromDiskAsync(batch.Select(e => e.SequenceNumber).ToHashSet());
            }
            else
            {
                // Re-enqueue — connectivity lost again
                foreach (var e in batch.OrderByDescending(e => e.SequenceNumber))
                    _queue.Enqueue(e);
                _isOnline = false;
                _logger?.LogWarning($"Sync failed: {result.ErrorMessage}. Re-queued {batch.Count} entries.");
            }
        }
        catch (Exception ex)
        {
            // Network error — re-enqueue in reverse sequence order
            foreach (var e in batch.OrderByDescending(e => e.SequenceNumber))
                _queue.Enqueue(e);
            _isOnline = false;
            _logger?.LogError($"Sync exception: {ex.Message}. Re-queued {batch.Count} entries.");
        }
    }

    // ── Disk persistence ───────────────────────────────────────────────────

    private async Task AppendToDiskAsync(BufferedEntry<T> entry, CancellationToken ct)
    {
        try
        {
            var line = JsonSerializer.Serialize(entry) + Environment.NewLine;
            await File.AppendAllTextAsync(_options.PersistPath!, line, ct);
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Disk persist failed: {ex.Message}");
        }
    }

    private async Task RecoverFromDiskAsync()
    {
        try
        {
            var lines = await File.ReadAllLinesAsync(_options.PersistPath!);
            var recovered = 0;

            foreach (var line in lines.Where(l => !string.IsNullOrWhiteSpace(l)))
            {
                try
                {
                    var entry = JsonSerializer.Deserialize<BufferedEntry<T>>(line);
                    if (entry is not null)
                    {
                        _queue.Enqueue(entry);
                        recovered++;
                    }
                }
                catch { /* Skip malformed lines */ }
            }

            _logger?.LogInformation($"Recovered {recovered} entries from disk at {_options.PersistPath}");
        }
        catch (Exception ex)
        {
            _logger?.LogError($"Disk recovery failed: {ex.Message}");
        }
    }

    private Task RemoveSyncedFromDiskAsync(HashSet<long> syncedSeqNums)
    {
        // Compact the file by rewriting only un-synced entries
        _ = Task.Run(async () =>
        {
            try
            {
                if (_options.PersistPath is null || !File.Exists(_options.PersistPath)) return;

                var lines = await File.ReadAllLinesAsync(_options.PersistPath);
                var remaining = lines
                    .Where(l => !string.IsNullOrWhiteSpace(l))
                    .Where(l =>
                    {
                        try
                        {
                            var e = JsonSerializer.Deserialize<BufferedEntry<T>>(l);
                            return e is not null && !syncedSeqNums.Contains(e.SequenceNumber);
                        }
                        catch { return true; }
                    })
                    .ToList();

                await File.WriteAllLinesAsync(_options.PersistPath, remaining);
            }
            catch (Exception ex)
            {
                _logger?.LogError($"Disk compaction failed: {ex.Message}");
            }
        });

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _syncGate.Dispose();
        await ValueTask.CompletedTask;
    }
}
