using System.Collections.Concurrent;

using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;

namespace QuantaCandle.Service.Pipeline;

public sealed class InMemoryTradeDeduplicator : ITradeDeduplicator
{
    private readonly int _capacityPerInstrument;
    private readonly ConcurrentDictionary<Instrument, RecentKeyCache> _caches;

    public InMemoryTradeDeduplicator(CollectorOptions options)
    {
        _capacityPerInstrument = Math.Max(1, options.DeduplicationCapacity);
        _caches = new ConcurrentDictionary<Instrument, RecentKeyCache>();

        foreach (Instrument instrument in options.Instruments)
        {
            _caches.TryAdd(instrument, new RecentKeyCache(_capacityPerInstrument));
        }
    }

    public bool TryAccept(TradeKey key)
    {
        RecentKeyCache cache = _caches.GetOrAdd(key.Symbol, _ => new RecentKeyCache(_capacityPerInstrument));
        return cache.TryAdd(key);
    }

    private sealed class RecentKeyCache
    {
        private readonly TradeKey[] ring;
        private readonly HashSet<TradeKey> set;
        private readonly object gate;
        private int nextIndex;
        private int count;

        public RecentKeyCache(int capacity)
        {
            ring = new TradeKey[capacity];
            set = new HashSet<TradeKey>();
            gate = new object();
            nextIndex = 0;
            count = 0;
        }

        public bool TryAdd(TradeKey key)
        {
            lock (gate)
            {
                var exists = set.Contains(key);

                if (!exists)
                {
                    if (count == ring.Length)
                    {
                        TradeKey evicted = ring[nextIndex];
                        set.Remove(evicted);
                    }
                    else
                    {
                        count++;
                    }

                    ring[nextIndex++] = key;
                    set.Add(key);
                    if (nextIndex == ring.Length)
                    {
                        nextIndex = 0;
                    }
                }

                return !exists;
            }
        }
    }
}

