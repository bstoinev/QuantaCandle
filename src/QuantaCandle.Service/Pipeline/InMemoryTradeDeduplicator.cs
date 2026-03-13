using System.Collections.Concurrent;
using QuantaCandle.Core.Trading;
using QuantaCandle.Service.Options;

namespace QuantaCandle.Service.Pipeline;

public sealed class InMemoryTradeDeduplicator : ITradeDeduplicator
{
    private readonly int capacityPerInstrument;
    private readonly ConcurrentDictionary<Instrument, RecentKeyCache> caches;

    public InMemoryTradeDeduplicator(CollectorOptions options)
    {
        capacityPerInstrument = Math.Max(1, options.DeduplicationCapacity);
        caches = new ConcurrentDictionary<Instrument, RecentKeyCache>();

        foreach (Instrument instrument in options.Instruments)
        {
            caches.TryAdd(instrument, new RecentKeyCache(capacityPerInstrument));
        }
    }

    public bool TryAccept(TradeKey key)
    {
        RecentKeyCache cache = caches.GetOrAdd(key.Symbol, _ => new RecentKeyCache(capacityPerInstrument));
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
            bool accepted;

            lock (gate)
            {
                if (set.Contains(key))
                {
                    accepted = false;
                }
                else
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

                    ring[nextIndex] = key;
                    set.Add(key);
                    nextIndex++;
                    if (nextIndex == ring.Length)
                    {
                        nextIndex = 0;
                    }

                    accepted = true;
                }
            }

            return accepted;
        }
    }
}

