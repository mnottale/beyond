using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Google.Protobuf;

// https://www.c-sharpcorner.com/article/fast-and-clean-o1-lru-cache-implementation/
public class ByteArrayComparer : EqualityComparer<byte[]>
{
    public override bool Equals(byte[] first, byte[] second)
    {
        if (first == null || second == null) {
            // null == null returns true.
            // non-null == null returns false.
            return first == second;
        }
        if (ReferenceEquals(first, second)) {
            return true;
        }
        if (first.Length != second.Length) {
            return false;
        }
        // Linq extension method is based on IEnumerable, must evaluate every item.
        return first.SequenceEqual(second);
    }
    public override int GetHashCode(byte[] obj)
    {
        if (obj == null) {
            throw new ArgumentNullException("obj");
        }
        if (obj.Length >= 4) {
            return BitConverter.ToInt32(obj, 0);
        }
        // Length occupies at most 2 bits. Might as well store them in the high order byte
        int value = obj.Length;
        foreach (var b in obj) {
            value <<= 8;
            value += b;
        }
        return value;
    }
}

public class LRUCache
{
    private int _capacity;
    private Dictionary<byte[], (LinkedListNode<byte[]> node, Block value)> _cache;
    private LinkedList<byte[]> _list;

    public LRUCache(int capacity)
    {
        _capacity = capacity;
        _cache = new Dictionary<byte[], (LinkedListNode<byte[]> node, Block value)>(new ByteArrayComparer());
        _list = new LinkedList<byte[]>();
    }

    public Block Get(byte[] key)
    {
        if (!_cache.ContainsKey(key))
            return null;

        var node = _cache[key];
        _list.Remove(node.node);
        _list.AddFirst(node.node);

        return node.value;
    }

    public void Put(byte[] key, Block value)
    {
        if (_cache.ContainsKey(key))
        {
            var node = _cache[key];
            _list.Remove(node.node);
            _list.AddFirst(node.node);

            _cache[key] = (node.node, value);
        }
        else
        {
            if (_cache.Count >= _capacity)
            {
                var removeKey = _list.Last!.Value;
                _cache.Remove(removeKey);
                _list.RemoveLast();
            }

            // add cache
            _cache.Add(key, (_list.AddFirst(key), value));
        }
    }
}

public class Cache
{
    private class DatedBlock
    {
        public Block block;
        public DateTime cachedAt;
    }
    private Dictionary<byte[], DatedBlock> _mutables;
    private LRUCache _immutables;
    private BeyondClient.BeyondClientClient _backend;
    private ulong _mutableDuration;
    public Cache(BeyondClient.BeyondClientClient backend, ulong immutableCount, ulong mutableDuration)
    {
        if (immutableCount != 0)
            _immutables = new LRUCache((int)immutableCount);
        if (mutableDuration > 0)
            _mutables = new(new ByteArrayComparer());
        _mutableDuration = mutableDuration;
        _backend = backend;
    }
    public Block GetImmutable(Key k)
    {
        if (_immutables == null)
            return _backend.Query(k);
        var res = _immutables.Get(k.Key_.ToByteArray());
        if (res != null)
            return res;
        res = _backend.Query(k);
        _immutables.Put(k.Key_.ToByteArray(), res);
        return res;
    }
    public Block GetMutable(Key k, bool noCache = false)
    {
        if (_mutables == null)
            return _backend.Query(k);
        if (_mutables.TryGetValue(k.Key_.ToByteArray(), out var datedBlock))
        {
            if (noCache || (DateTime.UtcNow - datedBlock.cachedAt) > TimeSpan.FromSeconds(_mutableDuration))
            {
                datedBlock.block = _backend.Query(k);
                datedBlock.cachedAt = DateTime.UtcNow;
                return datedBlock.block.Clone();
            }
            return datedBlock.block.Clone();
        }
        var res = _backend.Query(k);
        if (res.Owner != null) // FIXME proper error checking
            _mutables.Add(k.Key_.ToByteArray(), new DatedBlock { cachedAt = DateTime.UtcNow, block = res});
        return res.Clone();
    }
    public Error UpdateMutable(BlockAndKey bak)
    {
        var res = _backend.TransactionalUpdate(bak);
        if (res.Code != Error.Types.ErrorCode.Ok || _mutables == null)
            return res;
        if (_mutables.TryGetValue(bak.Key.Key_.ToByteArray(), out var datedBlock))
        {
            datedBlock.cachedAt = DateTime.UtcNow;
            datedBlock.block = bak.Block.Clone();
        }
        else
            _mutables.Add(bak.Key.Key_.ToByteArray(), new DatedBlock { cachedAt = DateTime.UtcNow, block = bak.Block.Clone()});
        return res;
    }
}