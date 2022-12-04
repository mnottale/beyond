using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Grpc.Core;
using Grpc.Core.Utils;
using Google.Protobuf;

namespace Beyond
{
    public class BeyondClientImpl: BeyondClient.BeyondClientBase
    {
        private BeyondServiceImpl backend;
        public BeyondClientImpl(BeyondServiceImpl backend)
        {
            this.backend = backend;
        }
        public override Task<Error> TransactionalUpdate(BlockAndKey bak, ServerCallContext ctx)
        {
            return backend.TransactionalUpdate(bak, ctx);
        }
        public override Task<Error> Insert(BlockAndKey bak, ServerCallContext ctx)
        {
            return backend.Insert(bak, ctx);
        }
        public override Task<Error> Delete(Key k, ServerCallContext ctx)
        {
            return backend.Delete(k, ctx);
        }
        public override Task<Block> Query(Key k, ServerCallContext ctx)
        {
            return backend.Query(k, ctx);
        }
    }
    public class BeyondServiceImpl : BeyondNode.BeyondNodeBase
    {
        private class BPeer
        {
            public Peer info;
            public BeyondNode.BeyondNodeClient client;
        }
        private int replicationFactor;
        private Storage storage;
        private List<BPeer> peers = new List<BPeer>();
        private Peer self;
        private ILogger logger;
        private ConcurrentDictionary<Key, Key> locks = new ConcurrentDictionary<Key, Key>();

        public BeyondServiceImpl(string rootPath, List<string> hosts, int port, int replicationFactor)
        {
            logger = Logger.loggerFactory.CreateLogger<BeyondServiceImpl>();
            storage = new Storage(rootPath + "/data");
            self = new Peer();
            try
            {
                var idBuf = File.ReadAllBytes(rootPath + "/identity");
                using (var ms = new MemoryStream(idBuf))
                {
                    var key = Key.Parser.ParseFrom(idBuf);
                    self.Id = key;
                }
            }
            catch (Exception)
            {
                logger.LogInformation("Generating key");
                self.Id = Utils.RandomKey();
            }
            self.Addresses.Add(hosts);
            self.Port = port;
            this.replicationFactor = replicationFactor;
        }
        private async Task<List<BPeer>> LocatePeers(Key key)
        {
            var tasks = new List<AsyncUnaryCall<Error>>();
            foreach (var p in peers)
            {
                tasks.Add(p.client.HasBlockAsync(key));
            }
            await Task.WhenAll(tasks.Select(x=>x.ResponseAsync));
            var res = new List<BPeer>();
            for (var i=0; i< tasks.Count; ++i)
            {
                if ((await tasks[i]).Code == Error.Types.ErrorCode.Ok)
                    res.Add(peers[i]);
            }
            if (storage.Has(key))
                res.Add(null);
            return res;
        }
        public async Task<Error> Delete(Key k, ServerCallContext ctx)
        {
            var peers = await LocatePeers(k);
            var tasks = new List<Task<Error>>();
            foreach (var p in peers)
            {
                if (p == null)
                    tasks.Add(DeleteBlock(k, ctx));
                else
                    tasks.Add(p.client.DeleteBlockAsync(k).ResponseAsync);
            }
            await Task.WhenAll(tasks);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> DeleteBlock(Key k, ServerCallContext ctx)
        {
            storage.Delete(k);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public async Task<Error> Insert(BlockAndKey bak, ServerCallContext ctx)
        {
            List<BPeer> candidates = new List<BPeer>(peers);
            candidates.Add(null); // me
            var targets = Utils.PickN(candidates, replicationFactor);
            if (targets.Count < replicationFactor/2 + 1)
                return Utils.ErrorFromCode(Error.Types.ErrorCode.NotEnoughPeers);
            bak.Block.Owners = new BlockOwnership();
            foreach (var c in targets)
            {
                bak.Block.Owners.Owners.Add(c.info.Id);
            }
            bak.Block.Owners.UptodateMask = (ulong)((1 << targets.Count) - 1);
            var tasks = new List<Task<Error>>();
            foreach (var c in targets)
            {
                if (c == null)
                    tasks.Add(Write(bak, ctx));
                else
                    tasks.Add(c.client.WriteAsync(bak).ResponseAsync);
            }
            await Task.WhenAll(tasks);
            // FIXME: handle errors
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> AcquireLock(Lock lk, ServerCallContext ctx)
        {
            var current = storage.VersionOf(lk.Target);
            if (current > lk.Version -1) // silently accept if we are behind
            {
                logger.LogInformation("AcquireLock failure: {current} vs {requested}", current, lk.Version);
                return Utils.ErrorFromCode(Error.Types.ErrorCode.Outdated, current);
            }
            if (locks.ContainsKey(lk.Target))
                return Utils.ErrorFromCode(Error.Types.ErrorCode.AlreadyLocked);
            locks.TryAdd(lk.Target, lk.LockUid);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> ForceAcquireLock(Lock lk, ServerCallContext ctx)
        {
            if (locks.ContainsKey(lk.Target))
                locks.TryRemove(lk.Target, out _);
            locks.TryAdd(lk.Target, lk.LockUid);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override Task<Error> ReleaseLock(Lock lk, ServerCallContext ctx)
        {
            if (locks.TryGetValue(lk.Target, out var uid) && uid.Equals(lk.LockUid))
                locks.TryRemove(lk.Target, out _);
            return Task.FromResult(Utils.ErrorFromCode(Error.Types.ErrorCode.Ok));
        }
        public override async Task<Error> Write(BlockAndKey bak, ServerCallContext ctx)
        {
            var serialized = bak.Block.ToByteArray();
            storage.Put(bak.Key, serialized);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> WriteAndRelease(BlockAndLock bal, ServerCallContext ctx)
        {
            var serialized = bal.BlockAndKey.Block.ToByteArray();
            storage.Put(bal.BlockAndKey.Key, serialized);
            if (locks.TryGetValue(bal.Lock.Target, out var lkId))
            {
                if (lkId.Equals(bal.Lock.LockUid))
                    locks.TryRemove(bal.Lock.Target, out _);
            }
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Block> GetBlock(Key k, ServerCallContext ctx)
        {
            var raw = storage.Get(k);
            using var ms = new MemoryStream(raw);
            return Block.Parser.ParseFrom(ms);
        }
        public async Task<Block> Query(Key k, ServerCallContext ctx)
        {
            var peers = await LocatePeers(k);
            if (peers.Contains(null))
                return await GetBlock(k, ctx);
            foreach (var p in peers)
            {
                var b = await p.client.GetBlockAsync(k);
                if (b.Version > 0)
                    return b;
            }
            return new Block();
        }
        public async Task<Error> TransactionalUpdate(BlockAndKey bak, ServerCallContext ctx)
        {
            var lk = new Lock();
            lk.Target = bak.Key;
            lk.Version = bak.Block.Version;
            lk.LockUid = Utils.RandomKey();
            var peers = await LocatePeers(bak.Key);
            if (peers.Count < replicationFactor / 2 + 1)
                return Utils.ErrorFromCode(Error.Types.ErrorCode.NotEnoughPeers);
            var acquired = new List<BPeer>();
            var failed = new List<BPeer>();
            var tasks = new List<Task<Error>>();
            foreach (var p in peers)
            {
                if (p == null)
                    tasks.Add(AcquireLock(lk, ctx));
                else
                    tasks.Add(p.client.AcquireLockAsync(lk).ResponseAsync);
            }
            await Task.WhenAll(tasks);
            var failCode = Error.Types.ErrorCode.Ok;
            for (var i=0; i<tasks.Count; i++)
            {
                var res = await tasks[i];
                if (res.Code == Error.Types.ErrorCode.Ok)
                    acquired.Add(peers[i]);
                else if (res.Code == Error.Types.ErrorCode.AlreadyLocked)
                    failed.Add(peers[i]);
                else
                    failCode = res.Code;
            }
            if (failCode != Error.Types.ErrorCode.Ok
                || acquired.Count < replicationFactor / 2 + 1)
            {
                foreach (var ack in acquired)
                   await ack.client.ReleaseLockAsync(lk);
                return Utils.ErrorFromCode(
                    failCode == Error.Types.ErrorCode.Ok?
                    Error.Types.ErrorCode.Conflict : failCode);
            }
            tasks.Clear();
            foreach (var p in failed)
            {
                if (p == null)
                    tasks.Add(ForceAcquireLock(lk, ctx));
                else
                    tasks.Add(p.client.ForceAcquireLockAsync(lk).ResponseAsync);
            }
            await Task.WhenAll(tasks);
            tasks.Clear();
            var bal = new BlockAndLock();
            bal.Lock = lk;
            bal.BlockAndKey = bak;
            ulong umsk = 0;
            ulong bit = 0;
            foreach (var ownerKey in bal.BlockAndKey.Block.Owners.Owners)
            {
                if (peers.Find(p => p.info.Id.Equals(ownerKey)) != null)
                    umsk |= bit;
                bit <<= 1;
            }
            bal.BlockAndKey.Block.Owners.UptodateMask = umsk;
            foreach (var p in peers)
            {
                if (p == null)
                    tasks.Add(WriteAndRelease(bal, ctx));
                else
                    tasks.Add(p.client.WriteAndReleaseAsync(bal).ResponseAsync);
            }
            await Task.WhenAll(tasks);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public Task Connect(string host, int port)
        {
            return Connect($"{host}:{port}");
        }
        public async Task Connect(string hostport)
        {
            var channel = new Channel(hostport, ChannelCredentials.Insecure);
            var client = new BeyondNode.BeyondNodeClient(channel);
            var desc = await client.DescribeAsync(new Void());
            var hit = false;
            foreach (var p in peers)
            {
                if (p.info.Id == desc.Id)
                {
                    p.info = desc;
                    hit = true;
                    break;
                }
            }
            if (!hit)
                peers.Add(new BPeer { info = desc, client = client});
            await client.AnnounceAsync(self);
            logger.LogInformation("Connected to peer at {host} {port}", desc.Addresses[0], desc.Port);
        }
        public override Task<Peer> Describe(Void v, ServerCallContext ctx)
        {
            return Task.FromResult(self);
        }
        public override Task<Void> Announce(Peer peer, ServerCallContext ctx)
        {
            _ = Connect(peer.Addresses[0], peer.Port);
            return Task.FromResult(new Void());
        }
        public override Task<Peers> GetPeers(Void v, ServerCallContext ctx)
        {
            var res = new Peers();
            foreach (var p in peers)
            {
                res.Peers_.Add(p.info);
            }
            return Task.FromResult(res);
        }
        public override Task<Blob> Get(Key k, ServerCallContext ctx)
        {
            var data = storage.Get(k);
            var res = new Blob();
            res.Blob_ = Google.Protobuf.ByteString.CopyFrom(data);
            return Task.FromResult(res);
        }
        public override Task<Void> Put(PutArgs pa, ServerCallContext ctx)
        {
            storage.Put(pa.Key, pa.Blob.Blob_.ToByteArray());
            return Task.FromResult(new Void());
        }
        public override Task<Error> HasBlock(Key key, ServerCallContext ctx)
        {
            if (storage.Has(key))
                return Task.FromResult(Utils.ErrorFromCode(Error.Types.ErrorCode.Ok));
            else
                return Task.FromResult(Utils.ErrorFromCode(Error.Types.ErrorCode.KeyNotFound));
        }
    }
}