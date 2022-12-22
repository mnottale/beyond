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

namespace Beyond
{
    internal class BPeer
    {
        public Peer info;
        public BeyondNode.BeyondNodeClient client;
    }
    // Horrible horrible hack since we don't control BeyondClientImpl lifetime
    // anymore.
    internal static class State
    {
        public static int replicationFactor;
        public static string rootPath;
        public static int port;

        public static Crypto crypto;
        public static Storage storage;
        public static List<BPeer> peers = new List<BPeer>();
        public static Peer self;
        public static ConcurrentDictionary<Key, Key> locks = new ConcurrentDictionary<Key, Key>();
        public static SemaphoreSlim sem = new SemaphoreSlim(1, 1);

        public static BeyondServiceImpl backend;
        public static ILogger logger;
    }
    public class BeyondClientImpl: BeyondClient.BeyondClientBase
    {
        public BeyondClientImpl()
        {
        }
        public override Task<Error> TransactionalUpdate(BlockAndKey bak, ServerCallContext ctx)
        {
            return State.backend.TransactionalUpdate(bak, ctx);
        }
        public override Task<Error> Insert(BlockAndKey bak, ServerCallContext ctx)
        {
            return State.backend.Insert(bak, ctx);
        }
        public override Task<Error> Delete(BlockAndKey k, ServerCallContext ctx)
        {
            return State.backend.Delete(k, ctx);
        }
        public override Task<Block> Query(Key k, ServerCallContext ctx)
        {
            return State.backend.Query(k, ctx);
        }
        private async Task<bool> DoEvict(Key k, Key owner)
        {
            try
            {
                var block = await State.backend.Query(k, null);
                if (block.Owners == null || !block.Owners.Owners.Contains(owner))
                    return false;
                block.Owners.Owners.Remove(owner);
                block.Version = block.Version + 1;
                var bak = new BlockAndKey();
                bak.Key = k;
                bak.Block = block;
                await State.backend.TransactionalUpdate(bak, null);
                return true;
            }
            catch (Exception e)
            {
                State.logger.LogError(e, "failed to evict a key");
                return false;
            }
        }
        public override async Task<Error> Evict(Key owner, ServerCallContext ctx)
        {
            var cnt = 0;
            foreach (var k in (await State.backend.ListKeys(new Void(), ctx)).Keys)
            {
                if (await DoEvict(k, owner))
                    cnt++;
            }
            foreach (var p in State.peers)
            {
                foreach (var kk in (await p.client.ListKeysAsync(new Void())).Keys)
                {
                    if (await DoEvict(kk, owner))
                        cnt++;
                }
            }
            State.logger.LogInformation("Evicted {count} blocks", cnt);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        private async Task<bool> DoHeal(Key k)
        {
            try
            {
                var block = await State.backend.Query(k, null);
                if (block.Owners == null || block.Owners.Owners.Count >= State.replicationFactor)
                    return false;
                List<BPeer> candidates = new List<BPeer>(State.peers);
                candidates.Add(null); // me
                candidates.Shuffle();
                foreach (var co in block.Owners.Owners)
                {
                    if (co.Equals(State.self.Id))
                        candidates.Remove(null);
                    else
                    {
                        var hit = candidates.Find(x=>x.info.Id.Equals(co));
                        if (hit != null)
                            candidates.Remove(hit);
                    }
                }
                if (!candidates.Any())
                    return false;
                var toAdd = State.replicationFactor - block.Owners.Owners.Count;
                while (toAdd > 0 && candidates.Any())
                {
                    toAdd--;
                    block.Owners.Owners.Add(candidates[0].info.Id);
                    candidates.RemoveAt(0);
                }
                block.Version++;
                var bak = new BlockAndKey();
                bak.Key = k;
                bak.Block = block;
                await State.backend.TransactionalUpdate(bak, null);
                return true;
            }
            catch (Exception e)
            {
                State.logger.LogError(e, "failed to heal a key");
                return false;
            }
        }
        public override async Task<Error> Heal(Void v, ServerCallContext ctx)
        {
            var cnt = 0;
            foreach (var k in (await State.backend.ListKeys(new Void(), ctx)).Keys)
            {
                if (await DoHeal(k))
                    cnt++;
            }
            foreach (var p in State.peers)
            {
                foreach (var kk in (await p.client.ListKeysAsync(new Void())).Keys)
                {
                    if (await DoHeal(kk))
                        cnt++;
                }
            }
            State.logger.LogInformation("healed {count} blocks", cnt);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
    }
    public class BeyondServiceImpl : BeyondNode.BeyondNodeBase
    {

        private ILogger logger;
        private ConcurrentDictionary<Key, Key> locks = new ConcurrentDictionary<Key, Key>();
        private SemaphoreSlim sem = new SemaphoreSlim(1, 1);
        public BeyondServiceImpl(ILogger<BeyondServiceImpl> logger)
        {
            this.logger = logger;
            State.logger = logger;
            if (State.backend == null)
            {
                State.backend = this;
            }
            if (State.crypto != null && State.crypto.GetKeyBlock == null)
                State.crypto.GetKeyBlock = async k => await State.backend.Query(k, null);
            if (State.storage == null)
            {
                State.storage = new Storage(State.rootPath + "/data");
                State.self = new Peer();
                try
                {
                    var idBuf = File.ReadAllBytes(State.rootPath + "/identity");
                    using (var ms = new MemoryStream(idBuf))
                {
                    var key = Key.Parser.ParseFrom(idBuf);
                    State.self.Id = key;
                }
                }
                catch (Exception)
                {
                    logger.LogInformation("Generating key");
                    State.self.Id = Utils.RandomKey();
                    var serialized = State.self.Id.ToByteArray();
                    File.WriteAllBytes(State.rootPath + "/identity", serialized);
                }
                State.self.Addresses.Add("localhost");
                State.self.Port = State.port;
            }
        }
        private async Task<List<BPeer>> LocatePeers(Key key)
        {
            var tasks = new List<Task<Error>>();
            foreach (var p in State.peers)
            {
                tasks.Add(Wrap(p, p.client.HasBlockAsync(key).ResponseAsync));
            }
            await Task.WhenAll(tasks);
            var res = new List<BPeer>();
            for (var i=0; i< tasks.Count; ++i)
            {
                if ((await tasks[i]).Code == Error.Types.ErrorCode.Ok)
                    res.Add(State.peers[i]);
            }
            if (State.storage.Has(key))
                res.Add(null);
            return res;
        }
        public async Task<Error> Delete(BlockAndKey bak, ServerCallContext ctx)
        {
            var peers = await LocatePeers(bak.Key);
            var tasks = new List<Task<Error>>();
            foreach (var p in State.peers)
            {
                if (p == null)
                    tasks.Add(DeleteBlock(bak, ctx));
                else
                    tasks.Add(p.client.DeleteBlockAsync(bak).ResponseAsync);
            }
            await Task.WhenAll(tasks);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> DeleteBlock(BlockAndKey bak, ServerCallContext ctx)
        {
            if (State.crypto != null)
            {
                var target = await Query(bak.Key, ctx);
                logger.LogInformation("Deletion target {block}", target);
                Block owner = null;
                if (target.OwningBlock != null)
                {
                    owner = await Query(target.OwningBlock, ctx);
                    logger.LogInformation("Deletion owner {block}", owner);
                }
                var failCode = await State.crypto.VerifyDelete(bak, target, owner);
                if (failCode != 0)
                {
                    logger.LogWarning("Deletion request for {key} denied with {code}", Utils.KeyString(bak.Key), failCode);
                    return Utils.ErrorFromCode(Error.Types.ErrorCode.VerificationFailed);
                }
            }
            State.storage.Delete(bak.Key);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public async Task<Error> Insert(BlockAndKey bak, ServerCallContext ctx)
        {
            List<BPeer> candidates = new List<BPeer>(State.peers);
            candidates.Add(null); // me
            var targets = Utils.PickN(candidates, State.replicationFactor);
            if (targets.Count < State.replicationFactor/2 + 1)
                return Utils.ErrorFromCode(Error.Types.ErrorCode.NotEnoughPeers);
            bak.Block.Owners = new BlockOwnership();
            foreach (var c in targets)
            {
                if (c == null)
                    bak.Block.Owners.Owners.Add(State.self.Id);
                else
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
            var current = State.storage.VersionOf(lk.Target);
            if (current > lk.Version -1) // silently accept if we are behind
            {
                logger.LogInformation("AcquireLock failure: {current} vs {requested}", current, lk.Version);
                return Utils.ErrorFromCode(Error.Types.ErrorCode.Outdated, current);
            }
            if (State.locks.ContainsKey(lk.Target))
                return Utils.ErrorFromCode(Error.Types.ErrorCode.AlreadyLocked);
            State.locks.TryAdd(lk.Target, lk.LockUid);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> ForceAcquireLock(Lock lk, ServerCallContext ctx)
        {
            if (State.locks.ContainsKey(lk.Target))
                State.locks.TryRemove(lk.Target, out _);
            State.locks.TryAdd(lk.Target, lk.LockUid);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override Task<Error> ReleaseLock(Lock lk, ServerCallContext ctx)
        {
            if (State.locks.TryGetValue(lk.Target, out var uid) && uid.Equals(lk.LockUid))
                State.locks.TryRemove(lk.Target, out _);
            return Task.FromResult(Utils.ErrorFromCode(Error.Types.ErrorCode.Ok));
        }
        private async Task<bool> VerifyWrite(BlockAndKey bak)
        {
            if (State.crypto == null)
                return true;
            BlockAndKey previous = null;
            if (State.storage.Has(bak.Key))
            {
                previous = new BlockAndKey();
                previous.Block = await GetBlock(bak.Key, null);
                previous.Key = bak.Key;
            }
            var failCode = await State.crypto.VerifyWrite(bak, previous);
            if (failCode != 0)
                logger.LogWarning("Block {key} verification failed, code {erc}", Utils.KeyString(bak.Key), failCode);
            return failCode == 0;
        }
        public override async Task<Error> Write(BlockAndKey bak, ServerCallContext ctx)
        {
            var ok = await VerifyWrite(bak);
            if (!ok)
                return Utils.ErrorFromCode(Error.Types.ErrorCode.VerificationFailed);
            var serialized = bak.Block.ToByteArray();
            State.storage.Put(bak.Key, serialized);
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Error> WriteAndRelease(BlockAndLock bal, ServerCallContext ctx)
        {
            var ok = await VerifyWrite(bal.BlockAndKey);
            if (!ok)
            {
                if (State.locks.TryGetValue(bal.Lock.Target, out var lkId))
                {
                    if (lkId.Equals(bal.Lock.LockUid))
                        State.locks.TryRemove(bal.Lock.Target, out _);
                }
                return Utils.ErrorFromCode(Error.Types.ErrorCode.VerificationFailed);
            }
            var serialized = bal.BlockAndKey.Block.ToByteArray();
            State.storage.Put(bal.BlockAndKey.Key, serialized);
            if (State.locks.TryGetValue(bal.Lock.Target, out var lkIdb))
            {
                if (lkIdb.Equals(bal.Lock.LockUid))
                    State.locks.TryRemove(bal.Lock.Target, out _);
            }
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public override async Task<Block> GetBlock(Key k, ServerCallContext ctx)
        {
            var raw = State.storage.Get(k);
            using var ms = new MemoryStream(raw);
            return Block.Parser.ParseFrom(ms);
        }
        public async Task<Block> Query(Key k, ServerCallContext ctx)
        {
            var peers = await LocatePeers(k);
            if (!peers.Any())
                return new Block();
            var tblks = new List<Task<Block>>();
            foreach (var p in peers)
            {
                if (p == null)
                    tblks.Add(GetBlock(k, ctx));
                else
                    tblks.Add(p.client.GetBlockAsync(k).ResponseAsync);
            }
            try
            {
                await Task.WhenAll(tblks);
            }
            catch (Exception e)
            {
            }
            var blks = tblks.Where(x=>!x.IsFaulted).Select(x=>x.Result).ToList();
            var vmax = blks.Max(b => b.Version);
            // FIXME repair
            // FIXME quorum check
            var anymax = blks.Find(b => b.Version == vmax);
            return anymax;
        }
        private async Task<Error> Wrap(BPeer peer, Task<Error> tsk)
        { // FIXME filter exception
            try
            {
                return await tsk;
            }
            catch (Exception e)
            {
                logger.LogError(e, "exception in call");
                State.peers.Remove(peer);
                return Utils.ErrorFromCode(Error.Types.ErrorCode.ExceptionThrown);
            }
        }
        public override async Task<KeyList> ListKeys(Void v, ServerCallContext ctx)
        {
            var res = new KeyList();
            res.Keys.AddRange(State.storage.List());
            return res;
        }
        public async Task<Error> TransactionalUpdate(BlockAndKey bak, ServerCallContext ctx)
        {
            var lk = new Lock();
            lk.Target = bak.Key;
            lk.Version = bak.Block.Version;
            lk.LockUid = Utils.RandomKey();
            List<BPeer> peers = null;
            var rf = State.replicationFactor;
            if (bak.Block.Owners != null && bak.Block.Owners.Owners.Count() > 0)
            {
                peers = new List<BPeer>();
                rf = bak.Block.Owners.Owners.Count();
                foreach (var pk in bak.Block.Owners.Owners)
                {
                    var hit = State.peers.Find(p=>p.info.Id.Equals(pk));
                    if (hit != null)
                        peers.Add(hit);
                    else if (pk.Equals(State.self.Id))
                        peers.Add(null);
                }
            }
            else
                peers = await LocatePeers(bak.Key);
            if (peers.Count < rf / 2 + 1)
                return Utils.ErrorFromCode(Error.Types.ErrorCode.NotEnoughPeers);
            var acquired = new List<BPeer>();
            var failed = new List<BPeer>();
            var tasks = new List<Task<Error>>();
            foreach (var p in peers)
            {
                if (p == null)
                    tasks.Add(AcquireLock(lk, ctx));
                else
                    tasks.Add(Wrap(p, p.client.AcquireLockAsync(lk).ResponseAsync));
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
                || acquired.Count < rf / 2 + 1)
            {
                foreach (var ack in acquired)
                   await Wrap(ack, ack.client.ReleaseLockAsync(lk).ResponseAsync);
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
                    tasks.Add(Wrap(p, p.client.ForceAcquireLockAsync(lk).ResponseAsync));
            }
            await Task.WhenAll(tasks);
            tasks.Clear();
            var bal = new BlockAndLock();
            bal.Lock = lk;
            bal.BlockAndKey = bak;
            ulong umsk = 0;
            ulong bit = 0;
            if (bal.BlockAndKey.Block.Owners != null)
            {
                foreach (var ownerKey in bal.BlockAndKey.Block.Owners.Owners)
                {
                    if (peers.Find(p => 
                        (p == null && State.self.Id.Equals(ownerKey != null))
                    || (p != null && p.info.Id.Equals(ownerKey))) != null)
                        umsk |= bit;
                    bit <<= 1;
                }
                bal.BlockAndKey.Block.Owners.UptodateMask = umsk;
            }
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
            logger.LogInformation("Connecting to peer at {hostport}", hostport);
            var channel = GrpcChannel.ForAddress("http://" + hostport, new GrpcChannelOptions
                {
                    Credentials = ChannelCredentials.Insecure
                });
            logger.LogInformation("  got channel to peer at {hostport}", hostport);
            var client = new BeyondNode.BeyondNodeClient(channel);
            logger.LogInformation("  got client to peer at {hostport}", hostport);
            var desc = await client.DescribeAsync(new Void());
            logger.LogInformation("  got desc to peer at {hostport}", hostport);
            var hit = false;
            await State.sem.WaitAsync();
            foreach (var p in State.peers)
            {
                if (p.info.Id.Equals(desc.Id))
                {
                    p.info = desc;
                    hit = true;
                    break;
                }
            }
            if (!hit)
                State.peers.Add(new BPeer { info = desc, client = client});
            var infos = State.peers.Select(x=>x.info).ToList();
            State.sem.Release();
            logger.LogInformation("Connected to peer at {host} {port} {hit}", desc.Addresses[0], desc.Port, hit);
            _ = Task.Delay(200).ContinueWith(async _ =>
                {
                    await client.AnnounceAsync(State.self);
                    foreach (var p in infos)
                    {
                        await client.AnnounceAsync(p);
                    }
                });
            logger.LogInformation("Done anouncing peers to {host} {port}", desc.Addresses[0], desc.Port);
        }
        public override Task<Peer> Describe(Void v, ServerCallContext ctx)
        {
            logger.LogInformation("Received describe() call");
            return Task.FromResult(State.self);
        }
        public override async Task<Void> Announce(Peer peer, ServerCallContext ctx)
        {
            await State.sem.WaitAsync();
            if (State.peers.Find(p=>p.info.Id.Equals(peer.Id)) != null
                || State.self.Id.Equals(peer.Id))
            {
                logger.LogInformation("Dropping announce from known peer {host} {port}", peer.Addresses[0], peer.Port);
                State.sem.Release();
                return new Void();
            }
            logger.LogInformation("Handling announce from new peer {host} {port}", peer.Addresses[0], peer.Port);
            _ = Task.Delay(100).ContinueWith(_ => Connect(peer.Addresses[0], peer.Port));
            State.sem.Release();
            return new Void();
        }
        public override Task<Peers> GetPeers(Void v, ServerCallContext ctx)
        {
            var res = new Peers();
            foreach (var p in State.peers)
            {
                res.Peers_.Add(p.info);
            }
            return Task.FromResult(res);
        }
        public override Task<Blob> Get(Key k, ServerCallContext ctx)
        {
            var data = State.storage.Get(k);
            var res = new Blob();
            res.Blob_ = Google.Protobuf.ByteString.CopyFrom(data);
            return Task.FromResult(res);
        }
        public override Task<Void> Put(PutArgs pa, ServerCallContext ctx)
        {
            State.storage.Put(pa.Key, pa.Blob.Blob_.ToByteArray());
            return Task.FromResult(new Void());
        }
        public override Task<Error> HasBlock(Key key, ServerCallContext ctx)
        {
            if (State.storage.Has(key))
                return Task.FromResult(Utils.ErrorFromCode(Error.Types.ErrorCode.Ok));
            else
                return Task.FromResult(Utils.ErrorFromCode(Error.Types.ErrorCode.KeyNotFound));
        }
    }
}