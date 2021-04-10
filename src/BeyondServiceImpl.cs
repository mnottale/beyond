using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Grpc.Core;
using Grpc.Core.Utils;
using Google.Protobuf;

namespace Beyond
{
    public class BeyondServiceImpl : BeyondService.BeyondServiceBase
    {
        private class BPeer
        {
            public Peer info;
            public BeyondService.BeyondServiceClient client;
        }
        private int replicationFactor;
        private Storage storage;
        private List<BPeer> peers = new List<BPeer>();
        private Peer self;
        private ILogger logger;
        private HashSet<byte[]> locks = new HashSet<byte[]>();

        public BeyondServiceImpl(Storage storage, List<string> hosts, int port, int replicationFactor)
        {
            self = new Peer();
            self.Addresses.Add(hosts);
            self.Port = port;
            this.storage = storage;
            this.replicationFactor = replicationFactor;
            logger = Logger.loggerFactory.CreateLogger<BeyondServiceImpl>();
        }
        private async Task<List<BPeer>> LocatePeers(Key key)
        {
            return new List<BPeer>();
        }
        public async Task<Error> AcquireLock(KeyAndVersion kav)
        {
            var current = storage.VersionOf(kav.Key);
            if (current != kav.Version -1)
            {
                logger.LogInformation("AcquireLock failure: {current} vs {requested}", current, kav.Version);
                return Utils.ErrorFromCode(Error.Types.ErrorCode.Outdated);
            }
            if (locks.Contains(kav.Key.Key_.ToByteArray()))
                return Utils.ErrorFromCode(Error.Types.ErrorCode.AlreadyLocked);
            locks.Add(kav.Key.Key_.ToByteArray());
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public async Task<Error> ForceAcquireLock(KeyAndVersion kav)
        {
            locks.Add(kav.Key.Key_.ToByteArray());
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public async Task<Error> WriteAndRelease(BlockAndKey bak)
        {
            var serialized = bak.Block.ToByteArray();
            storage.Put(bak.Key, serialized);
            locks.Remove(bak.Key.Key_.ToByteArray());
            return Utils.ErrorFromCode(Error.Types.ErrorCode.Ok);
        }
        public async Task TransactionalUpdate(Key key, Block block)
        {
            var bv = new KeyAndVersion();
            bv.Key = key;
            bv.Version = block.Version;
            var peers = await LocatePeers(key);
            if (peers.Count < replicationFactor / 2 + 1)
                throw new Exception($"Cannot write, only {peers.Count} located");
            var acquired = new List<BPeer>();
            var failed = new List<BPeer>();
            var tasks = new List<AsyncUnaryCall<Error>>();
            foreach (var p in peers)
            {
                tasks.Add(p.client.AcquireLockAsync(bv));
            }
            await Task.WhenAll(tasks.Select(x=>x.ResponseAsync));
            for (var i=0; i<tasks.Count; i++)
            {
                var res = await tasks[i];
                if (res.Code == Error.Types.ErrorCode.Ok)
                    acquired.Add(peers[i]);
                else if (res.Code == Error.Types.ErrorCode.AlreadyLocked)
                    failed.Add(peers[i]);
                else
                    throw new Exception("Failed to acquire!");
            }
            if (acquired.Count < replicationFactor / 2 + 1)
                throw new Exception("Failed to acquire lock, retry...");
            tasks.Clear();
            foreach (var p in failed)
            {
                tasks.Add(p.client.ForceAcquireLockAsync(bv));
            }
            await Task.WhenAll(tasks.Select(x=>x.ResponseAsync));
            tasks.Clear();
            var bak = new BlockAndKey();
            bak.Block = block;
            bak.Key = key;
            foreach (var p in peers)
            {
                tasks.Add(p.client.WriteAndReleaseAsync(bak));
            }
            await Task.WhenAll(tasks.Select(x=>x.ResponseAsync));
        }
        public Task Connect(string host, int port)
        {
            return Connect($"{host}:{port}");
        }
        public async Task Connect(string hostport)
        {
            var channel = new Channel(hostport, ChannelCredentials.Insecure);
            var client = new BeyondService.BeyondServiceClient(channel);
            var desc = await client.DescribeAsync(new Void());
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
    }
}