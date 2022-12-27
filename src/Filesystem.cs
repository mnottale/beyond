using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;

using Mono.Fuse.NETStandard;
using Mono.Unix.Native;

namespace Beyond
{
    class FileSystem: Mono.Fuse.NETStandard.FileSystem
    {
        private BeyondClient.BeyondClientClient client;
        private ILogger logger;
        const ulong CHUNK_SIZE = 65536;

        private class FileChunk
        {
            public DateTime atime;
            public bool dirty;
            public byte[] data;
            public ulong version;
        };
        private class OpenedHandle
        {
            public bool dirty;
            public ulong openCount;
            public BlockAndKey fileBlock;
            public AESKey key;
            public ConcurrentDictionary<ulong, FileChunk> chunks = new ConcurrentDictionary<ulong, FileChunk>();
        };
        ConcurrentDictionary<string, OpenedHandle> openedFiles = new ConcurrentDictionary<string, OpenedHandle>();
        byte[] rootAddress = new byte[32];
        private uint permUid = 0;
        private uint permGid = 0;
        private Crypto crypto = null;
        private Cache cache = null;
        public FileSystem(BeyondClient.BeyondClientClient client, string fsName = null, uint uid=0, uint gid=0, Crypto c = null, ulong immutableCacheSize = 0, ulong mutableCacheDuration = 0)
        {
            logger = Logger.loggerFactory.CreateLogger<BeyondServiceImpl>();
            this.client = client;
            permUid = uid;
            permGid = gid;
            this.crypto = c;
            this.cache = new Cache(client, immutableCacheSize, mutableCacheDuration);
            if (fsName != null)
                SetFilesystem(fsName);
            //GetRoot(); // ping
            if (crypto != null)
            {
                crypto.GetKeyBlock = async k => await client.QueryAsync(k);
                try
                {
                    var root = GetRoot();
                    if (root.Block.Data?.Admins != null)
                    {
                        crypto.SetAdmins(root.Block.Data.Admins.KeyHashes);
                    }
                }
                catch (Exception e)
                {
                    logger.LogInformation(e, "Cannot fetch admin keys");
                }
            }
        }
        public void MkFS()
        {
            logger.LogInformation("MAKING NEW FILESYSTEM");
            // FIXME: add a safety check
            if (crypto != null)
            { // use a pointer block
                var root = new BlockAndKey();
                root.Block = new Block();
                root.Block.Version = 1;
                root.Block.Data = new BlockData();
                root.Block.Data.Directory = new DirectoryIndex();
                root.Block.Data.Mode = "777";
                crypto.SealMutable(root, BlockChange.Data | BlockChange.Readers, null).Wait();
                client.Insert(root);
                var ptr = new BlockAndKey();
                ptr.Key = RootAddress();
                ptr.Block = new Block();
                ptr.Block.Pointer = root.Key;
                client.Insert(ptr);
            }
            else
            {
                var root = new BlockAndKey();
                root.Key = RootAddress();
                root.Block = new Block();
                root.Block.Version = 1;
                root.Block.Data = new BlockData();
                root.Block.Data.Directory = new DirectoryIndex();
                root.Block.Data.Mode = "777";
                client.Insert(root);
            }
        }
        private void SetFilesystem(string name)
        {
            byte[] hashValue = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(name));
            rootAddress = hashValue;
        }
        protected Key RootAddress()
        {
            var res = new Key();
            res.Key_ = Google.Protobuf.ByteString.CopyFrom(rootAddress);
            return res;
        }
        protected BlockAndKey GetRoot()
        {
            BlockAndKey res = new BlockAndKey();
            res.Key = RootAddress();
            res.Block = cache.GetMutable(res.Key);
            if (crypto == null)
                return res;
            var rp = new BlockAndKey();
            rp.Key = res.Block.Pointer;
            rp.Block = cache.GetMutable(rp.Key);
            crypto.UnsealMutable(rp).Wait();
            return rp;
        }
        protected Errno Get(string path, out BlockAndKey block, bool parent=false)
        {
            block = null;
            var components = path.Split('/');
            logger.LogInformation("GET {path} {components}", path, components.Length);
            BlockAndKey current = GetRoot();
            for (var i = 1; i<components.Length - (parent?1:0); ++i)
            {
                if (components[i] == "")
                    continue;
                var hit = false;
                if (current?.Block?.Data?.Directory?.Entries == null)
                {
                    logger.LogInformation("null data at {index}", i);
                    return Errno.ENOENT;
                }
                foreach (var ent in current.Block.Data.Directory.Entries)
                {
                    if (ent.Name == components[i])
                    {
                        var blk = cache.GetMutable(ent.Address);
                        current.Key = ent.Address;
                        current.Block = blk;
                        if (crypto != null)
                        {
                            var ok = crypto.UnsealMutable(current).Result;
                            if (ok == null)
                                return Errno.EPERM;
                        }
                        hit = true;
                        break;
                    }
                }
                if (!hit)
                {
                    logger.LogWarning("No hit for {component}", components[i]);
                    return Errno.ENOENT;
                }
            }
            block = current;
            return 0;
        }
        protected override Errno OnGetPathStatus (string path, out Stat buf)
		{
		    buf = new Stat();
		    if (path.StartsWith("/beyond:"))
		    {
		        buf.st_mode = FilePermissions.S_IFREG |  NativeConvert.FromOctalPermissionString("644");
		        buf.st_blksize = 65536;
		        buf.st_uid = permUid;
		        buf.st_gid = permGid;
		        buf.st_nlink = 1;
		        logger.LogInformation("STAT META {path} OK", path);
		        return 0;
		    }
		    try
		    {
		        logger.LogInformation("STAT {path}", path);
		        var components = path.Split('/');
		        var filename = components[components.Length-1];
		        var err = Get(path, out var parent, parent:true);
		        if (err != 0)
		        {
		            logger.LogInformation("get failed, returning {errno}", err);
		            return err;
		        }
		        var block = new BlockAndKey();
		        var writeable = true;
		        if (path == "/")
		        {
		            buf.st_mode = FilePermissions.S_IFDIR;
		            block = parent;
		        }
		        else
		        {
		            var hit = parent.Block.Data?.Directory?.Entries?.Where(x=>x.Name == filename).FirstOrDefault();
		            if (hit == null)
		                return Errno.ENOENT;
		            buf.st_mode = hit.EntryType switch
		            {
		                DirectoryEntry.Types.EntryType.File =>  FilePermissions.S_IFREG,
		                DirectoryEntry.Types.EntryType.Directory =>  FilePermissions.S_IFDIR,
		                DirectoryEntry.Types.EntryType.Symlink =>  FilePermissions.S_IFLNK,
		            };
		            var rawblock = cache.GetMutable(hit.Address);
		            block.Key = hit.Address;
		            block.Block = rawblock;
		            if (crypto != null)
		            {
		                var aes = crypto.UnsealMutable(block).Result;
		                if (aes == null)
		                {
		                    logger.LogInformation("STAT {path} PARTIAL", path);
		                    return 0;
		                }
		                if (!crypto.CanWrite(block))
		                    writeable = false;
		            }
		        }

		        buf.st_blksize = 65536;
		        buf.st_uid = permUid;
		        buf.st_gid = permGid;
		        buf.st_nlink = 1;

		        buf.st_mode |= NativeConvert.FromOctalPermissionString(block.Block.Data.Mode);
		        if (!writeable)
		            buf.st_mode &= ~(FilePermissions.S_IWUSR | FilePermissions.S_IWGRP | FilePermissions.S_IWOTH);
		        logger.LogInformation("MODE PRE {mode}", buf.st_mode);
		        logger.LogInformation("MODE POST {mode}", buf.st_mode);
		        buf.st_size = (block.Block.Data.File != null) ? (long)block.Block.Data.File.Size : 1024;
		        buf.st_blocks = buf.st_size / 512;
		        buf.st_ctime = block.Block.Ctime;
		        buf.st_mtime = block.Block.Mtime;

		        logger.LogInformation("STAT {path} OK", path);
		        return 0;
		    }
		    catch (Exception e)
		    {
		        logger.LogError("unexpected bronk {exception}", e);
		        return Errno.EIO;
		    }
		}
		protected override Errno OnAccessPath (string path, AccessModes mask)
		{
		    logger.LogInformation("ACCESS {path}", path);
		    return 0; // sure, try, whatever
		}
		protected override Errno OnReadSymbolicLink (string path, out string target)
		{
		    target = null;
		    BlockAndKey file = null;
		    var err = Get(path, out file);
		    if (err != 0)
		        return err;
		    target = file.Block.Data.SymLink.Target;
		    return 0;
		}
		protected override Errno OnReadDirectory (string directory, OpenedPathInfo info, 
				out IEnumerable<Mono.Fuse.NETStandard.DirectoryEntry> paths)
		{
		    logger.LogInformation("READDIR {path}", directory);
		    paths = null;
		    BlockAndKey block;
		    var err = Get(directory, out block);
		    if (err != 0)
		        return err;
		    var res = new List<Mono.Fuse.NETStandard.DirectoryEntry>();
		    foreach (var ent in block.Block.Data.Directory.Entries)
		    {
		        res.Add(new Mono.Fuse.NETStandard.DirectoryEntry(ent.Name));
		    }
		    paths = res;
		    return 0;
		}
		protected override Errno OnCreateSpecialFile (string path, FilePermissions mode, ulong rdev)
		{
		    return Errno.ENOENT;
		}
		delegate void RetryUpdate(BlockAndKey block);
		private Errno UpdateBlock(string path, bool parentPath, BlockAndKey block, RetryUpdate retry)
		{
		    retry(block);
		    while (true)
		    {
		        if (crypto != null)
		        {
		            var aes = crypto.ExtractKey(block).Result; 
		            crypto.SealMutable(block, BlockChange.Data, aes).Wait();
		        }
		        var res = cache.UpdateMutable(block);
		        if (res.Code == Error.Types.ErrorCode.Ok)
		            break;
		        if (res.Code != Error.Types.ErrorCode.AlreadyLocked
		            && res.Code != Error.Types.ErrorCode.Conflict
		        && res.Code !=  Error.Types.ErrorCode.Outdated)
		        {
		            logger.LogWarning("UpdateBlock got fatal: {code}", res.Code);
		          return Errno.EIO;
		        }
		        // try again
		        logger.LogInformation("UpdateBlock is retrying {code}", res.Code);
		        var err = Get(path, out block, parent:parentPath);
		        if (err != 0)
		        {
		            logger.LogWarning("UpdateBlock failed to refetch: {error}", err);
		            return err;
		        }
		        retry(block);
		    }
		    return 0;
		}
		protected override Errno OnCreateDirectory (string path, FilePermissions mode)
		{
		    logger.LogInformation("MKDIR {path}", path);
		    BlockAndKey parent;
		    var err = Get(path, out parent, parent:true);
		    if (err != 0)
		        return err;
		    var components = path.Split('/');
		    var last = components[components.Length-1];
		    // first create and push the new directory block
		    var bak = new BlockAndKey();
		    if (crypto == null)
		        bak.Key = Utils.RandomKey();
		    bak.Block = new Block();
		    bak.Block.Version = 1;
		    bak.Block.Data = new BlockData();
		    bak.Block.Data.Directory = new DirectoryIndex();
		    bak.Block.Data.Mode = NativeConvert.ToOctalPermissionString(mode);
		    bak.Block.InheritReaders = parent.Block.InheritReaders;
		    bak.Block.InheritWriters = parent.Block.InheritWriters;
		    bak.Block.Mtime = DateTimeOffset.Now.ToUnixTimeSeconds();
		    bak.Block.Ctime = bak.Block.Mtime;
		    if (crypto != null)
		    {
		        crypto.Inherit(bak, parent, bak.Block.InheritReaders, bak.Block.InheritWriters);
		        crypto.SealMutable(bak, BlockChange.All, null).Wait();
		    }
		    client.Insert(bak);
		    var dirent = new DirectoryEntry();
		    dirent.EntryType = DirectoryEntry.Types.EntryType.Directory;
		    dirent.Name = last;
		    dirent.Address = bak.Key;
		    // then link it
		    UpdateBlock(path, true, parent, p => {
		            p.Block.Data.Directory.Entries.Add(dirent);
		            p.Block.Version = p.Block.Version + 1;
		    });
		    return 0;
		}
		private Errno Unlink(string filePath, BlockAndKey dir, string name)
		{
		    return UpdateBlock(filePath, true, dir, p => {
		            var count = p.Block.Data.Directory.Entries.Count;
		            for (var i = 0; i<count; ++i)
		            {
		                if (p.Block.Data.Directory.Entries[i].Name == name)
		                {
		                    p.Block.Data.Directory.Entries.RemoveAt(i);
		                    p.Block.Version = p.Block.Version + 1;
		                    return;
		                }
		            }
		    });
		}
		protected override Errno OnRemoveFile (string path)
		{
		    var comps = path.Split('/');
		    var fileName = comps[comps.Length-1];
		    logger.LogInformation("RMF {path}", path);
		    BlockAndKey file;
		    var err = Get(path, out file);
		    if (err != 0)
		        return err;
		    BlockAndKey parent;
		    err = Get(path, out parent, parent: true);
		    if (err != 0)
		        return err;
		    if (crypto != null)
		    {
		        if (!crypto.CanWrite(parent) || !crypto.CanWrite(file))
		            return Errno.EPERM;
		    }
		    err = Unlink(path, parent, fileName);
		    if (err != 0)
		        return err;
		    if (file.Block.Data.File != null)
		    {
		        foreach (var b in file.Block.Data.File.Blocks)
		        {
		            if (crypto != null)
		                client.Delete(crypto.DeletionRequest(b.Address, file.Key));
		            else
		                client.Delete(new BlockAndKey{Key = b.Address});
		        }
		    }
		    if (crypto != null)
		        client.Delete(crypto.DeletionRequest(file.Key, file.Key));
		    else
		        client.Delete(new BlockAndKey {Key = file.Key});
		    return 0;
		}
		protected override Errno OnRemoveDirectory (string path)
		{
		    var comps = path.Split('/');
		    var fileName = comps[comps.Length-1];
		    logger.LogInformation("RMD {path}", path);
		    BlockAndKey file;
		    var err = Get(path, out file);
		    if (err != 0)
		        return err;
		    if (file.Block.Data.Directory.Entries.Count != 0)
		        return Errno.ENOTEMPTY;
		    BlockAndKey parent;
		    err = Get(path, out parent, parent: true);
		    if (err != 0)
		        return err;
		    if (crypto != null)
		    {
		        if (!crypto.CanWrite(parent) || !crypto.CanWrite(file))
		            return Errno.EPERM;
		    }
		    err = Unlink(path, parent, fileName);
		    if (err != 0)
		        return err;
		    if (crypto != null)
		        client.Delete(crypto.DeletionRequest(file.Key, file.Key));
		    else
		        client.Delete(new BlockAndKey {Key = file.Key});
		    return 0;
		}
		protected override Errno OnCreateSymbolicLink (string target, string path)
		{
		    var comps = path.Split('/');
		    var fileName = comps[comps.Length-1];
		    BlockAndKey parent = null;
		    var err = Get(path, out parent, parent: true);
		    if (err != 0)
		        return err;
		    var file = new BlockAndKey();
		    file.Key = Utils.RandomKey();
		    file.Block = new Block();
		    file.Block.Version = 1;
		    file.Block.Data = new BlockData();
		    file.Block.Data.SymLink = new SymLink();
		    file.Block.Data.SymLink.Target = target;
		    file.Block.Data.Mode = "777";
		    if (crypto != null)
		    {
		        file.Key = null;
		        file.Block.Salt = Utils.RandomKey().Key_;
		        var key = new AESKey();
		        key.Key = Utils.RandomKey().Key_;
		        key.Iv = Utils.RandomKey().Key_;
		        crypto.SealMutable(file, BlockChange.All, key).Wait();
		    }
		    client.Insert(file);
		    var dirent = new DirectoryEntry();
		    dirent.EntryType = DirectoryEntry.Types.EntryType.Symlink;
		    dirent.Name = fileName;
		    dirent.Address = file.Key;
		    err = UpdateBlock(path, true, parent, b => {
		            b.Block.Version = b.Block.Version + 1;
		            b.Block.Data.Directory.Entries.Add(dirent);
		    });
		    return 0;
		}
		protected override Errno OnRenamePath (string from, string to)
		{
		    logger.LogInformation("MV {from} {to}", from, to);
		    try
		    {
		        BlockAndKey todir = null;
		        BlockAndKey fromdir = null;
		        var compsfrom = from.Split('/');
		        var fileNameFrom = compsfrom[compsfrom.Length-1];
		        var compsto = to.Split('/');
		        var fileNameTo = compsto[compsto.Length-1];
		        var err = Get(from, out fromdir, parent:true);
		        if (err != 0)
		            return err;
		        err = Get(to, out todir, parent:true);
		        if (err != 0)
		            return err;
		        var exists = todir.Block.Data.Directory.Entries.Where(e=>e.Name == fileNameTo).FirstOrDefault();
		        if (exists != null)
		        {
		            if (exists.EntryType == DirectoryEntry.Types.EntryType.Directory)
		                return Errno.EISDIR;
		            err = OnRemoveFile(to);
		            if (err != 0)
		                return err;
		        }
		        var de = fromdir.Block.Data.Directory.Entries.Where(e=>e.Name == fileNameFrom).FirstOrDefault();
		        if (de == null)
		            return Errno.ENOENT;
		        var det = new DirectoryEntry();
		        det.EntryType = de.EntryType;
		        det.Name = fileNameTo;
		        det.Address = de.Address;
		        err = UpdateBlock(to, true, todir, b => {
		                b.Block.Version = b.Block.Version + 1;
		                b.Block.Data.Directory.Entries.Add(det);
		        });
		        if (err != 0)
		            return err;
		        err = UpdateBlock(from, true, fromdir, b => {
		                b.Block.Version = b.Block.Version + 1;
		                var deb = b.Block.Data.Directory.Entries.Where(e=>e.Name == fileNameFrom).FirstOrDefault();
		                if (deb != null)
		                    b.Block.Data.Directory.Entries.Remove(deb);
		        });
		        if (err != 0)
		            return err;
		        return 0;
		    }
		    catch(Exception e)
		    {
		        logger.LogError(e, "Exception in MV");
		        throw;
		    }
		}
		protected override Errno OnCreateHardLink (string from, string to)
		{
		    logger.LogInformation("HARDLNK {from} {to}", from, to);
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnChangePathPermissions (string path, FilePermissions mode)
		{
		    BlockAndKey block = null;
		    var err = Get(path, out block);
		    if (err != 0)
		        return err;
		    err = UpdateBlock(path, true, block, b => {
		            b.Block.Version = b.Block.Version + 1;
		            b.Block.Data.Mode = NativeConvert.ToOctalPermissionString(mode);
		    });
		    return err;
		}
		protected override Errno OnChangePathOwner (string path, long uid, long gid)
		{
		    return 0;
		}
		protected override Errno OnTruncateFile (string path, long size)
		{
		    logger.LogInformation("TRUNK {path} {size}", path, size);
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnChangePathTimes (string path, ref Utimbuf buf)
		{
		    return 0;
		}
		protected override Errno OnCreateHandle (string file, OpenedPathInfo info, FilePermissions mode)
		{
			return OpenOrCreate(file, true, info, mode);
		}
		protected override Errno OnOpenHandle (string path, OpenedPathInfo info)
		{
		    return OpenOrCreate(path, false, info);
		}
		private Errno OpenOrCreate(string path, bool allowCreation, OpenedPathInfo info, FilePermissions? mode=null)
		{
		    var amask = info.OpenFlags  & (OpenFlags.O_RDONLY | OpenFlags.O_WRONLY | OpenFlags.O_RDWR);
		    var canWrite = amask == OpenFlags.O_WRONLY || amask == OpenFlags.O_RDWR;
		    logger.LogInformation("OPEN {path}", path);
		    try
		    {
		        if (openedFiles.TryGetValue(path, out var oh))
		        {
		            oh.openCount += 1;
		            logger.LogInformation("Already opened");
		            return 0;
		        }
		        BlockAndKey file;
		        var err = Get(path, out file);
		        AESKey key = null;
		        if (err == Errno.ENOENT)
		        {
		            if (!allowCreation)
		            {
		                logger.LogInformation("OPEN returns NOENT");
		                return err;
		            }
		            BlockAndKey parent;
		            err = Get(path, out parent, parent: true);
		            if (err != 0)
		            {
		                logger.LogInformation("failed to get parent: {error}", err);
		                return err;
		            }
		            file = new BlockAndKey();
		            file.Key = Utils.RandomKey();
		            file.Block = new Block();
		            file.Block.Version = 1;
		            file.Block.Data = new BlockData();
		            file.Block.Data.Mode = NativeConvert.ToOctalPermissionString(mode.Value);
		            file.Block.Mtime = DateTimeOffset.Now.ToUnixTimeSeconds();
		            file.Block.Ctime = file.Block.Mtime;
		            if (crypto != null)
		            {
		                file.Block.Salt = Utils.RandomKey().Key_;
		                file.Block.InheritReaders = parent.Block.InheritReaders;
		                file.Block.InheritWriters = parent.Block.InheritWriters;
		                crypto.Inherit(file, parent, file.Block.InheritReaders, file.Block.InheritWriters);
		                key = new AESKey();
		                key.Key = Utils.RandomKey().Key_;
		                key.Iv = Utils.RandomKey().Key_;
		                var fseal = file.Clone();
		                fseal.Key = null;
		                crypto.SealMutable(fseal, BlockChange.All, key).Wait();
		                client.Insert(fseal);
		                file.Key = fseal.Key;
		                file.Block.Owner = fseal.Block.Owner;
		            }
		            else
		                client.Insert(file);
		            var comps = path.Split('/');
		            var fileName = comps[comps.Length-1];
		            var dirent = new DirectoryEntry();
		            dirent.EntryType = DirectoryEntry.Types.EntryType.File;
		            dirent.Name = fileName;
		            dirent.Address = file.Key;
		            err = UpdateBlock(path, true, parent, b => {
		                    b.Block.Version = b.Block.Version + 1;
		                    b.Block.Data.Directory.Entries.Add(dirent);
		            });
		        }
		        else if (err != 0)
		        {
		            logger.LogInformation("failed to write block: {err}", err);
		            return err;
		        }
		        else
		            key = crypto?.ExtractKey(file).Result;
		        if (file.Block.Data.File == null)
		            file.Block.Data.File = new FileIndex();
		        var ofh = new OpenedHandle
		            {
		                openCount = 1,
		                fileBlock = file,
		                key = key,
		            };
		        if (canWrite)
		        {
		            // do a write check now, otherwise errors
		            // will happen at close() time which is too late
		            err = FlushFile(ofh);
		            if (err != 0)
		                return err;
		        }
		        openedFiles.TryAdd(path, ofh);
		        logger.LogInformation("File block: {file}", file);
		        logger.LogInformation("Open success");
		        return 0;
		    }
		    catch(Exception e)
		    {
		        logger.LogError("Exception in open: {exception}", e);
		        throw;
		    }
		}
		private Errno ReadFromChunk(OpenedHandle oh, ulong chunkIndex, long offset, byte[] buf, int bufOffset, out int nRead)
		{
		    nRead = 0;
		    if (!oh.chunks.TryGetValue(chunkIndex, out var chunk))
		    {
		        if (chunkIndex >= (ulong)oh.fileBlock.Block.Data.File.Blocks.Count)
		            return 0;
		        var baddr = oh.fileBlock.Block.Data.File.Blocks[(int)chunkIndex].Address;
		        var block = cache.GetImmutable(baddr);
		        if (crypto != null)
		        {
		            var bak = new BlockAndKey();
		            bak.Key = baddr;
		            bak.Block = block;
		            crypto.UnsealImmutable(bak, oh.key);
		        }
		        
		        chunk = new FileChunk {
		            atime = DateTime.Now,
		            dirty = false,
		            data = block.Raw.ToByteArray(),
		            version = (ulong)block.Version,
		        };
		        oh.chunks.TryAdd(chunkIndex, chunk);
		    }
		    logger.LogInformation("ReadFromChunk idx {index} off {offset} clen {chunkLenght} blen {bufLength} boff {bufOffset}", chunkIndex, offset, chunk.data.Length, buf.Length, bufOffset);
		    var len = Math.Min(chunk.data.Length - offset, buf.Length - bufOffset);
		    Array.Copy(chunk.data, offset, buf, bufOffset, len);
		    nRead = (int)len;
		    return 0;
		}
		protected override Errno OnReadHandle (string path, OpenedPathInfo info, byte[] buf, 
				long offset, out int bytesRead)
		{
		    logger.LogInformation("READ {path} {offset} {len}", path, offset, buf.Length);
		    try
		    {
		        bytesRead = 0;
		        if (!openedFiles.TryGetValue(path, out var oh))
		            return Errno.EBADF;
		        ulong start_chunk = (ulong)((ulong)offset / CHUNK_SIZE);
		        ulong end_chunk = (ulong)((ulong)(offset + buf.Length-1) / CHUNK_SIZE);
		        logger.LogInformation("CHUNKS {startChunk} {endChunk}", start_chunk, end_chunk);
		        for (var c = start_chunk; c <= end_chunk; ++c)
		        {
		            int read = 0;
		            var err = ReadFromChunk(oh, c, offset + bytesRead - (long)c * (long)CHUNK_SIZE, buf, bytesRead, out read);
		            if (err != 0)
		                return err;
		            bytesRead += read;
		        }
		    }
		    catch (Exception e)
		    {
		        logger.LogError(e, "read exception");
		        throw;
		    }
		    logger.LogInformation("READ returning {bytesRead}", bytesRead);
		    return 0;
		}
		private Errno WriteToChunk(OpenedHandle oh, ulong chunkIndex, long offset,
		                         byte[] buf, int bufOffset, out int nWrite)
		{
		    nWrite = 0;
		    if (!oh.chunks.TryGetValue(chunkIndex, out var chunk))
		    {
		        if (chunkIndex >= (ulong)oh.fileBlock.Block.Data.File.Blocks.Count)
		        {
		            for (int ci = oh.fileBlock.Block.Data.File.Blocks.Count; ci <= (int)chunkIndex; ++ci)
		            {
		                BlockAndKey bak = new BlockAndKey();
		                if (crypto == null)
		                {
		                    bak.Key = Utils.RandomKey();
		                    bak.Block = new Block();
		                    bak.Block.Version = 1;
		                    client.Insert(bak);
		                }
		                var fb = new FileBlock();
		                fb.Address = bak.Key;
		                oh.fileBlock.Block.Data.File.Blocks.Add(fb);
		                oh.dirty = true;
		            }
		            chunk = new FileChunk
		            {
		                atime = DateTime.Now,
		                dirty = true,
		                data = new byte[0],
		                version = 1,
		            };
		        }
		        else
		        {
		            var baddr = oh.fileBlock.Block.Data.File.Blocks[(int)chunkIndex].Address;
		            var bl = cache.GetImmutable(baddr);
		            if (crypto != null)
		            {
		                 var bak = new BlockAndKey();
		                 bak.Key = baddr;
		                 bak.Block = bl;
		                 crypto.UnsealImmutable(bak, oh.key);
		            }
		            chunk = new FileChunk
		            {
		                atime = DateTime.Now,
		                dirty = false,
		                data = bl.Raw.ToByteArray(),
		                version = (ulong)bl.Version,
		            };
		        }
		        
		        oh.chunks.TryAdd(chunkIndex, chunk);
		    }
		    nWrite = (int)Math.Min(buf.Length - bufOffset, (long)CHUNK_SIZE - offset);
		    if (chunk.data.Length < nWrite + offset)
		    {
		        var nd = new byte[nWrite + offset];
		        Array.Copy(chunk.data, 0, nd, 0, chunk.data.Length);
		        chunk.data = nd;
		    }
		    Array.Copy(buf, bufOffset, chunk.data, offset, nWrite);
		    chunk.dirty = true;
		    if (nWrite + offset == (long)CHUNK_SIZE)
		        return FlushChunk(oh, (int)chunkIndex);
		    return 0;
		}
		protected override Errno OnWriteHandle (string path, OpenedPathInfo info,
				byte[] buf, long offset, out int bytesWritten)
		{
		    logger.LogInformation("WRITE {path} {offset} {len}", path, offset, buf.Length);
		    try
		    {
		        bytesWritten = 0;
		        if (!openedFiles.TryGetValue(path, out var oh))
		            return Errno.EBADF;
		        ulong start_chunk = (ulong)((ulong)offset / CHUNK_SIZE);
		        ulong end_chunk = (ulong)((ulong)(offset + buf.Length-1) / CHUNK_SIZE);
		        for (var c = start_chunk; c <= end_chunk; ++c)
		        {
		            int wr = 0;
		            var err = WriteToChunk(oh, c, offset - (long)c * (long)CHUNK_SIZE, buf, bytesWritten, out wr);
		            if (err != 0)
		                return err;
		            bytesWritten += wr;
		        }
		        ulong newSz = (ulong)offset + (ulong)buf.Length;
		        if (oh.fileBlock.Block.Data.File.Size < newSz)
		        {
		            oh.fileBlock.Block.Data.File.Size = newSz;
		            oh.dirty = true;
		        }
		    }
		    catch (Exception e)
		    {
		        logger.LogError(e, "Write exception");
		        throw;
		    }
		    logger.LogInformation("WRITE returning {bytesWritten}", bytesWritten);
		    return 0;
		}
		private Errno FlushFile(OpenedHandle oh)
		{
		    logger.LogInformation("Flush file with {block}", oh.fileBlock);
		    oh.fileBlock.Block.Mtime = DateTimeOffset.Now.ToUnixTimeSeconds();
		    while (true)
		    {
		        var fbc = oh.fileBlock;
		        if (crypto != null)
		        {
		            fbc = fbc.Clone();
		            fbc.Block = fbc.Block.Clone();
		            crypto.SealMutable(fbc, BlockChange.Data, oh.key).Wait();
		        }
		        var res = cache.UpdateMutable(fbc);
		        if (res.Code == Error.Types.ErrorCode.Ok)
		            break;
		        if (res.Code != Error.Types.ErrorCode.AlreadyLocked
		            && res.Code != Error.Types.ErrorCode.Conflict
		        && res.Code !=  Error.Types.ErrorCode.Outdated)
		          return Errno.EIO;
		        logger.LogInformation("Flush retry from {version}", oh.fileBlock.Block.Version);
		        var current = cache.GetMutable(oh.fileBlock.Key, true);
		        oh.fileBlock.Block.Version = current.Version + 1;
		    }
		    return 0;
		}
		private Errno FlushChunk(OpenedHandle oh, int chunkIndex)
		{
		    if (!oh.chunks.TryGetValue((ulong)chunkIndex, out var chunk))
		        return 0;
		    var bak = new BlockAndKey();
		    bak.Key = oh.fileBlock.Block.Data.File.Blocks[chunkIndex].Address;
		    bak.Block = new Block();
		    bak.Block.Version = (long)(chunk.version+1);
		    bak.Block.Raw = Google.Protobuf.ByteString.CopyFrom(chunk.data);
		    bak.Block.OwningBlock = oh.fileBlock.Key;
		    if (crypto != null)
		    {
		        var prevAddr = bak.Key;
		        crypto.SealImmutable(bak, oh.key).Wait();
		        if (prevAddr != null && prevAddr.Equals(bak.Key))
		            return 0; // actually can't happen, we resalt
		        logger.LogInformation("Immutable replace {old} to {new}", prevAddr, bak.Key);
		        client.Insert(bak);
		        if (prevAddr != null)
		            client.Delete(crypto.DeletionRequest(prevAddr, oh.fileBlock.Key));
		        oh.fileBlock.Block.Data.File.Blocks[chunkIndex].Address = bak.Key;
		        oh.dirty = true;
		        chunk.dirty = false;
		        return 0;
		    }
		    while (true)
		    {
		        var res = client.TransactionalUpdate(bak);
		        if (res.Code == Error.Types.ErrorCode.Ok)
		            break;
		        if (res.Code != Error.Types.ErrorCode.AlreadyLocked
		            && res.Code != Error.Types.ErrorCode.Conflict
		        && res.Code !=  Error.Types.ErrorCode.Outdated)
		          return Errno.EIO;
		        var current = client.Query(bak.Key);
		        bak.Block.Version = current.Version + 1;
		    }
		    chunk.dirty = false;
		    return 0;
		}
		/*protected override Errno OnGetFileSystemStatus (string path, out Statvfs stbuf)
		{
		    stbuf = new Statvfs();
		    return Errno.EOPNOTSUPP;
		}*/
		protected override Errno OnReleaseHandle (string path, OpenedPathInfo info)
		{
		    logger.LogInformation("CLOSE {path}", path);
		    if (openedFiles.TryGetValue(path, out var oh))
		    {
		        oh.openCount -= 1;
		        logger.LogInformation("open count: {open}", oh.openCount);
		        if (oh.openCount == 0)
		        { // flush
		            try
		            {
		                foreach (var kv in oh.chunks)
		                {
		                    if (kv.Value.dirty)
		                    {
		                        FlushChunk(oh, (int)kv.Key);
		                    }
		                }
		                if (oh.dirty)
		                {
		                    FlushFile(oh);
		                }
		                openedFiles.TryRemove(path, out _);
		            }
		            catch (Exception e)
		            {
		                logger.LogError(e, "Exception while flushing");
		                throw;
		            }
		        }
		    }
		    return 0;
		}
		protected override Errno OnSynchronizeHandle (string path, OpenedPathInfo info, bool onlyUserData)
		{
		    return 0;
		}
		private Key GetAlias(string als)
		{
		    var err = Get("/", out var file);
		    if (err != 0)
		        return null;
		    var hit = file.Block.Data.Aliases.KeyAliases.Where(x=>x.Alias == als).FirstOrDefault();
		    if (hit == null)
		        return null;
		    return hit.KeyHash;
		}
		private Errno Update(BlockAndKey file, bool groupRem=false)
		{
		    file.Block.Version += 1;
		    if (crypto != null)
		    {
		        var bk = crypto.ExtractKey(file).Result;
		        if (bk == null)
		            return Errno.EPERM;
		        var bc = BlockChange.All;
		        if (groupRem)
		            bc |= BlockChange.GroupRemove;
		        crypto.SealMutable(file, bc, bk).Wait();
		    }
		    var res = cache.UpdateMutable(file);
		    if (res.Code != Error.Types.ErrorCode.Ok)
		    {
		        logger.LogWarning("Update failure with {code}", res.Code);
		        return Errno.EIO;
		    }
		    return 0;
		}
		protected override Errno OnSetPathExtendedAttribute (string path, string name, byte[] value, XattrFlags flags)
		{
		    if (name.StartsWith("beyond."))
		    {
		        try
		        {
		            BlockAndKey file = null;
		            Errno err = 0;
		            if (path.StartsWith("/beyond:"))
		            {
		                var bid = path.Substring("/beyond:".Length);
		                if (bid.Length == 64)
		                {
		                    var k = Utils.StringKey(bid);
		                    file = new BlockAndKey();
		                    file.Key = k;
		                    file.Block = client.Query(file.Key);
		                }
		                else
		                {
		                    err = Get("/", out var root);
		                    if (err != 0)
		                        return err;
		                    var hit = root.Block.Data.Aliases.KeyAliases.Where(x=>x.Alias == bid).FirstOrDefault();
		                    if (hit == null)
		                    {
		                        logger.LogWarning("alias {alias} not found", bid);
		                        return Errno.ENOENT;
		                    }
		                    file = new BlockAndKey();
		                    file.Key = hit.KeyHash;
		                    file.Block = client.Query(file.Key);
		                }
		            }
		            else
		                err = Get(path, out file);
		            if (err != 0)
		            {
		                logger.LogWarning("setxattr failed to get {path} with {errno}", path, err);
		                return err;
		            }
		            if (name == "beyond.addreader")
		            {
		                var keySpec = System.Text.Encoding.UTF8.GetString(value);
		                var keyAddr = keySpec.Length == 64 ? Utils.StringKey(keySpec) : GetAlias(keySpec);
		                if (keyAddr == null)
		                    return Errno.EIO;
		                var pkb = client.Query(keyAddr);
		                var exists = file.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(keyAddr)).FirstOrDefault();
		                if (exists != null)
		                    return 0;
		                file.Block.Readers.EncryptionKeys.Add(new EncryptionKey { Recipient = keyAddr});
		                return Update(file);
		            }
		            if (name == "beyond.removereader")
		            {
		                var keySpec = System.Text.Encoding.UTF8.GetString(value);
		                var keyAddr = keySpec.Length == 64 ? Utils.StringKey(keySpec) : GetAlias(keySpec);
		                if (keyAddr == null)
		                    return Errno.EIO;
		                var exists = file.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(keyAddr)).FirstOrDefault();
		                if (exists == null)
		                    return Errno.ENOENT;
		                file.Block.Readers.EncryptionKeys.Remove(exists);
		                return Update(file, file.Block.Data.GroupKeys != null);
		            }
		            if (name == "beyond.addwriter")
		            {
		                var keySpec = System.Text.Encoding.UTF8.GetString(value);
		                var keyAddr = keySpec.Length == 64 ? Utils.StringKey(keySpec) : GetAlias(keySpec);
		                if (keyAddr == null)
		                    return Errno.EIO;
		                var pkb = client.Query(keyAddr);
		                if (file.Block.Writers == null)
		                    file.Block.Writers = new KeyHashList();
		                var exists = file.Block.Writers.KeyHashes.Where(x=>x.Equals(keyAddr)).FirstOrDefault();
		                if (exists != null)
		                    return 0;
		                file.Block.Writers.KeyHashes.Add(keyAddr);
		                return Update(file);
		            }
		            if (name == "beyond.removewriter")
		            {
		                var keySpec = System.Text.Encoding.UTF8.GetString(value);
		                var keyAddr = keySpec.Length == 64 ? Utils.StringKey(keySpec) : GetAlias(keySpec);
		                if (keyAddr == null)
		                    return Errno.EIO;  
		                if (file.Block.Writers == null)
		                    return Errno.ENOENT;
		                var exists = file.Block.Writers.KeyHashes.Where(x=>x.Equals(keyAddr)).FirstOrDefault();
		                if (exists == null)
		                    return Errno.ENOENT;
		                file.Block.Writers.KeyHashes.Remove(exists);
		                return Update(file);
		            }
		            if (name == "beyond.addadmin")
		            {
		                var keySpec = System.Text.Encoding.UTF8.GetString(value);
		                var keyAddr = keySpec.Length == 64 ? Utils.StringKey(keySpec) : GetAlias(keySpec);
		                if (keyAddr == null)
		                    return Errno.EIO;
		                var pkb = client.Query(keyAddr);
		                if (file.Block.Data.Admins == null)
		                    file.Block.Data.Admins = new KeyHashList();
		                var exists = file.Block.Data.Admins.KeyHashes.Where(x=>x.Equals(keyAddr)).FirstOrDefault();
		                if (exists != null)
		                    return 0;
		                file.Block.Data.Admins.KeyHashes.Add(keyAddr);
		                return Update(file);
		            }
		            if (name == "beyond.removeadmin")
		            {
		                var keySpec = System.Text.Encoding.UTF8.GetString(value);
		                var keyAddr = keySpec.Length == 64 ? Utils.StringKey(keySpec) : GetAlias(keySpec);
		                if (keyAddr == null)
		                    return Errno.EIO;  
		                if (file.Block.Data.Admins == null)
		                    return Errno.ENOENT;
		                var exists = file.Block.Data.Admins.KeyHashes.Where(x=>x.Equals(keyAddr)).FirstOrDefault();
		                if (exists == null)
		                    return Errno.ENOENT;
		                file.Block.Data.Admins.KeyHashes.Remove(exists);
		                return Update(file);
		            }
		            if (name == "beyond.addalias")
		            {
		                var ak = System.Text.Encoding.UTF8.GetString(value).Split(':');
		                var als = ak[0];
		                var keyAddr = Utils.StringKey(ak[1]);
		                var pkb = client.Query(keyAddr);
		                if (file.Block.Data.Aliases == null)
		                    file.Block.Data.Aliases = new KeyAliasList();
		                var exists = file.Block.Data.Aliases.KeyAliases.Where(x=>x.Alias == als).FirstOrDefault();
		                if (exists != null)
		                    return Errno.EEXIST;
		                file.Block.Data.Aliases.KeyAliases.Add(new KeyAlias
		                   {
		                       Alias = als,
		                       KeyHash = keyAddr,
		                   });
		                return Update(file);
		            }
		            if (name == "beyond.removealias")
		            {
		                var als = System.Text.Encoding.UTF8.GetString(value);
		                if (file.Block.Data.Aliases == null)
		                    return Errno.ENOENT;
		                var exists = file.Block.Data.Aliases.KeyAliases.Where(x=>x.Alias == als).FirstOrDefault();
		                if (exists == null)
		                    return Errno.ENOENT;
		                file.Block.Data.Aliases.KeyAliases.Remove(exists);
		                return Update(file);
		            }
		            if (name == "beyond.inherit")
		            {
		                var mode = System.Text.Encoding.UTF8.GetString(value).ToLower();
		                file.Block.InheritReaders = mode.Contains("r");
		                file.Block.InheritWriters = mode.Contains("w");
		                return Update(file);
		            }
		            if (name == "beyond.creategroup")
		            {
		                var ga = System.Text.Encoding.UTF8.GetString(value);
		                var bak = new BlockAndKey();
		                bak.Block = new Block();
		                crypto.SealMutable(bak, BlockChange.Data | BlockChange.GroupRemove, null).Wait();
		                var res = client.Insert(bak);
		                if (res.Code != Error.Types.ErrorCode.Ok)
		                {
		                    logger.LogWarning("addgroup failed with code {code}", res.Code);
		                    return Errno.EIO;
		                }
		                //alias it
		                if (file.Block.Data.Aliases == null)
		                    file.Block.Data.Aliases = new KeyAliasList();
		                file.Block.Data.Aliases.KeyAliases.Add(new KeyAlias
		                   {
		                       Alias = ga,
		                       KeyHash = bak.Key,
		                   });
		                file.Block.Version += 1;
		                var ka = crypto.ExtractKey(file).Result;
		                if (ka == null)
		                    return Errno.EPERM;
		                crypto.SealMutable(file, BlockChange.Data, ka).Wait();
		                res = client.TransactionalUpdate(file);
		                if (res.Code != Error.Types.ErrorCode.Ok)
		                {
		                    logger.LogWarning("add group alias failed with code {code}", res.Code);
		                    return Errno.EIO;
		                }
		                return 0;
		            }
		        }
		        catch(Exception e)
		        {
		            logger.LogWarning(e, "Exception in setxattr");
		        }
		    }
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnGetPathExtendedAttribute (string path, string name, byte[] value, out int bytesWritten)
		{
		    bytesWritten = 0;
		    logger.LogInformation("XATTR {name} {len}", name, value.Length);
		    if (name.StartsWith("beyond."))
		    {
		        try {
		            BlockAndKey file;
		            var err = Get(path, out file);
		            if (err != 0)
		                return err;
		            string result = null;
		            if (name == "beyond.address")
		                result = Utils.KeyString(file.Key);
		            else if (name == "beyond.owners")
		                result = String.Join('\n', file.Block.Owners.Owners.Select(x=>Utils.KeyString(x)));
		            else if (name == "beyond.dump")
		                result = file.ToString();
		            else if (name == "beyond.info")
		            {
		                var root = GetRoot();
		                Func<Key, string> aliasOrKey = k =>
		                {
		                    if (root.Block.Data.Aliases == null)
		                        return Utils.KeyString(k);
		                    var hit = root.Block.Data.Aliases.KeyAliases.Where(x=>x.KeyHash.Equals(k)).FirstOrDefault();
		                    if (hit == null)
		                        return Utils.KeyString(k);
		                    else
		                        return hit.Alias;
		                };
		                var kown = aliasOrKey(file.Block.Owner);
		                var kr = file.Block.Readers.EncryptionKeys.Select(r=>aliasOrKey(r.Recipient));
		                var kw = file.Block.Writers?.KeyHashes.Select(x=>aliasOrKey(x)).ToList() ?? new List<string>();
		                result = "address " + Utils.KeyString(file.Key) + "\n"
		                    + "readers " + String.Join(" ", kr) + "\n"
		                    + "writers " + String.Join(" ", kw) + "\n"
		                    + "inheritRead " + file.Block.InheritReaders + "\n"
		                    + "inheritWrite " + file.Block.InheritWriters + "\n";
		            }
		            var rb = System.Text.Encoding.UTF8.GetBytes(result);
		            var l = Math.Min(value.Length, rb.Length);
		            Array.Copy(rb, value, l);
		            bytesWritten = l;
		            return 0;
		        }
		        catch (Exception e)
		        {
		            logger.LogError(e, "bronk xattr");
		        }
		    }
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnListPathExtendedAttributes (string path, out string[] names)
		{
		    names = new string[] { "beyond.address", "beyond.owners", "beyond.dump"};
		    return 0;
		}
		protected override Errno OnRemovePathExtendedAttribute (string path, string name)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnLockHandle (string file, OpenedPathInfo info, FcntlCommand cmd, ref Flock @lock)
		{
		    return 0; // yes, it's locked, no worries uwu
		}
		public void Run(string mountPoint, string[] fuseArgs)
		{
		    base.MountPoint = mountPoint;
		    ParseFuseArguments(fuseArgs);
		    Start();
		}
    }
}