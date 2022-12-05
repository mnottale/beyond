using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
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
            public ConcurrentDictionary<ulong, FileChunk> chunks = new ConcurrentDictionary<ulong, FileChunk>();
        };
        ConcurrentDictionary<string, OpenedHandle> openedFiles = new ConcurrentDictionary<string, OpenedHandle>();
        public FileSystem(BeyondClient.BeyondClientClient client)
        {
            logger = Logger.loggerFactory.CreateLogger<BeyondServiceImpl>();
            this.client = client;
            GetRoot(); // ping
        }
        public void MkFS()
        {
            logger.LogInformation("MAKING NEW FILESYSTEM");
            // FIXME: add a safety check
            var root = new BlockAndKey();
            root.Key = RootAddress();
            root.Block = new Block();
            root.Block.Version = 1;
            root.Block.Directory = new DirectoryIndex();
            client.Insert(root);
        }
        protected Key RootAddress()
        {
            var res = new Key();
            res.Key_ = Google.Protobuf.ByteString.CopyFrom(new byte[32]);
            return res;
        }
        protected BlockAndKey GetRoot()
        {
            BlockAndKey res = new BlockAndKey();
            res.Key = RootAddress();
            res.Block = client.Query(res.Key);
            return res;
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
                if (current?.Block?.Directory?.Entries == null)
                {
                    logger.LogInformation("null data at {index}", i);
                    return Errno.ENOENT;
                }
                foreach (var ent in current.Block.Directory.Entries)
                {
                    if (ent.Name == components[i])
                    {
                        var blk = client.Query(ent.Address);
                        current.Key = ent.Address;
                        current.Block = blk;
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
		    try
		    {
		        logger.LogInformation("STAT {path}", path);
		        BlockAndKey block;
		        var err = Get(path, out block);
		        if (err != 0)
		        {
		            logger.LogInformation("get failed, returning {errno}", err);
		            return err;
		        }
		        buf.st_mode = ((block.Block.Directory != null) ? FilePermissions.S_IFDIR : FilePermissions.S_IFREG) | NativeConvert.FromOctalPermissionString("0777");
		        buf.st_nlink = 1;
		        buf.st_size = (block.Block.File != null) ? (long)block.Block.File.Size : 1024;
		        buf.st_blksize = 65536;
		        buf.st_blocks = buf.st_size / 512;
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
		    return Errno.ENOENT;
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
		    foreach (var ent in block.Block.Directory.Entries)
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
		        var res = client.TransactionalUpdate(block);
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
		        logger.LogInformation("UpdateBlock is retrying");
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
		    bak.Key = Utils.RandomKey();
		    bak.Block = new Block();
		    bak.Block.Version = 1;
		    bak.Block.Directory = new DirectoryIndex();
		    client.Insert(bak);
		    var dirent = new DirectoryEntry();
		    dirent.EntryType = DirectoryEntry.Types.EntryType.Directory;
		    dirent.Name = last;
		    dirent.Address = bak.Key;
		    // then link it
		    UpdateBlock(path, true, parent, p => {
		            p.Block.Directory.Entries.Add(dirent);
		            p.Block.Version = p.Block.Version + 1;
		    });
		    return 0;
		}
		private Errno Unlink(string filePath, BlockAndKey dir, string name)
		{
		    return UpdateBlock(filePath, true, dir, p => {
		            var count = p.Block.Directory.Entries.Count;
		            for (var i = 0; i<count; ++i)
		            {
		                if (p.Block.Directory.Entries[i].Name == name)
		                {
		                    p.Block.Directory.Entries.RemoveAt(i);
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
		    err = Unlink(path, parent, fileName);
		    if (err != 0)
		        return err;
		    foreach (var b in file.Block.File.Blocks)
		    {
		        client.Delete(b.Address);
		    }
		    client.Delete(file.Key);
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
		    if (file.Block.Directory.Entries.Count != 0)
		        return Errno.ENOTEMPTY;
		    BlockAndKey parent;
		    err = Get(path, out parent, parent: true);
		    if (err != 0)
		        return err;
		    err = Unlink(path, parent, fileName);
		    if (err != 0)
		        return err;
		    client.Delete(file.Key);
		    return 0;
		}
		protected override Errno OnCreateSymbolicLink (string from, string to)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnRenamePath (string from, string to)
		{
		    logger.LogInformation("MV {from} {to}", from, to);
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnCreateHardLink (string from, string to)
		{
		    logger.LogInformation("HARDLNK {from} {to}", from, to);
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnChangePathPermissions (string path, FilePermissions mode)
		{
		    return 0;
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
			return OpenOrCreate(file, true);
		}
		protected override Errno OnOpenHandle (string path, OpenedPathInfo info)
		{
		    return OpenOrCreate(path, false);
		}
		private Errno OpenOrCreate(string path, bool allowCreation)
		{
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
		            client.Insert(file);
		            var comps = path.Split('/');
		            var fileName = comps[comps.Length-1];
		            var dirent = new DirectoryEntry();
		            dirent.EntryType = DirectoryEntry.Types.EntryType.File;
		            dirent.Name = fileName;
		            dirent.Address = file.Key;
		            err = UpdateBlock(path, true, parent, b => {
		                    b.Block.Version = b.Block.Version + 1;
		                    b.Block.Directory.Entries.Add(dirent);
		            });
		        }
		        if (err != 0)
		        {
		            logger.LogInformation("failed to write block: {err}", err);
		            return err;
		        }
		        if (file.Block.File == null)
		            file.Block.File = new FileIndex();
		        openedFiles.TryAdd(path, new OpenedHandle
		            {
		                openCount = 1,
		                fileBlock = file,
		            });
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
		        if (chunkIndex >= (ulong)oh.fileBlock.Block.File.Blocks.Count)
		            return 0;
		        var block = client.Query(oh.fileBlock.Block.File.Blocks[(int)chunkIndex].Address);
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
		        if (chunkIndex >= (ulong)oh.fileBlock.Block.File.Blocks.Count)
		        {
		            for (int ci = oh.fileBlock.Block.File.Blocks.Count; ci <= (int)chunkIndex; ++ci)
		            {
		                BlockAndKey bak = new BlockAndKey();
		                bak.Key = Utils.RandomKey();
		                bak.Block = new Block();
		                bak.Block.Version = 1;
		                client.Insert(bak);
		                var fb = new FileBlock();
		                fb.Address = bak.Key;
		                oh.fileBlock.Block.File.Blocks.Add(fb);
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
		            var bl = client.Query(oh.fileBlock.Block.File.Blocks[(int)chunkIndex].Address);
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
		        if (oh.fileBlock.Block.File.Size < newSz)
		        {
		            oh.fileBlock.Block.File.Size = newSz;
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
		    while (true)
		    {
		        var res = client.TransactionalUpdate(oh.fileBlock);
		        if (res.Code == Error.Types.ErrorCode.Ok)
		            break;
		        if (res.Code != Error.Types.ErrorCode.AlreadyLocked
		            && res.Code != Error.Types.ErrorCode.Conflict
		        && res.Code !=  Error.Types.ErrorCode.Outdated)
		          return Errno.EIO;
		        logger.LogInformation("Flush retry from {version}", oh.fileBlock.Block.Version);
		        var current = client.Query(oh.fileBlock.Key);
		        oh.fileBlock.Block.Version = current.Version + 1;
		    }
		    return 0;
		}
		private Errno FlushChunk(OpenedHandle oh, int chunkIndex)
		{
		    if (!oh.chunks.TryGetValue((ulong)chunkIndex, out var chunk))
		        return 0;
		    var bak = new BlockAndKey();
		    bak.Key = oh.fileBlock.Block.File.Blocks[chunkIndex].Address;
		    bak.Block = new Block();
		    bak.Block.Version = (long)(chunk.version+1);
		    bak.Block.Raw = Google.Protobuf.ByteString.CopyFrom(chunk.data);
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
		protected override Errno OnSetPathExtendedAttribute (string path, string name, byte[] value, XattrFlags flags)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnGetPathExtendedAttribute (string path, string name, byte[] value, out int bytesWritten)
		{
		    bytesWritten = 0;
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnListPathExtendedAttributes (string path, out string[] names)
		{
		    names = null;
		    return Errno.EOPNOTSUPP;
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