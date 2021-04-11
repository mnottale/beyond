using System;
using System.Collections.Generic;

using Microsoft.Extensions.Logging;

using Mono.Fuse.NETStandard;
using Mono.Unix.Native;

namespace Beyond
{
    class FileSystem: Mono.Fuse.NETStandard.FileSystem
    {
        private BeyondService.BeyondServiceClient client;
        private ILogger logger;

        public FileSystem(BeyondService.BeyondServiceClient client)
        {
            logger = Logger.loggerFactory.CreateLogger<BeyondServiceImpl>();
            this.client = client;
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
		            logger.LogInformation("get failed");
		            return err;
		        }
		        buf.st_mode = ((block.Block.Directory != null) ? FilePermissions.S_IFDIR : FilePermissions.S_IFREG) | NativeConvert.FromOctalPermissionString("0777");
		        buf.st_nlink = 1;
		        buf.st_size = 1000;
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
		    while (true)
		    {
		        parent.Block.Directory.Entries.Add(dirent);
		        parent.Block.Version = parent.Block.Version + 1;
		        var res = client.TransactionalUpdate(parent);
		        if (res.Code == Error.Types.ErrorCode.Ok)
		            break;
		        if (res.Code != Error.Types.ErrorCode.AlreadyLocked
		            && res.Code != Error.Types.ErrorCode.Conflict
		        && res.Code !=  Error.Types.ErrorCode.Outdated)
		          return Errno.EIO;
		        // try again
		        err = Get(path, out parent, parent:true);
		        if (err != 0)
		            return err;
		    }
		    return 0;
		}
		protected override Errno OnRemoveFile (string path)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnRemoveDirectory (string path)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnCreateSymbolicLink (string from, string to)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnRenamePath (string from, string to)
		{
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnCreateHardLink (string from, string to)
		{
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
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnChangePathTimes (string path, ref Utimbuf buf)
		{
		    return 0;
		}
		protected override Errno OnOpenHandle (string path, OpenedPathInfo info)
		{
		     return Errno.EOPNOTSUPP;
		}
		protected override Errno OnReadHandle (string path, OpenedPathInfo info, byte[] buf, 
				long offset, out int bytesRead)
		{
		    bytesRead = 0;
		    return Errno.EOPNOTSUPP;
		}
		protected override Errno OnWriteHandle (string path, OpenedPathInfo info,
				byte[] buf, long offset, out int bytesWritten)
		{
		    bytesWritten = 0;
		    return Errno.EOPNOTSUPP;
		}
		/*protected override Errno OnGetFileSystemStatus (string path, out Statvfs stbuf)
		{
		    stbuf = new Statvfs();
		    return Errno.EOPNOTSUPP;
		}*/
		protected override Errno OnReleaseHandle (string path, OpenedPathInfo info)
		{
		     return Errno.EOPNOTSUPP;
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