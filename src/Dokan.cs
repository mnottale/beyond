using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using Microsoft.Extensions.Logging;
using DokanNet;
using static DokanNet.FormatProviders;
using FileAccess = DokanNet.FileAccess;
using Mono.Fuse.NETStandard;
using Mono.Unix.Native;
namespace Beyond
{
    internal class DokanFS : FileSystem, IDokanOperations
    {

        private const FileAccess DataAccess = FileAccess.ReadData | FileAccess.WriteData | FileAccess.AppendData |
                                              FileAccess.Execute |
                                              FileAccess.GenericExecute | FileAccess.GenericWrite |
                                              FileAccess.GenericRead;

        private const FileAccess DataWriteAccess = FileAccess.WriteData | FileAccess.AppendData |
                                                   FileAccess.Delete |
                                                   FileAccess.GenericWrite;

        private readonly ILogger _logger;
        private readonly DokanFS _backend;

        public DokanFS(ILoggerFactory loggerFactory, BeyondClient.BeyondClientClient client, string fsName = null, uint uid=0, uint gid=0, Crypto c = null, ulong immutableCacheSize = 0, ulong mutableCacheDuration = 0)
        :base (loggerFactory, client, fsName, uid, gid, c, immutableCacheSize, mutableCacheDuration)
        {
            _logger = loggerFactory.CreateLogger<DokanFS>();
            _backend = this;
        }

        protected NtStatus Trace(string method, string fileName, IDokanFileInfo info, NtStatus result,
            params object[] parameters)
        {
#if TRACE
            var extraParameters = parameters != null && parameters.Length > 0
                ? ", " + string.Join(", ", parameters.Select(x => string.Format(DefaultFormatProvider, "{0}", x)))
                : string.Empty;

            _logger.LogDebug(DokanFormat($"{method}('{fileName}', {info}{extraParameters}) -> {result}"));
#endif

            return result;
        }

        private NtStatus Trace(string method, string fileName, IDokanFileInfo info,
            FileAccess access, FileShare share, FileMode mode, FileOptions options, FileAttributes attributes,
            NtStatus result)
        {
#if TRACE
            _logger.LogDebug(
                DokanFormat(
                    $"{method}('{fileName}', {info}, [{access}], [{share}], [{mode}], [{options}], [{attributes}]) -> {result}"));
#endif

            return result;
        }

        protected static Int32 GetNumOfBytesToCopy(Int32 bufferLength, long offset, IDokanFileInfo info, FileStream stream)
        {
            if (info.PagingIo)
            {
                var longDistanceToEnd = stream.Length - offset;
                var isDistanceToEndMoreThanInt = longDistanceToEnd > Int32.MaxValue;
                if (isDistanceToEndMoreThanInt) return bufferLength;
                var distanceToEnd = (Int32)longDistanceToEnd;
                if (distanceToEnd < bufferLength) return distanceToEnd;
                return bufferLength;
            }
            return bufferLength;
        }

        #region Implementation of IDokanOperations

        public NtStatus CreateFile(string fileName, FileAccess access, FileShare share, FileMode mode,
            FileOptions options, FileAttributes attributes, IDokanFileInfo info)
        {
            fileName = fileName.Replace("\\", "/");
            var result = DokanResult.Success;
            Errno err = 0;
            if (info.IsDirectory)
            {
                try
                {
                    switch (mode)
                    {
                        case FileMode.Open:
                            err = _backend.OnGetPathStatus(fileName, out var stat);
                            if (err != 0)
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                            attributes, DokanResult.NotADirectory); // FIXME error code
                            if ((stat.st_mode & FilePermissions.S_IFDIR) == 0)
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                            attributes, DokanResult.NotADirectory);
                            break;

                        case FileMode.CreateNew:
                            err = _backend.OnCreateDirectory(fileName, NativeConvert.FromOctalPermissionString("777"));
                            if (err != 0)
                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                            attributes, DokanResult.AccessDenied);
                            break;
                    }
                }
                catch (UnauthorizedAccessException)
                { //FIXME dead
                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                        DokanResult.AccessDenied);
                }
            }
            else
            {
                var pathExists = true;
                var pathIsDirectory = false;
                var hasAccess = false;
                err = _backend.OnGetPathStatus(fileName, out var stat);
                hasAccess = (err == 0);
                pathIsDirectory = err == 0 && ((stat.st_mode & FilePermissions.S_IFDIR) != 0);
                pathExists = err != Errno.ENOENT;

                var readWriteAttributes = (access & DataAccess) == 0;
                var readAccess = (access & DataWriteAccess) == 0;

                switch (mode)
                {
                    case FileMode.Open:

                        if (pathExists)
                        {
                            // check if driver only wants to read attributes, security info, or open directory
                            if (readWriteAttributes || pathIsDirectory)
                            {
                                if (pathIsDirectory && (access & FileAccess.Delete) == FileAccess.Delete
                                    && (access & FileAccess.Synchronize) != FileAccess.Synchronize)
                                    //It is a DeleteFile request on a directory
                                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                        attributes, DokanResult.AccessDenied);

                                info.IsDirectory = pathIsDirectory;
                                info.Context = new object();
                                // must set it to something if you return DokanError.Success

                                return Trace(nameof(CreateFile), fileName, info, access, share, mode, options,
                                    attributes, DokanResult.Success);
                            }
                        }
                        else
                        {
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.FileNotFound);
                        }
                        break;

                    case FileMode.CreateNew:
                        if (pathExists)
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.FileExists);
                        break;

                    case FileMode.Truncate:
                        if (!pathExists)
                            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                                DokanResult.FileNotFound);
                        break;
                }
                // FIXME truncate
                err = _backend.OpenOrCreate(fileName,
                    mode == FileMode.CreateNew,
                    null,
                    NativeConvert.FromOctalPermissionString("777"),
                    readAccess ? OpenFlags.O_RDONLY : OpenFlags.O_RDWR);
                _logger.LogWarning("OPEN " + fileName + " "  + err.ToString());
                if (err != 0)
                    return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                        DokanResult.AccessDenied);
               info.Context = "handle";
            }
            return Trace(nameof(CreateFile), fileName, info, access, share, mode, options, attributes,
                result);
        }

        public void Cleanup(string fileName, IDokanFileInfo info)
        {
            fileName = fileName.Replace("\\", "/");
#if TRACE
            if (info.Context != null)
                Console.WriteLine(DokanFormat($"{nameof(Cleanup)}('{fileName}', {info} - entering"));
#endif
            if (info.Context != null && info.Context as string == "handle")
                _backend.OnReleaseHandle(fileName, null);
            info.Context = null;

            if (info.DeleteOnClose)
            {
                if (info.IsDirectory)
                {
                    _backend.OnRemoveDirectory(fileName);
                }
                else
                {
                    _backend.OnRemoveFile(fileName);
                }
            }
            Trace(nameof(Cleanup), fileName, info, DokanResult.Success);
        }

        public void CloseFile(string fileName, IDokanFileInfo info)
        {
            fileName = fileName.Replace("\\", "/");
#if TRACE
            if (info.Context != null)
                Console.WriteLine(DokanFormat($"{nameof(CloseFile)}('{fileName}', {info} - entering"));
#endif
            /*if (info.Context != null)
                _backend.OnReleaseHandle(fileName, null);
            info.Context = null;*/
            Trace(nameof(CloseFile), fileName, info, DokanResult.Success);
            // could recreate cleanup code here but this is not called sometimes
        }

        public NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, IDokanFileInfo info)
        {
            fileName = fileName.Replace("\\", "/");
            var err = _backend.OnReadHandle(fileName, null, buffer, offset, out bytesRead);
            if (err == Errno.EBADF)
            { // ooookay, according to dokan, this is expected, see for instance
              // https://github.com/dokan-dev/dokany/issues/1016
              // Todo: implement shadow handles to alieviate the cost
              var openerr = _backend.OpenOrCreate(fileName, false, null, null, OpenFlags.O_RDONLY);
              if (openerr != 0)
                   return Trace(nameof(ReadFile), fileName, info, DokanResult.InternalError);
              err = _backend.OnReadHandle(fileName, null, buffer, offset, out bytesRead);
              _backend.OnReleaseHandle(fileName, null);
            }
            if (err != 0)
            {
                _logger.LogWarning("READ ERROR " + fileName + " "  + err.ToString() + " " + offset.ToString());
                return Trace(nameof(ReadFile), fileName, info, DokanResult.InternalError);
            }
            return Trace(nameof(ReadFile), fileName, info, DokanResult.Success, "out " + bytesRead.ToString(),
                offset.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset, IDokanFileInfo info)
        {
            fileName = fileName.Replace("\\", "/");
            var err = _backend.OnWriteHandle(fileName, null, buffer, offset, out bytesWritten);
            if (err == Errno.EBADF)
            { // ooookay, according to dokan, this is expected, see for instance
              // https://github.com/dokan-dev/dokany/issues/1016
              // Todo: implement shadow handles to alieviate the cost
              var openerr = _backend.OpenOrCreate(fileName, false, null, null, OpenFlags.O_RDWR);
              if (openerr != 0)
                   return Trace(nameof(WriteFile), fileName, info, DokanResult.InternalError);
              err = _backend.OnWriteHandle(fileName, null, buffer, offset, out bytesWritten);
              _backend.OnReleaseHandle(fileName, null);
            }
            if (err != 0)
                return Trace(nameof(WriteFile), fileName, info, DokanResult.InternalError);
            return Trace(nameof(WriteFile), fileName, info, DokanResult.Success, "out " + bytesWritten.ToString(),
                offset.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus FlushFileBuffers(string fileName, IDokanFileInfo info)
        {
            fileName = fileName.Replace("\\", "/");
            _backend.OnSynchronizeHandle(fileName, null, false);
            return Trace(nameof(FlushFileBuffers), fileName, info, DokanResult.Success);
        }

        public NtStatus GetFileInformation(string fileName, out FileInformation fileInfo, IDokanFileInfo info)
        {
            var err = _backend.OnGetPathStatus(fileName.Replace("\\", "/"), out var stat);
            if (err != 0) // FIXME error code
            {
                fileInfo = new FileInformation
                {
                    FileName = fileName,
                    Attributes = FileAttributes.Offline,
                };
                return Trace(nameof(GetFileInformation), fileName, info, DokanResult.AccessDenied);
            }
            // may be called with info.Context == null, but usually it isn't
            FileAttributes attrs = 0;
            if ((stat.st_mode & FilePermissions.S_IFDIR) != 0)
                attrs |= FileAttributes.Directory;
            if ((stat.st_mode & FilePermissions.S_IWUSR) == 0)
                attrs |= FileAttributes.ReadOnly;
            if (attrs == 0)
                attrs = FileAttributes.Normal;
            fileInfo = new FileInformation
            {
                FileName = fileName,
                Attributes = attrs,
                CreationTime = DateTimeOffset.FromUnixTimeSeconds(stat.st_ctime).DateTime,
                LastAccessTime = new DateTime(),
                LastWriteTime = DateTimeOffset.FromUnixTimeSeconds(stat.st_mtime).DateTime,
                Length = stat.st_size,
            };
            return Trace(nameof(GetFileInformation), fileName, info, DokanResult.Success);
        }

        private string GetFileName(string input)
        {
            input = input.Replace("\\", "/");
            var comps = input.Split('/');
            return comps[comps.Length-1];
        }
        public NtStatus FindFiles(string fileName, out IList<FileInformation> files, IDokanFileInfo info)
        {
            _logger.LogInformation("*** FF " + fileName);
            var err = _backend.OnReadDirectory(fileName.Replace("\\", "/"), null, out var entries);
            if (err != 0)
            {
                files = null;
                return Trace(nameof(FindFiles), fileName, info, DokanResult.AccessDenied);
            }
            files = new List<FileInformation>();
            foreach (var fi in entries)
            {
                GetFileInformation(fileName.Replace("\\", "/") + "/" + fi.Name, out var finfo, null);
                _logger.LogInformation("Fillling FI from " + finfo.FileName);
                finfo.FileName = GetFileName(finfo.FileName);//fileName + ((fileName ==  "\\") ? "" : "\\") + GetFileName(finfo.FileName);
                _logger.LogInformation("Fillling FI with " + finfo.FileName);
                files.Add(finfo);
            }
            // This function is not called because FindFilesWithPattern is implemented
            // Return DokanResult.NotImplemented in FindFilesWithPattern to make FindFiles called
            return Trace(nameof(FindFiles), fileName, info, DokanResult.Success);
        }

        public NtStatus SetFileAttributes(string fileName, FileAttributes attributes, IDokanFileInfo info)
        {
            return Trace(nameof(SetFileAttributes), fileName, info, DokanResult.Success, attributes.ToString());
        }

        public NtStatus SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime,
            DateTime? lastWriteTime, IDokanFileInfo info)
        {
            return Trace(nameof(SetFileTime), fileName, info, DokanResult.Success, creationTime, lastAccessTime,
                    lastWriteTime);
        }

        public NtStatus DeleteFile(string fileName, IDokanFileInfo info)
        {
            var err = _backend.OnRemoveFile(fileName.Replace("\\", "/"));
            if (err != 0)
                return Trace(nameof(DeleteFile), fileName, info, DokanResult.AccessDenied);
            return Trace(nameof(DeleteFile), fileName, info, DokanResult.Success);
            // we just check here if we could delete the file - the true deletion is in Cleanup
        }

        public NtStatus DeleteDirectory(string fileName, IDokanFileInfo info)
        {
            var err = _backend.OnRemoveDirectory(fileName.Replace("\\", "/"));
            if (err != 0)
                return Trace(nameof(DeleteDirectory), fileName, info, DokanResult.AccessDenied);
            return Trace(nameof(DeleteDirectory), fileName, info, DokanResult.Success);

            // if dir is not empty it can't be deleted
        }

        public NtStatus MoveFile(string oldName, string newName, bool replace, IDokanFileInfo info)
        {
            var err = _backend.OnRenamePath(oldName.Replace("\\", "/"), newName.Replace("\\", "/"), replace);
            if (err != 0)
                return Trace(nameof(MoveFile), oldName, info, (err == Errno.EEXIST) ? DokanResult.FileExists : DokanResult.AccessDenied);
            return Trace(nameof(MoveFile), oldName, info, DokanResult.Success, newName,
                replace.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus SetEndOfFile(string fileName, long length, IDokanFileInfo info)
        {
            var err = _backend.OnTruncateFile(fileName.Replace("\\", "/"), length);
            if (err != 0)
                return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.AccessDenied);
            return Trace(nameof(SetEndOfFile), fileName, info, DokanResult.Success);
        }

        public NtStatus SetAllocationSize(string fileName, long length, IDokanFileInfo info)
        {
            //FIXME should we truncate
            return Trace(nameof(SetAllocationSize), fileName, info, DokanResult.Success,
                    length.ToString(CultureInfo.InvariantCulture));
        }

        public NtStatus LockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {

            return DokanResult.NotImplemented;
        }

        public NtStatus UnlockFile(string fileName, long offset, long length, IDokanFileInfo info)
        {
            return DokanResult.NotImplemented;
        }

        public NtStatus GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes, out long totalNumberOfFreeBytes, IDokanFileInfo info)
        {
            freeBytesAvailable = 10000000000;
            totalNumberOfBytes = 10000000000;
            totalNumberOfFreeBytes = 10000000000;
            return DokanResult.Success;
        }

        public NtStatus GetVolumeInformation(out string volumeLabel, out FileSystemFeatures features,
            out string fileSystemName, out uint maximumComponentLength, IDokanFileInfo info)
        {
            volumeLabel = "DOKAN";
            fileSystemName = "NTFS";
            maximumComponentLength = 256;

            features = FileSystemFeatures.CasePreservedNames | FileSystemFeatures.CaseSensitiveSearch |
                       FileSystemFeatures.PersistentAcls | FileSystemFeatures.SupportsRemoteStorage |
                       FileSystemFeatures.UnicodeOnDisk;

            return Trace(nameof(GetVolumeInformation), null, info, DokanResult.Success, "out " + volumeLabel,
                "out " + features.ToString(), "out " + fileSystemName);
        }

        public NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            security = null;
            return DokanResult.NotImplemented;
            /*
            try
            {
#if NET5_0_OR_GREATER
                security = info.IsDirectory
                    ? (FileSystemSecurity)new DirectoryInfo(GetPath(fileName)).GetAccessControl()
                    : new FileInfo(GetPath(fileName)).GetAccessControl();
#else
                security = info.IsDirectory
                    ? (FileSystemSecurity)Directory.GetAccessControl(GetPath(fileName))
                    : File.GetAccessControl(GetPath(fileName));
#endif
                return Trace(nameof(GetFileSecurity), fileName, info, DokanResult.Success, sections.ToString());
            }
            catch (UnauthorizedAccessException)
            {
                security = null;
                return Trace(nameof(GetFileSecurity), fileName, info, DokanResult.AccessDenied, sections.ToString());
            }
            */
        }

        public NtStatus SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections,
            IDokanFileInfo info)
        {
            return DokanResult.NotImplemented;
            /*
            try
            {
#if NET5_0_OR_GREATER
                if (info.IsDirectory)
                {
                    new DirectoryInfo(GetPath(fileName)).SetAccessControl((DirectorySecurity)security);
                }
                else
                {
                    new FileInfo(GetPath(fileName)).SetAccessControl((FileSecurity)security);
                }
#else
                if (info.IsDirectory)
                {
                    Directory.SetAccessControl(GetPath(fileName), (DirectorySecurity)security);
                }
                else
                {
                    File.SetAccessControl(GetPath(fileName), (FileSecurity)security);
                }
#endif
                return Trace(nameof(SetFileSecurity), fileName, info, DokanResult.Success, sections.ToString());
            }
            catch (UnauthorizedAccessException)
            {
                return Trace(nameof(SetFileSecurity), fileName, info, DokanResult.AccessDenied, sections.ToString());
            }
            */
        }

        public NtStatus Mounted(string mountPoint, IDokanFileInfo info)
        {
            return Trace(nameof(Mounted), null, info, DokanResult.Success);
        }

        public NtStatus Unmounted(IDokanFileInfo info)
        {
            return Trace(nameof(Unmounted), null, info, DokanResult.Success);
        }

        public NtStatus FindStreams(string fileName, IntPtr enumContext, out string streamName, out long streamSize,
            IDokanFileInfo info)
        {
            streamName = string.Empty;
            streamSize = 0;
            return Trace(nameof(FindStreams), fileName, info, DokanResult.NotImplemented, enumContext.ToString(),
                "out " + streamName, "out " + streamSize.ToString());
        }

        public NtStatus FindStreams(string fileName, out IList<FileInformation> streams, IDokanFileInfo info)
        {
            streams = new FileInformation[0];
            return Trace(nameof(FindStreams), fileName, info, DokanResult.NotImplemented);
        }

        public NtStatus FindFilesWithPattern(string fileName, string searchPattern, out IList<FileInformation> files,
            IDokanFileInfo info)
        {
            files = null;
            return Trace(nameof(FindFilesWithPattern), fileName, info, DokanResult.NotImplemented);
        }
        #endregion Implementation of IDokanOperations
    }
}
