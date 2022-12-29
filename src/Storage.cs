using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;

namespace Beyond
{
    public class Storage
    {
        private class SSLock: IDisposable
        {
            private readonly SemaphoreSlim _sem;
            public SSLock(SemaphoreSlim sem)
            {
                _sem = sem;
                sem.Wait();
            }
            public void Dispose()
            {
                _sem.Release();
            }
        }
        private string root;
        private SemaphoreSlim sem = new SemaphoreSlim(1, 1);
        public Storage(string root)
        {
            this.root = root;
            Directory.CreateDirectory(root);
            for (int i = 0; i < 256; i++)
            {
                string s=i.ToString("X2");
                Directory.CreateDirectory(root + "/" + s);
            }
        }
        protected string PathOf(Key key)
        {
            var skey = Utils.KeyString(key);
            return root + "/" + skey[0]+skey[1]+"/" + skey;
        }
        public List<Key> List()
        {
            var res = new List<Key>();
            for (int i = 0; i < 256; i++)
            {
                string s=i.ToString("X2");
                var dir = root + "/" + s;
                foreach (var f in Directory.GetFiles(dir))
                {
                    try
                    {
                        var fn = Path.GetFileName(f);
                        res.Add(Utils.StringKey(fn));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"BRONK {f} {e}");
                    }
                }
            }
            return res;
        }
        public long VersionOf(Key key)
        {
            var raw = Get(key);
            using var ms = new MemoryStream(raw);
            var block = Block.Parser.ParseFrom(ms);
            return block.Version;
        }
        public void Put(Key key, byte[] value)
        {
            var path = PathOf(key);
            using var lk = new SSLock(sem);
            File.WriteAllBytes(path, value);
        }
        public byte[] Get(Key key)
        {
            var path = PathOf(key);
            using var lk = new SSLock(sem);
            return File.ReadAllBytes(path);
        }
        public bool Has(Key key)
        {
            var path = PathOf(key);
            return File.Exists(path);
        }
        public void Delete(Key key)
        {
            var path = PathOf(key);
            File.Delete(path);
        }
    }
}