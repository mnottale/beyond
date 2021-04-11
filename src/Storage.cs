using System;
using System.IO;

namespace Beyond
{
    public class Storage
    {
        private string root;
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
            File.WriteAllBytes(path, value);
        }
        public byte[] Get(Key key)
        {
            var path = PathOf(key);
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