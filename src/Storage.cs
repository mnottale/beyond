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
            var skey = ByteArrayToHexViaLookup32(key.Key_.ToByteArray());
            return root + "/" + skey[0]+skey[1]+"/" + skey;
        }
        public int VersionOf(Key key)
        {
            return 0;
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
        private static readonly uint[] _lookup32 = CreateLookup32();
        
        private static uint[] CreateLookup32()
        {
            var result = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                string s=i.ToString("X2");
                result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
            }
            return result;
        }
        
        private static string ByteArrayToHexViaLookup32(byte[] bytes)
        {
            var lookup32 = _lookup32;
            var result = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var val = lookup32[bytes[i]];
                result[2*i] = (char)val;
                result[2*i + 1] = (char) (val >> 16);
            }
            return new string(result);
        }
    }
}