using System;
using System.Security.Cryptography;
using System.Collections.Generic;

namespace Beyond
{
    public static class Utils
    {
        private static Random rnd = new Random();
        public static List<T> PickN<T>(List<T> candidates, int n)
        {
            if (candidates.Count <= n)
                return candidates;
            // FIXME: more efficient implementation
            var idxs = new List<int>();
            for (int i = 0; i<n; i++)
            {
                int choice = rnd.Next(candidates.Count);
                while (idxs.Contains(choice))
                    choice = rnd.Next(candidates.Count);
                idxs.Add(choice);
            }
            var res = new List<T>();
            foreach (var idx in idxs)
                res.Add(candidates[idx]);
            return res;
        }
        public static void Shuffle<T>(this IList<T> list)
        {
            for(var i=list.Count; i > 0; i--)
                list.Swap(0, rnd.Next(0, i));
        }

        public static void Swap<T>(this IList<T> list, int i, int j)
        {
            var temp = list[i];
            list[i] = list[j];
            list[j] = temp;
        }
        public static Error ErrorFromCode(Error.Types.ErrorCode erc, long cv=0)
        {
            var res = new Error();
            res.Code = erc;
            res.CurrentVersion = cv;
            return res;
        }
        public static Key RandomKey()
        {
            var bytes = new byte[32];
            for (var i=0; i<32; i++)
                bytes[i] = (byte)rnd.Next();
            var res = new Key();
            res.Key_ = Google.Protobuf.ByteString.CopyFrom(bytes);
            return res;
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
        public static Key StringKey(string s)
        {
            var bytes = new byte[s.Length / 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Convert.ToByte(s.Substring(i * 2, 2), 16);
            }
            var res = new Key();
            res.Key_ = Google.Protobuf.ByteString.CopyFrom(bytes);
            return res;
        }
        public static byte[] HalfKey(byte[] data)
        {
            var res = new byte[data.Length / 2];
            Array.Copy(data, res, res.Length);
            return res;
        }
        public static Key Checksum(byte[] dataa, byte[] datab = null)
        {
            using var sha = SHA256.Create();
            byte[] bytes = null;
            if (datab == null)
                bytes = SHA256.HashData(dataa);
            else
            {//FIXME this sucks
                var d = new byte[dataa.Length + datab.Length];
                Array.Copy(dataa, d, dataa.Length);
                Array.Copy(datab, 0, d, dataa.Length, datab.Length);
                bytes = SHA256.HashData(d);
                //chatGPT answer, totally wrong, grmbl
//               sha.TransformBlock(dataa, 0, dataa.Length, null, 0);
//               bytes = sha.TransformFinalBlock(datab, 0, datab.Length);
            }
            var res = new Key();
            res.Key_ = Google.Protobuf.ByteString.CopyFrom(bytes);
            return res;
        }
        public static string KeyString(Key k)
        {
            return ByteArrayToHexViaLookup32(k.Key_.ToByteArray());
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