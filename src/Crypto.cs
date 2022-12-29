using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Google.Protobuf;

using Mono.Unix.Native;
namespace Beyond;

[Flags]
public enum BlockChange
{
    Data = 1,
    Writers = 2,
    Readers = 4,
    All = 7,
    GroupRemove = 8,
}
public class Crypto
{
    private AsymmetricAlgorithm _owner;
    private Key _ownerSig;
    private List<Key> _admins = new();
    private Dictionary<Key, AsymmetricAlgorithm> _keys = new();
    private ILogger _logger;

    public Func<Key, Task<Block>> GetKeyBlock;
    public Func<Key, Task<Block>> GetPreviousBlock;
    public Crypto(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<Crypto>();
    }
    public void SetAdmins(IEnumerable<Key> adms)
    {
        _admins.Clear();
        _admins.AddRange(adms);
    }
    public void SetOwner(byte[] encryptedKey, string passphrase)
    {
        var aa = new RSACryptoServiceProvider(512);
        aa.ImportEncryptedPkcs8PrivateKey(passphrase, encryptedKey, out int br);
        _owner = aa;
        var pub = aa.ExportRSAPublicKey();
        _ownerSig = Utils.Checksum(pub);
    }
    public static (byte[], string) MakeAsymmetricKey(string passphrase)
    {
        var k = new RSACryptoServiceProvider(2048);
        return (k.ExportEncryptedPkcs8PrivateKey(passphrase,
            new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc,
                HashAlgorithmName.SHA256,
                1000)), Utils.KeyString(Utils.Checksum(k.ExportRSAPublicKey()))); // no idea what I'm doing, this is dog
    }
    public BlockAndKey ExportOwnerPublicKey()
    {
        var bak = new BlockAndKey();
        var pub = (_owner as RSA).ExportRSAPublicKey();
        bak.Key = _ownerSig;
        bak.Block = new Block();
        bak.Block.Raw = Google.Protobuf.ByteString.CopyFrom(pub);
        _logger.LogInformation("Key export {addr} {block}", Utils.KeyString(bak.Key), bak.Block);
        return bak;
    }
    public async Task<(AsymmetricAlgorithm, long)> GetKey(Key sig)
    {
        if (_keys.TryGetValue(sig, out var res))
            return (res, 0);
        if (_ownerSig != null && _ownerSig.Equals(sig))
            return (_owner, 0);
        var blk = await GetKeyBlock(sig);
        byte[] data = null;
        long version = 0;
        if (blk.GroupVersion != 0)
        {
            data = blk.GroupPublicKey.ToByteArray();
            version = blk.GroupVersion;
        }
        else
            data = blk.Raw.ToByteArray();
        _logger.LogInformation("Key import {addr} {block}", Utils.KeyString(sig), blk);
        var aa = new RSACryptoServiceProvider(512);
        int br = 0;
        aa.ImportRSAPublicKey(data, out br);
        if (version == 0)
            _keys.Add(sig, aa);
        return (aa, version);
    }
    public byte[] Decrypt(byte[] input, AESKey key)
    {
        using (Aes aesAlg = Aes.Create())
        {
            aesAlg.Key = key.Key.ToByteArray();
            aesAlg.IV = Utils.HalfKey(key.Iv.ToByteArray());
            
            // Create a decryptor to perform the stream transform.
            ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);
            
            // Create the streams used for decryption.
            using (MemoryStream msDecrypt = new MemoryStream(input))
            {
                using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                {
                    using (var output = new MemoryStream())
                    {
                        csDecrypt.CopyTo(output);
                        return output.ToArray();
                    }
                }
            }
        }
    }
    public byte[] Encrypt(byte[] input, AESKey key)
    {
        Console.WriteLine($"IV is {key.Iv.ToByteArray().Length}");
        using (Aes aesAlg = Aes.Create())
        {
            aesAlg.Key = key.Key.ToByteArray();
            aesAlg.IV = Utils.HalfKey(key.Iv.ToByteArray());
            
            
            // Create an encryptor to perform the stream transform.
            ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);
            
            // Create the streams used for encryption.
            using (MemoryStream msEncrypt = new MemoryStream())
            {
                using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                {
                    using (var swEncrypt = new BinaryWriter(csEncrypt))
                    {
                        //Write all data to the stream.
                        swEncrypt.Write(input);
                    }
                    return msEncrypt.ToArray();
                }
            }
        }
    }
    public async Task SealImmutable(BlockAndKey bak, AESKey aes)
    {
        if (bak.Block.Salt == null || bak.Block.Salt.Length == 0)
        {
            bak.Block.Salt = Utils.RandomKey().Key_;
        }
        bak.Block.Signed = new SignedData();
        bak.Block.Signed.EncryptedData = Google.Protobuf.ByteString.CopyFrom(
                Encrypt(bak.Block.Raw.ToByteArray(), aes));
        var addr = Utils.Checksum(bak.Block.Salt.ToByteArray(), bak.Block.Signed.EncryptedData.ToByteArray());
        bak.Key = addr;
        bak.Block.Raw = Google.Protobuf.ByteString.Empty;
    }
    public async Task SealMutable(BlockAndKey bak, BlockChange bc, AESKey aes)
    {
        _logger.LogInformation("PRE-Sealed block: {block}", bak.ToString());
        if (aes == null)
        {
            aes = new AESKey();
            aes.Key = Utils.RandomKey().Key_;
            aes.Iv = Utils.RandomKey().Key_;
        }
        if (bak.Key == null)
        {
            if (bak.Block.Salt == null || bak.Block.Salt.Length == 0)
                bak.Block.Salt = Utils.RandomKey().Key_;
            if (bak.Block.Owner == null)
                bak.Block.Owner = _ownerSig;
            bak.Key = Utils.Checksum(bak.Block.Owner.ToByteArray(), bak.Block.Salt.ToByteArray());
        }
        else if (bak.Block.Owner == null)
            throw new System.Exception("Owner not set, but key set");
        if (bak.Block.Readers == null)
        {
            bak.Block.Readers = new EncryptionKeyList();
            bc |= BlockChange.Readers;
        }
        if (_admins.Any())
        {
            foreach (var ak in _admins)
            {
                if (ak.Equals(_ownerSig))
                    continue;
                var exists = bak.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(ak)).FirstOrDefault();
                if (exists == null)
                {
                    bc |= BlockChange.Readers;
                    bak.Block.Readers.EncryptionKeys.Add(new EncryptionKey { Recipient = ak});
                }
                if (bak.Block.Writers == null)
                    bak.Block.Writers = new KeyHashList();
                var ew = bak.Block.Writers.KeyHashes.Where(x=>x.Equals(ak)).FirstOrDefault();
                if (ew == null)
                {
                    bc |= BlockChange.Writers;
                    bak.Block.Writers.KeyHashes.Add(ak);
                }
            }
        }
        if ((bc & BlockChange.GroupRemove) != 0)
        {
            if (bak.Block.Data.GroupKeys == null)
                bak.Block.Data.GroupKeys = new GroupKeyList();
            bak.Block.GroupVersion += 1;
            var k = new RSACryptoServiceProvider(2048);
            var priv = k.ExportRSAPrivateKey();
            var pub = k.ExportRSAPublicKey();
            bak.Block.Data.GroupKeys.Keys.Add(Google.Protobuf.ByteString.CopyFrom(priv));
            bak.Block.GroupPublicKey = Google.Protobuf.ByteString.CopyFrom(pub);
        }
        if ((bc & BlockChange.Data) != 0)
        {
            var ser = bak.Block.Data.ToByteArray();
            var enc = Encrypt(ser, aes);
            bak.Block.Signed = new SignedData();
            bak.Block.Signed.EncryptedBlock = Google.Protobuf.ByteString.CopyFrom(enc);
            bak.Block.Signed.Version = bak.Block.Version;
            //sign
            var sig = (_owner as RSA).SignData(bak.Block.Signed.ToByteArray(), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            bak.Block.EncryptedDataSignature = new Signature();
            bak.Block.EncryptedDataSignature.KeyHash = _ownerSig;
            bak.Block.EncryptedDataSignature.Signature_ = Google.Protobuf.ByteString.CopyFrom(sig);
        }
        if ((bc & BlockChange.Writers) != 0 && bak.Block.Writers != null)
        {
            foreach (var w in bak.Block.Writers.KeyHashes)
            {
                // ensure w is in readers
                var r = bak.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(w)).FirstOrDefault();
                if (r == null)
                {
                    var (wk, wv) = await GetKey(w);
                    var aesSer = aes.ToByteArray();
                    var crypted = (wk as RSA).Encrypt(aesSer, RSAEncryptionPadding.Pkcs1);
                    var ek = new EncryptionKey();
                    ek.Recipient = w;
                    ek.GroupVersion = wv;
                    ek.EncryptedKeyIv = Google.Protobuf.ByteString.CopyFrom(crypted);
                    bak.Block.Readers.EncryptionKeys.Add(ek);
                    bc |= BlockChange.Readers; // notify we must resign readers
                }
            }
            // sign writers block
            var ws = bak.Block.Writers.ToByteArray();
            var sig = (_owner as RSA).SignData(ws, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            bak.Block.WritersSignature = new Signature();
            bak.Block.WritersSignature.KeyHash = _ownerSig;
            bak.Block.WritersSignature.Signature_ = Google.Protobuf.ByteString.CopyFrom(sig);
        }
        if ((bc & BlockChange.Readers) != 0)
        {
            var ekowner = bak.Block.Readers.EncryptionKeys.Where(k=>k.Recipient.Equals(_ownerSig)).FirstOrDefault();
            if (ekowner == null)
            {
                bak.Block.Readers.EncryptionKeys.Add(new EncryptionKey { Recipient = _ownerSig}); 
            }
            foreach (var r in bak.Block.Readers.EncryptionKeys)
            { // ensure everyone has a key
                if (r.EncryptedKeyIv == null || r.EncryptedKeyIv.Length == 0)
                {
                    var (wk,wv) = await GetKey(r.Recipient);
                    var aesSer = aes.ToByteArray();
                    var crypted = (wk as RSA).Encrypt(aesSer, RSAEncryptionPadding.Pkcs1);
                    r.EncryptedKeyIv = Google.Protobuf.ByteString.CopyFrom(crypted);
                    r.GroupVersion = wv;
                }
            }
            var ws = bak.Block.Readers.ToByteArray();
            var sig = (_owner as RSA).SignData(ws, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            bak.Block.ReadersSignature = new Signature();
            bak.Block.ReadersSignature.KeyHash = _ownerSig;
            bak.Block.ReadersSignature.Signature_ = Google.Protobuf.ByteString.CopyFrom(sig);
        }
        // clear plaintext fields
        bak.Block.Raw = ByteString.Empty;
        bak.Block.Data = null;
        _logger.LogInformation("Sealed block: {block}", bak.ToString());
    }
    public Errno UnsealImmutable(BlockAndKey bak, AESKey aes)
    {
        var raw = Decrypt(bak.Block.Signed.EncryptedData.ToByteArray(), aes);
        bak.Block.Raw = Google.Protobuf.ByteString.CopyFrom(raw);
        return 0;
    }
    public async Task<AESKey> ExtractKey(BlockAndKey bak)
    {
        AESKey aes = null;
        if (bak.Block.Readers == null)
        {
            _logger.LogWarning("Empty reader block");
            return null;
        }
        var me = bak.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(_ownerSig)).FirstOrDefault();
        if (me == null)
        {
            // try groups
            foreach (var ek in bak.Block.Readers.EncryptionKeys.Where(x=>x.GroupVersion != 0))
            {
                var gb = await GetKeyBlock(ek.Recipient);
                var gbk = new BlockAndKey { Key = ek.Recipient, Block = gb};
                var uk = await UnsealMutable(gbk);
                if (uk == null)
                    continue;
                var privb = gb.Data.GroupKeys.Keys[(int)(ek.GroupVersion-1)].ToByteArray();
                var k = new RSACryptoServiceProvider(512);
                k.ImportRSAPrivateKey(privb, out _);
                var aesSerg = k.Decrypt(ek.EncryptedKeyIv.ToByteArray(), RSAEncryptionPadding.Pkcs1);
                using var msg = new MemoryStream(aesSerg);
                return AESKey.Parser.ParseFrom(msg);
            }
             _logger.LogWarning("fs owner not found in readers");
            return null;
        }
        var aesSer = (_owner as RSA).Decrypt(me.EncryptedKeyIv.ToByteArray(), RSAEncryptionPadding.Pkcs1);
        using var ms = new MemoryStream(aesSer);
        return aes = AESKey.Parser.ParseFrom(ms);
    }
    public async Task<AESKey> UnsealMutable(BlockAndKey bak)
    {
        _logger.LogInformation("unsealing block: {block}", bak.ToString());
        var aes = await ExtractKey(bak);
        if (aes == null)
        {
            _logger.LogWarning("Key extraction failed");
            return null;
        }
        var raw = Decrypt(bak.Block.Signed.EncryptedBlock.ToByteArray(), aes);
        using var msblock = new MemoryStream(raw);
        var dblock = BlockData.Parser.ParseFrom(msblock);
        bak.Block.Data = dblock;
         _logger.LogInformation("unsealed block: {block}", bak.ToString());
        return aes;
    }
    public async Task<int> VerifyRead(BlockAndKey bak)
    {
        if (bak.Block.Owner == null)
        { // immutable
            var iaddr = Utils.Checksum(bak.Block.Salt.ToByteArray(), bak.Block.Signed.EncryptedData.ToByteArray());
            if (!iaddr.Equals(bak.Key))
                return 1;
            return 0;
        }
        // mutable
        var addr = Utils.Checksum(bak.Block.Owner.ToByteArray(), bak.Block.Salt.ToByteArray());
        if (!addr.Equals(bak.Key))
            return 2;
        var (bok, bv) = await GetKey(bak.Block.Owner);
        if (bok == null)
            return 10;
        if (bv != 0)
            return 12;
        var ok = true;
        if (bak.Block.Writers != null)
        {
            ok = (bok as RSA).VerifyData(
                bak.Block.Writers.ToByteArray(),
                bak.Block.WritersSignature.Signature_.ToByteArray(),
                HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            if (!ok)
                return 5;
        }
        ok = (bok as RSA).VerifyData(
            bak.Block.Readers.ToByteArray(),
            bak.Block.ReadersSignature.Signature_.ToByteArray(),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (!ok)
            return 6;
        if (!bak.Block.EncryptedDataSignature.KeyHash.Equals(bak.Block.Owner))
        {
             if (bak.Block.Writers == null)
                 return 7;
            var hit = bak.Block.Writers.KeyHashes.Where(x=>x.Equals(bak.Block.EncryptedDataSignature.KeyHash)).FirstOrDefault();
            if (hit == null)
                return 8; // not a writer
            (bok, bv) = await GetKey(bak.Block.EncryptedDataSignature.KeyHash);
            if (bok == null)
                return 11;
            if (bv != 0)
                return 14;
        }
        ok = (bok as RSA).VerifyData(
            bak.Block.Signed.ToByteArray(),
            bak.Block.EncryptedDataSignature.Signature_.ToByteArray(),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (!ok)
            return 9;
        if (bak.Block.Signed.Version != bak.Block.Version)
            return 15;
        return 0;
    }
    public async Task<int> VerifyWrite(BlockAndKey bak, BlockAndKey previous)
    {
        if (bak.Block.Owner == null)
            return 0;
        if (previous != null && !bak.Block.Owner.Equals(previous.Block.Owner))
            return 4;
        var vr = await VerifyRead(bak);
        if (vr != 0)
            return vr;
        return 0;
    }
    public async Task<int> VerifyDelete(BlockAndKey request, Block target, Block owner)
    {
        Key allowed = target.OwningBlock != null ? owner.Owner : target.Owner;
        if (allowed == null)
            return 2;
        var (bok, bv) = await GetKey(allowed);
        if (! (bok as RSA).VerifyData(
            request.Key.ToByteArray(),
            request.Block.EncryptedDataSignature.Signature_.ToByteArray(),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1))
            return 1;
        return 0;
    }
    public BlockAndKey DeletionRequest(Key target, Key owner)
    {
        var res = new BlockAndKey();
        res.Key = target;
        res.Block = new Block();
        res.Block.EncryptedDataSignature = new Signature();
        res.Block.EncryptedDataSignature.KeyHash = _ownerSig;
        res.Block.EncryptedDataSignature.Signature_ =
          Google.Protobuf.ByteString.CopyFrom(
              (_owner as RSA).SignData(target.ToByteArray(), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
              );
        return res;
    }
    public bool CanWrite(BlockAndKey bak)
    {
        if (bak.Block.Owner == null)
            return false;
        if (bak.Block.Owner.Equals(_ownerSig))
            return true;
        if (bak.Block.Writers == null)
            return false;
        var hit = bak.Block.Writers.KeyHashes.Where(x=>x.Equals(_ownerSig)).FirstOrDefault();
        return hit != null;
    }
    public void Inherit(BlockAndKey target, BlockAndKey parent, bool ir, bool iw)
    {
        if (ir)
        {
            if (target.Block.Readers == null)
                target.Block.Readers = new EncryptionKeyList();
            foreach (var r in parent.Block.Readers.EncryptionKeys)
            {
                target.Block.Readers.EncryptionKeys.Add(new EncryptionKey { Recipient = r.Recipient}); 
            }
        }
        if (iw)
        {
            if (parent.Block.Writers != null)
            {
                if (target.Block.Writers == null)
                    target.Block.Writers = new KeyHashList();
                foreach (var w in parent.Block.Writers.KeyHashes)
                {
                    target.Block.Writers.KeyHashes.Add(w);
                }
            }
            if (!parent.Block.Owner.Equals(target.Block.Owner))
            {
                if (target.Block.Writers == null)
                    target.Block.Writers = new KeyHashList();
                target.Block.Writers.KeyHashes.Add(parent.Block.Owner);
            }
        }
    }
}