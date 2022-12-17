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
}
public class Crypto
{
    private AsymmetricAlgorithm _owner;
    private Key _ownerSig;
    private Dictionary<Key, AsymmetricAlgorithm> _keys = new();
    private ILogger _logger;

    public Func<Key, Task<Block>> GetKeyBlock;
    public Func<Key, Task<Block>> GetPreviousBlock;
    public Crypto()
    {
        _logger = Logger.loggerFactory.CreateLogger<Crypto>();
    }
    public void SetOwner(byte[] encryptedKey, string passphrase)
    {
        var aa = new RSACryptoServiceProvider(512);
        aa.ImportEncryptedPkcs8PrivateKey(passphrase, encryptedKey, out int br);
        _owner = aa;
        var pub = aa.ExportRSAPublicKey();
        _ownerSig = Utils.Checksum(pub);
    }
    public static byte[] MakeAsymmetricKey(string passphrase)
    {
        var k = new RSACryptoServiceProvider(2048);
        return k.ExportEncryptedPkcs8PrivateKey(passphrase,
            new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc,
                HashAlgorithmName.SHA256,
                1000)); // no idea what I'm doing, this is dog
    }
    public BlockAndKey ExportOwnerPublicKey()
    {
        var bak = new BlockAndKey();
        var pub = (_owner as RSA).ExportRSAPublicKey();
        bak.Key = _ownerSig;
        bak.Block = new Block();
        bak.Block.Raw = Google.Protobuf.ByteString.CopyFrom(pub);
        return bak;
    }
    public async Task<AsymmetricAlgorithm> GetKey(Key sig)
    {
        if (_keys.TryGetValue(sig, out var res))
            return res;
        if (_ownerSig.Equals(sig))
            return _owner;
        var blk = await GetKeyBlock(sig);
        var data = blk.Raw.ToByteArray();
        var aa = new RSACryptoServiceProvider(512);
        int br = 0;
        aa.ImportRSAPublicKey(data, out br);
        _keys.Add(sig, aa);
        return aa;
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
        bak.Block.EncryptedData = Google.Protobuf.ByteString.CopyFrom(
                Encrypt(bak.Block.Raw.ToByteArray(), aes));
        var addr = Utils.Checksum(bak.Block.Salt.ToByteArray(), bak.Block.EncryptedData.ToByteArray());
        bak.Key = new Key();
        bak.Key.Key_ = addr.Key_;
        bak.Block.Raw = null;
    }
    public async Task SealMutable(BlockAndKey bak, BlockChange bc, AESKey aes)
    {
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
        if ((bc & BlockChange.Data) != 0)
        {
            var bclear = new Block();
            bclear.File = bak.Block.File;
            bclear.Directory = bak.Block.Directory;
            bclear.SymLink = bak.Block.SymLink;
            bclear.Mode = bak.Block.Mode;
            var ser = bclear.ToByteArray();
            bak.Block.EncryptedBlock = Google.Protobuf.ByteString.CopyFrom(
                Encrypt(ser, aes));
            //sign
            var sig = (_owner as RSA).SignData(ser, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            bak.Block.EncryptedDataSignature = new Signature();
            bak.Block.EncryptedDataSignature.KeyHash = _ownerSig;
            bak.Block.EncryptedDataSignature.Signature_ = Google.Protobuf.ByteString.CopyFrom(sig);
        }
        if ((bc & BlockChange.Writers) != 0)
        {
            foreach (var w in bak.Block.Writers.KeyHashes)
            {
                // ensure w is in readers
                var r = bak.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(w)).FirstOrDefault();
                if (r == null)
                {
                    var wk = await GetKey(w);
                    var aesSer = aes.ToByteArray();
                    var crypted = (wk as RSA).Encrypt(aesSer, RSAEncryptionPadding.Pkcs1);
                    var ek = new EncryptionKey();
                    ek.Recipient = w;
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
        if (bak.Block.Readers == null)
            bak.Block.Readers = new EncryptionKeyList();
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
                    var wk = await GetKey(r.Recipient);
                    var aesSer = aes.ToByteArray();
                    var crypted = (wk as RSA).Encrypt(aesSer, RSAEncryptionPadding.Pkcs1);
                    r.EncryptedKeyIv = Google.Protobuf.ByteString.CopyFrom(crypted);
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
        bak.Block.File = null;
        bak.Block.Directory = null;
        bak.Block.SymLink = null;
        bak.Block.Mode = "";
    }
    public Errno UnsealImmutable(BlockAndKey bak, AESKey aes)
    {
        var raw = Decrypt(bak.Block.EncryptedData.ToByteArray(), aes);
        bak.Block.Raw = Google.Protobuf.ByteString.CopyFrom(raw);
        return 0;
    }
    public Errno ExtractKey(BlockAndKey bak, out AESKey aes)
    {
        aes = null;
        if (bak.Block.Readers == null)
            return 0;
        var me = bak.Block.Readers.EncryptionKeys.Where(x=>x.Recipient.Equals(_ownerSig)).FirstOrDefault();
        if (me == null)
            return Errno.EPERM;
        var aesSer = (_owner as RSA).Decrypt(me.EncryptedKeyIv.ToByteArray(), RSAEncryptionPadding.Pkcs1);
        using var ms = new MemoryStream(aesSer);
        aes = AESKey.Parser.ParseFrom(ms);
        return 0;
    }
    public Errno UnsealMutable(BlockAndKey bak, out AESKey aes)
    {
        var err = ExtractKey(bak, out aes);
        if (err != 0)
        {
            _logger.LogWarning("Key extraction failed");
            return err;
        }
        var raw = Decrypt(bak.Block.EncryptedBlock.ToByteArray(), aes);
        using var msblock = new MemoryStream(raw);
        var dblock = Block.Parser.ParseFrom(msblock);
        bak.Block.File = dblock.File;
        bak.Block.Directory = dblock.Directory;
        bak.Block.SymLink = dblock.SymLink;
        bak.Block.Mode = dblock.Mode;
        return 0;
    }
    public async Task<bool> VerifyRead(BlockAndKey bak)
    {
        if (bak.Block.Owner == null)
        { // immutable
            var iaddr = Utils.Checksum(bak.Block.Salt.ToByteArray(), bak.Block.EncryptedData.ToByteArray());
            if (!iaddr.Equals(bak.Key))
                return false;
            return true;
        }
        // mutable
        var addr = Utils.Checksum(bak.Block.Owner.ToByteArray(), bak.Block.Salt.ToByteArray());
        if (!addr.Equals(bak.Key))
            return false;
        var bok = await GetKey(bak.Block.Owner);
        var ok = (bok as RSA).VerifyData(
            bak.Block.EncryptedBlock.ToByteArray(),
            bak.Block.EncryptedDataSignature.ToByteArray(),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (!ok)
            return false;
        // Is there a point in verifying readers and writers signature when reading?
        return true;
    }
    public async Task<bool> VerifyWrite(BlockAndKey bak)
    {
        if (!await VerifyRead(bak))
            return false;
        if (bak.Block.Owner == null)
            return true;
        // for now only owner can change readers/writers, although block format permits others to do so
        var bok = await GetKey(bak.Block.Owner);
        var ok = (bok as RSA).VerifyData(
            bak.Block.Writers.ToByteArray(),
            bak.Block.WritersSignature.Signature_.ToByteArray(),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (!ok)
            return false;
        ok = (bok as RSA).VerifyData(
            bak.Block.Readers.ToByteArray(),
            bak.Block.ReadersSignature.Signature_.ToByteArray(),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (!ok)
            return false;
        return true;
    }
}