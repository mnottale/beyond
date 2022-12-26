# Beyond: A prototype distributed filesystem for Linux

## What is it?

Beyond is a work-in-progress cryptographically secure distributed filesystem.

Data is split into blocks which are spread across storage nodes, with
a configureable replication factor.

## How to build it?

Clone the repository and its submodules.

Then run `./build-libs.sh` in directory `Mono.Fuse.NETStandard`.

Then `dotnet build` in `src`.


## How to run it?

Launch storage nodes with `beyond --serve /path/to/storage --port 20003 --peers localhost:20001 --replication 3`.
First started node has no `--peer`, each subsequent node need one other peer to be given.

By default a node will advertise its first non-loopback address to the others.
Use `--advertise-address` passing an explicit IP or a netmask to search to force a different IP address.

By default a node will listen on all network interfaces, use `--listen <ip>` to override.


Launch mount with `beyond --mount /mount/point --replication 3 --peers http://localhost:20001 --fs-name myname`.
On first launch add `--create`. Never pass it again or it will destroy everything.

It is mandatory to pass the same `--replication` to all commands.

## How to activate crypto layer?

Each user should create a key pair with `beyond --createKey <keyname>`.

Storage nodes should run with `--crypt`, and mounts with `--mountKey <keyname>`.

## How is it implemented?

C# GRPC, Fuse (C# binding). There is one service for node-to-node communication, and each node
exposes a mount service.

Data and metadata (filesystem layout) are stored in Blocks. Each Block
is replicated on the given number of nodes, writes are only permitted if
a majority of nodes can be reached and use a simple consensus algorithm.


## Should I use it yet in production or for any data I can't loose?

No.

## Can you describe the crypto model?

File data is stored in AES-encrypted salted immutable blocks. Their address
is the hash of salt+encrypted_data, preventing any file tampering.

File manifest (list of data blocks) and directory content are stored in
mutable blocks, whose address is the hash of salt+owner_key_fingerprint.

Mutable blocks are AES-encrypted and signed. One copy of the AES key is stored
RSA-encrypted per registered reader.
Reader and writer lists are signed by the owner.

All blocks are validated by nodes at read and write time. All signatures and
hashes are checked, and at update time nodes verify that owner doesn't change.

User keys are referenced by their fingerprint (hash of public key), the actual
keys are stored as immutable blocks.

## How to manage multiple users with crypto enabled?

You can add keys to each file or directory's authorized readers and writers.

This is achieved using extended attributes.

The root block owner (creator of the filesystem) can register aliases to avoid
using key hashes all the time.

Ask for a user's key hash (displayed when mounting) and do

    xattr -w beyond.addalias alice:AABBCCDD... mountpoint/

where alice is your alias and AABBCCDD... is the 64 characters hex encoded key hash.

Then the owner of a file or directory can add readers and writers
using `beyond.addwriter` and `beyond.addreader` xattr names.
The payload is a key hash or a registered alias.

To ease things a bit, you can mark a directory for read or write key inheritance:

    xattr -w beyond.inherit r mountpoint/some/dir

If set to r/w (or rw), all created files and directory will copy the list of readers/writers
from the parent directory. Inheritance flags are also inherited.

Note that a writer of a directory can set it's content to anything, potentially
unlinking files she does not have access to.

Aditionally, you can create groups of users and add read permission to the group.

To create a group, use:

    xattr -w beyond.creategroup <groupname> mountpoint/

replacing <groupname> with the name you want to give to the group.

Then you can add the group through its alias to the readers of a file or directory;

    xattr -w beyond.addreader <groupname> mountpoint/some/file

The members of a group are its readers. To add a user key to the group, simply run;

    xattr -w beyond.addreader <username> mountpoint/beyond:<groupname>

the `beyond:something` is a special file at the root that allows pointing to a specific
block by its alias or address.

Furthermore as an admin you might want to keep some control over the whole content.
This is achieved with admin keys. Those keys once registered are added automatically
by the filesystem layer to the reader and writers of all blocks created.

    xattr -w beyond.addadmin <userOrKeyHash> mountpoint/

## Future features and goals

  - Caching layer for faster usage
  - Plug crypto layer weaknesse by signing block deletion requests.
  - Safer read that checks quorum
  - Healing outdated blocks
  - ...