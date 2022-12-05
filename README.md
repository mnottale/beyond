# Beyond: A prototype distributed filesystem for Linux

## What is it?

Beyond is a work-in-progress distributed filesystem.
Data is split into blocks which are spread across storage nodes, with
a configureable replication factor.

## How to build it?

Clone the repository and its submodules.

Then run `./build-libs.sh` in directory `Mono.Fuse.NETStandard`.

Then `dotnet build` in `src`.


## How to run it?

Launch storage nodes with `beyond --serve /path/to/storage --port 20003 --peers localhost:20001 --replication 3`.
First started node has no `--peer`, each subsequent node need one random peer to be given.

Launch mount with `beyond --mount /mount/point --replication 3 --peers http://localhost:20001 --fs-name myname`.
On first launch add `--create`. Never pass it again or it will destroy everything.

It is mandatory to pass the same `--replication` to all commands.

## How is it implemented?

C# GRPC, Fuse (C# binding). There is one service for node-to-node communication, and each node
exposes a mount service.

Data and metadata (filesystem layout) are stored in Blocks. Each Block
is replicated on the given number of nodes, writes are only permitted if
a majority of nodes can be reached and use a simple consensus algorithm.


## Should I use it yet in production or for any data I can't loose?

No.

## Known bugs/limitations

- GRPC connection errors are not handled, if a node disconnects you'll need to
  restart all other nodes.

## Future features and goals

  - Caching layer for faster usage
  - Handle node disconnection
  - Crypto layer
  - Safer read that checks quorum
  - Healing under-replicated blocks
  - Node eviction