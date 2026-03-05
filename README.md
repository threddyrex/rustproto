
# rustproto - software for ATProto and Bluesky

Rust port of [dnproto](https://github.com/threddyrex/dnproto).

This is an atproto PDS.



&nbsp;

# Source code

CAR repo encoding and decoding:

- [repo.rs](/src/repo/repo.rs) - repo parsing entry point, and explains repo structure ⭐
- [dag_cbor.rs](/src/repo/dag_cbor.rs) - decoding/encoding dag cbor ⭐
- [cid.rs](/src/repo/cid.rs) - decoding/encoding cid
- [varint.rs](/src/repo/varint.rs) - decoding/encoding varint

MST data structure:

- [mst.rs](/src/mst/mst.rs) - MST
- [mst_node.rs](/src/mst/mst_node.rs) - represents one node in the MST
- [mst_entry.rs](/src/mst/mst_entry.rs) - represents one entry in a MST node

PDS implementation:

- [/xrpc/](/src/pds/xrpc/) - the XRPC endpoints
- [installer.rs](/src/pds/installer.rs) - installing the PDS
- [server.rs](/src/pds/server.rs) - PDS entry point
- [db/](/src/pds/db/) - the database interface, where the repo is stored
- [repo_mst.rs](/src/repo/repo_mst.rs) - converting MST into dag-cbor for use in repos
- [user_repo.rs](/src/pds/user_repo.rs) - operations on the user's repo

Listening to a firehose:

- [firehose/](/src/firehose/)

General Bluesky WS calls:

- [bluesky_client.rs](/src/ws/bluesky_client.rs) - calling the Bluesky API.


&nbsp;

# Using the command line tool (Windows)

The following steps show how to use the command line tool on Windows in PowerShell.
Requires Rust.

To get started, change into the root directory and build.

```powershell
cargo build --release
```

Next, change into the scripts directory, and list the files:

```powershell
cd powershell
ls
```

Most of the files in this directory represent one "command" of the tool. Here are the available commands:

```powershell
# resolve actor info and retrieve did, did doc
.\ResolveActorInfo.ps1 -actor <handle or did>

# download the user's repo and store in the data directory
.\GetRepo.ps1 -actor <handle or did>

# print stats for the downloaded repo
.\PrintRepoStats.ps1 -actor <handle or did>

# print records for the downloaded repo
.\PrintRepoRecords.ps1 -actor <handle or did>

# walk the MST for the downloaded repo
.\WalkMst.ps1 -actor <handle or did>

# start listening to a firehose
.\StartFirehoseConsumer.ps1
```

Note: Some client debugging commands from dnproto are not yet ported to rustproto, including:
- GetPlcHistory
- PrintRepoPosts
- CreateSession / DeleteSession
- GetUnreadCount
- CreatePost
- GetPdsInfo
- DescribeRepo


&nbsp;

# The data directory

When you are using the command line tool, it uses a local directory to store cached data.
By default, it uses the "data" directory in the repo. You can change this in the _Defaults.ps1 file.


&nbsp;

# Linux Support

Is Linux supported? Yes! I run my dnproto PDS on Linux.
