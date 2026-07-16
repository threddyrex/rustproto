
# rustproto

This is a third-party implementation of an atproto PDS. It is a Rust port of [dnproto](https://github.com/threddyrex/dnproto). 
My main account ([threddyrex.org](https://bsky.app/profile/did:web:threddyrex.org)) is hosted on rustproto.

For info on how to implement a PDS from scratch, check out [Ten Steps to a PDS](/docs/articles/ten-steps-to-a-pds.md).

*(Note: rustproto is an independent, community-driven project and is not affiliated with, sponsored by, or endorsed by Bluesky Social, PBC.)*



&nbsp;

# Source code

CAR repo encoding and decoding:

- [repo/mod.rs](/src/repo/mod.rs) - repo module - explains repo structure ⭐
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

