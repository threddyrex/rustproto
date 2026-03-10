

# Ten Steps to a PDS

Building an atproto Personal Data Server (PDS) seems daunting at first. It was for me — I wasn't sure where to start.

If you'd like to write a PDS, I suggest following these ten steps. Each step builds on the previous one, 
starting from simple command-line tools and working up to a fully functioning PDS.

Each section includes:

- **T-shirt size** — effort required to implement
- **Reading** — items to read first
- **Coding concepts** — programming concepts used
- **Source code examples** — links to rustproto source
- **Instructions** — what you should code in this step

&nbsp;

---

## Step 0: Hello World (prerequisites)

Ok, this is cheating and not actually a step. It's more about the prerequisites.

Before getting to any of the atproto stuff, get started with the basics. Create a new git repo. Create a folder structure 
with places for sources and tests. Write a simple console program. Parse the command line arguments. Create wrapper scripts (Bash, PowerShell, etc.) 
so you can easily run your tool from the command line. Everything needed to write a simple "Hello World!" in your chosen language.

**Source code examples:**

- [main.rs](/src/main.rs) — CLI entry point with argument parsing and command dispatch
- [powershell/](/powershell/) — PowerShell scripts for all CLI commands
- [bash/](/bash/) — Bash scripts for Linux/macOS

Now onto the actual steps.

&nbsp;

---

## Step 1: Resolving an actor

This is often the first thing that atproto devs learn about the protocol. In this step you will go from a user's handle, to their DID, to their PDS hostname.

**T-shirt size:** Small

**Reading:**

- *Handle → DID*
  - [Handle Resolution](https://atproto.com/specs/handle#handle-resolution) — describes how you go from a handle to a DID. There are two ways to do this: either by DNS TXT record, or by HTTPS well-known. Only one needs to be set (but both can).
  - The [bsky-debug](https://bsky-debug.app/handle) page allows you to see this in action.
- *DID → did.json*
  - If it's a `did:plc`, use the [PLC Directory](https://web.plc.directory) to look up the DID document.
  - If it's a `did:web`, use the hostname in the DID to find the did.json (e.g. `threddyrex.org/.well-known/did.json`).
  - [DID - AT Protocol](https://atproto.com/specs/did)
- *did.json → PDS hostname*
  - Now that you have the did.json, you can look up the PDS hostname in the document itself.

**Coding concepts:**

- Sending an HTTPS request
- Sending a DNS query
- Parsing JSON

**Source code examples:**

- [bluesky_client.rs](/src/ws/bluesky_client.rs) — the resolve_actor_info function resolves a handle to a DID, fetches the DID document, and extracts the PDS hostname
- [actor_info.rs](/src/ws/actor_info.rs) — data structure representing the resolved actor info

**Instructions:**

Write a tool that accepts a handle and prints out the DID, did.json, and PDS hostname.

&nbsp;

---

## Step 2: Downloading a repo

A user's data is stored in a repo. This repo can be downloaded directly from the PDS. In this step, you will call the endpoint to download the repo and write it to disk.

**T-shirt size:** Small

**Reading:**

- [com.atproto.sync.getRepo](https://docs.bsky.app/docs/api/com-atproto-sync-get-repo) — the HTTP reference doc for the `getRepo` endpoint.

**Coding concepts:**

- Sending an HTTPS request
- Writing an HTTPS response body to local disk

**Source code examples:**

- [bluesky_client.rs](/src/ws/bluesky_client.rs) — the `get_repo` function downloads a CAR file from a PDS

**Instructions:**

Write a command-line tool that downloads a repository for a user and writes it to local disk.

&nbsp;

---

## Step 3: Parsing a repo

Now things get more interesting. You have a CAR repo file on disk, and you need to parse it and print out what you see.

CAR files are a binary file format. You can approach this step by either using existing libraries that can understand DAG-CBOR, 
or write it from scratch yourself. In rustproto I did the latter.

**T-shirt size:** Medium

**Reading:**

- [Content-Addressable aRchives (CAR)](https://ipld.io/specs/transport/car/carv1/) — great first doc to read
- [Repository](https://atproto.com/specs/repository) — AT Protocol repository spec

**Coding concepts:**

- Reading file input, one byte at a time
- Converting a binary file to an in-memory representation in your programming language
- Bit shifting and variable-length integer encoding

**Source code examples:**

- [repo.rs](/src/repo/repo.rs) — repo parsing entry point, explains the overall repo structure
- [dag_cbor.rs](/src/repo/dag_cbor.rs) — decoding DAG-CBOR binary format
- [cid.rs](/src/repo/cid.rs) — decoding CID (Content Identifier)
- [varint.rs](/src/repo/varint.rs) — decoding variable-length integers
- [repo_header.rs](/src/repo/repo_header.rs) — parsing the repo header
- [repo_record.rs](/src/repo/repo_record.rs) — representing individual records

**Instructions:**

Write a tool that reads a CAR file from disk, parses it, and prints out the records that appear in the file. You should see the repo header, repo records, MST nodes, and repo commit.

&nbsp;

---

## Step 4: Writing the repo and comparing (round trip)

Now that you can parse a repo, it's time to reverse the direction and write a repo to disk.

**T-shirt size:** Medium

**Reading:**

- Same as Step 3.

**Coding concepts:**

- Converting an in-memory representation of a CAR file to binary data
- Writing that binary data to a local file

**Source code examples:**

- [dag_cbor.rs](/src/repo/dag_cbor.rs) — the encoding side of DAG-CBOR
- [cid.rs](/src/repo/cid.rs) — encoding CIDs

**Instructions:**

Write a tool that reads a repo from disk, fully parses it into an in-memory representation (Step 3 above), then converts that 
in-memory representation back to binary and writes it to a new file. The two files (input and output) should be identical. Additionally, 
write a tool that loads both files and compares them byte-by-byte.

&nbsp;

---

## Step 5: Listening to a firehose

Data gets passed around the atproto network via "event streams". The data is formatted like CAR files, so the work you did in previous steps 
can be reused here. The new code is mostly about connecting to the `subscribeRepos` endpoint via WebSockets.

**T-shirt size:** Small

**Reading:**

- [Event Stream](https://atproto.com/specs/event-stream) — AT Protocol event stream spec

**Coding concepts:**

- WebSockets — connecting to a WebSocket endpoint and reading frames

**Source code examples:**

- [firehose/mod.rs](/src/firehose/mod.rs) — WebSocket consumer that connects to `subscribeRepos` and processes incoming frames

**Instructions:**

Write a tool that connects to the `subscribeRepos` endpoint on a PDS and prints out the events it sees come over the wire. 
If it's a Bluesky PDS, it will be noisy.

&nbsp;

---

## Step 6: MST implementation

atproto uses a data structure called a Merkle Search Tree (MST) for indexing all records in a repository.
To build a functioning PDS, you'll need to be able to assemble, query, and update this tree.

**T-shirt size:** Medium

**Reading:**

- [Repository](https://atproto.com/specs/repository) — the MST section of the AT Protocol repository spec

**Coding concepts:**

- Tree data structures
- Hashing (SHA-256) to determine tree depth
- Recursive tree assembly and traversal

**Source code examples:**

- [mst.rs](/src/mst/mst.rs) — MST assembly and querying
- [mst_node.rs](/src/mst/mst_node.rs) — represents one node in the MST
- [mst_entry.rs](/src/mst/mst_entry.rs) — represents one entry in an MST node
- [mst_item.rs](/src/mst/mst_item.rs) — flat item representation used during tree assembly
- [repo_mst.rs](/src/repo/repo_mst.rs) — converting MST into DAG-CBOR for use in repos
- [main.rs](/src/main.rs) - "cmd_walk_mst" shows an example of walking a MST in a CAR repo
**Instructions:**

Implement the MST data structure. Given a flat list of record paths and CIDs (keys and values), assemble them into the correct 
tree structure. Then implement querying: given a record path, walk the tree to find its CID.

&nbsp;

---

## Step 7: Database implementation

Your PDS needs persistent storage. In this step, build out the database layer that will store accounts, repo records, sessions, blobs, 
and configuration. Start by first adding the boilerplate stuff - like creating tables, creating connections, etc. - and then
as you progress through the rest of the steps, you'll wind up adding more entities to store in the db.

**T-shirt size:** Large

**Reading:**

- Review the XRPC endpoints you want to support — the database schema will be driven by what those endpoints need.

**Coding concepts:**

- SQL and database design
- CRUD operations
- Schema migrations

**Source code examples:**

- [pds_db.rs](/src/pds/db/pds_db.rs) — main database operations interface
- [sqlite_db.rs](/src/pds/db/sqlite_db.rs) — SQLite wrapper with connection management
- [entities.rs](/src/pds/db/entities.rs) — data model (Config, RepoHeader, RepoCommit, RepoRecord, Session, Passkey, etc.)
- [installer.rs](/src/pds/installer.rs) — database schema creation and initial configuration setup

**Instructions:**

Design and implement the basic database schema. Add helper functions for creating tables, creating connections, etc.


&nbsp;

---

## Step 8: Administrative interface

Before diving into the XRPC endpoints, build an administrative UI for your PDS. At least the basics.
This gives you a way to manage the server. I found that once I had a simple admin UI started, I kept
adding more functions to it. Feel free to skip things like passkeys for now.

**T-shirt size:** Medium

**Coding concepts:**

- Server-side HTML rendering
- Session-based authentication

**Source code examples:**

- [admin/login.rs](/src/pds/admin/login.rs) — admin authentication
- [admin/home.rs](/src/pds/admin/home.rs) — home dashboard
- [admin/sessions.rs](/src/pds/admin/sessions.rs) — active session management
- [admin/stats.rs](/src/pds/admin/stats.rs) — server statistics
- [admin/config.rs](/src/pds/admin/config.rs) — configuration management
- [admin/actions.rs](/src/pds/admin/actions.rs) — administrative actions

**Instructions:**

Build a web-based admin interface with pages for viewing server status, managing sessions, etc.

&nbsp;

---

## Step 9: XRPC endpoints

This is the big one. Implement the XRPC endpoints that the larger network use to connect with your PDS. I suggest implementing them 
in the following order, grouped by category. See the [XRPC Reference](/docs/xrpc-reference.md) for the list implemented by rustproto.

**T-shirt size:** Extra Large

**Reading:**

- [AT Protocol XRPC](https://atproto.com/specs/xrpc) — the XRPC spec
- [Bluesky HTTP API Reference](https://docs.bsky.app/docs/category/http-reference) — detailed endpoint documentation

**Coding concepts:**

- HTTP server routing
- JSON request/response handling
- JWT authentication and token management
- WebSocket server implementation
- Proxying requests to upstream services

**Suggested implementation order:**

| # | Group | Endpoints | Notes |
|---|-------|-----------|-------|
| 1 | **Foundation** (3) | `_health`, `hello`, `describeServer` | Get server responding |
| 2 | **Identity** (1) | `resolveHandle` | Handle resolution |
| 3 | **Authentication** (7) | `createSession`, `getSession`, `refreshSession`, `getServiceAuth`, `checkAccountStatus`, `activateAccount`, `deactivateAccount` | Sessions — needed for testing authenticated endpoints |
| 4 | **Repo Operations** (7) | `describeRepo`, `getRecord`, `listRecords`, `createRecord`, `putRecord`, `deleteRecord`, `applyWrites` | Core CRUD for records |
| 5 | **Blobs** (3) | `uploadBlob`, `listBlobs`, `getBlob` | Image/media support |
| 6 | **Sync** (5) | `getRepo`, `getRecord`, `listRepos`, `getRepoStatus`, `subscribeRepos` | Federation, includes WebSocket firehose |
| 7 | **App.Bsky** (2 + proxy) | `getPreferences`, `putPreferences`, `app.bsky.*` proxy | User preferences and AppView proxy |
| 8 | **OAuth** (9) | `protected-resource`, `authorization-server`, `jwks`, `par`, `authorize` (GET/POST), `token`, `passkeyauthenticationoptions`, `authenticatepasskey` | Full OAuth flow for client apps |

**Source code examples:**

- [server.rs](/src/pds/server.rs) — PDS HTTP server setup and route registration
- [xrpc/](/src/pds/xrpc/) — all XRPC endpoint implementations
- [auth/jwt.rs](/src/pds/auth/jwt.rs) — JWT token creation and verification
- [auth/password.rs](/src/pds/auth/password.rs) — password hashing
- [auth/signer.rs](/src/pds/auth/signer.rs) — repo commit signing (P-256 ECDSA)
- [user_repo.rs](/src/pds/user_repo.rs) — write operations on the user's repo (create/update/delete records)
- [blob_db.rs](/src/pds/blob_db.rs) — blob storage management
- [firehose_event_generator.rs](/src/pds/firehose_event_generator.rs) — generating firehose events from repo changes
- [oauth/](/src/pds/oauth/) — full OAuth2 implementation

**Instructions:**

Work through the groups in order. Each group builds on the previous one. Start with foundation endpoints to prove the server works, 
then add authentication so you can test the rest, then implement the core repo operations, and so on.

&nbsp;

---

## Step 10: Background jobs

Some operations should happen in the background. Background jobs handle deferred work, like cleaning up expired sessions or old firehose events.

**T-shirt size:** Small

**Coding concepts:**

- Async task scheduling
- Shared state between HTTP handlers and background workers

**Source code examples:**

- [background_jobs.rs](/src/pds/background_jobs.rs) — async background task processing

**Instructions:**

Implement a background job system that can run tasks on a schedule or in response to events, without blocking the main HTTP request/response cycle.

