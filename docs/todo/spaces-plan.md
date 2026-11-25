# Ten Steps to Spaces in rustproto

Bring rustproto's existing Spaces prototype up to the **November 24, 2026
Spaces alpha**, starting with small, independently testable libraries and ending
with an interoperable PDS. Like `docs\articles\ten-steps-to-a-pds.md`, each step
has reading, coding concepts, source examples, instructions, and something
concrete to demonstrate when it is finished.

This is an implementation plan, not a claim that the features below exist.
Sizes are relative effort estimates, not calendar commitments. Large steps
should be split into several changes at the checkpoints listed.

## Step 0: Pick the destination and preserve the starting point

**T-shirt size:** Small

**Reading:**

- [Alpha migration guides](https://github.com/bluesky-social/atproto/tree/16bfeba760aede46f0b87bf94febef9e6ea84b95/alpha-migrations):
  [October 1](https://github.com/bluesky-social/atproto/blob/16bfeba760aede46f0b87bf94febef9e6ea84b95/alpha-migrations/prompt-10-1-26.md),
  [October 22](https://github.com/bluesky-social/atproto/blob/16bfeba760aede46f0b87bf94febef9e6ea84b95/alpha-migrations/prompt-10-22-26.md),
  [October 29](https://github.com/bluesky-social/atproto/blob/16bfeba760aede46f0b87bf94febef9e6ea84b95/alpha-migrations/prompt-10-29-26.md),
  [November 5](https://github.com/bluesky-social/atproto/blob/16bfeba760aede46f0b87bf94febef9e6ea84b95/alpha-migrations/prompt-11-5-26.md),
  [November 12](https://github.com/bluesky-social/atproto/blob/16bfeba760aede46f0b87bf94febef9e6ea84b95/alpha-migrations/prompt-11-12-26.md),
  [November 24](https://github.com/bluesky-social/atproto/blob/355b73380411a24c751b9896ee9e17f5b593bca7/alpha-migrations/prompt-11-24-26.md).
- [Spaces proposal at the November 24 URI update](https://github.com/bluesky-social/proposals/blob/1831b5db05b3caeef2397fdafaf3a16d91c98747/0016-permissioned-data/README.md).
- [November 24 alpha reference implementation](https://github.com/bluesky-social/atproto/tree/1f510f8e36a973f6fd055da349b586918805ae66).
- [Ongoing alpha updates](https://discourse.atmosphere.community/t/atproto-spaces-alpha-updates/1129).

**Coding concepts:** Compatibility baselines, fixtures, schema migrations.

**Where rustproto is now (inspected November 15, 2026):**

| Area | Existing foundation | Work still needed for this target |
|---|---|---|
| Space identity | `src\pds\spaces\space_uri.rs` parses and emits legacy space and record URIs | Adopt `at:space/`, strict validation, and migration of persisted references |
| Space configuration | `Space` table; `simplespace.createSpace` and `getSpace` | New policy types, lifecycle methods, members, scopes, and data migration |
| Record storage | `SpaceRepoRecord` stores DAG-CBOR and CIDs; create/put handlers | Set hashes, commits, revisions, oplog, remaining CRUD, reads, and blobs |
| Credentials | Delegation JWTs, DPoP-bound space credentials, auth opt-in | HTTP signatures, current audiences and body fields, shorter expiry, revocations |
| Notifications | Persisted `registerNotify` / `unregisterNotify` intent | Writer sequencing, reliable delivery, authorization-time webhook registration |
| General infrastructure | SQLite, CID/CBOR/varint code, OAuth, DID resolution, background jobs | Reuse selectively without changing public repo behavior |

The existing prototype predates even parts of October 1: it still uses `type`,
a single simplespace `policy` plus `appAccess`, a two-hour credential default,
and DPoP key binding. Do **not** implement each historical release in sequence.
Use the older guides to understand missing mechanisms, but implement the
November 24 behavior directly. The November 12 source links in individual
steps are historical examples; check them against the November 24 reference
before relying on their wire behavior.

**Instructions:**

1. Keep `FeatureEnabled_Spaces` off by default while upgrading. Back up the DB
   and representative existing space records/configurations before migrations.
2. Record a route/field/error checklist from the pinned Lexicons. In particular:
   `authorizeSpaceSync`, `listWriters`, `notifyRepoUpdate`,
   `notifyCredentialsRevoked`, `putMembers`, `removeMembers`, and `checkAccess`
   are the current names. `registerNotify`, `unregisterNotify`, and
   `notifySpaceDeleted` are removed.
3. Use small pure Rust modules for the first five steps, exposed through
   `src\lib.rs`; keep Axum/SQLite orchestration in `src\pds\spaces`.
   Suggested new modules below are proposals, not existing files. Use vetted
   crypto primitives rather than implementing BLAKE3, ECDH, or HKDF ourselves.
4. Capture upstream fixtures and their provenance. Rust self-round-trips alone
   will not establish compatibility with another implementation.

**Done when:** There is a fixed compatibility target, saved legacy fixtures,
and a checklist distinguishing reused code from missing protocol behavior.

**Moving target:** The proposal now specifies `at:space/`; the November 24 alpha
ships it with temporary lenient validation for old URIs embedded in records.
This is still a proposal, and the alpha read compatibility is expected to end
before beta. Recheck the thread before implementation and rollout. For a
JavaScript interoperability client, pin a compatible build such as the
November 24 alpha snapshot `0.0.0-spaces-alpha-20261124235736`.

---

## Step 1: Hash a set of records with LtHash

Start here. No server, database, OAuth session, or network is needed.

**T-shirt size:** Small-Medium

**Reading:**

- Proposal: [Commit digest](https://github.com/bluesky-social/proposals/blob/b54ec6853bab77edd4fd373804a480633094b324/0016-permissioned-data/README.md#commit-digest).
- Upstream: [lthash.ts](https://github.com/bluesky-social/atproto/blob/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/space/src/lthash.ts)
  and [lthash.test.ts](https://github.com/bluesky-social/atproto/blob/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/space/tests/lthash.test.ts).

**Coding concepts:** Fixed-size byte arrays, little-endian integers, wrapping
arithmetic, incremental hashing.

**Source code examples:** `src\repo\cid.rs`; proposed `src\space\lthash.rs`.

**Instructions:**

1. Implement a 2,048-byte state containing 1,024 little-endian `u16` lanes.
   Expand an element with BLAKE3 XOF to 2,048 bytes, then add or subtract its
   lanes with wrapping arithmetic. Serialize explicitly as little-endian.
2. Provide add, remove, state import/export, and SHA-256 digest operations.
   Reject imported states of the wrong length.
3. Keep record-to-element formatting separate: UTF-8 bytes of
   `{collection}/{rkey}/{record_cid}`. Use the canonical CID string, not raw CID
   bytes, CBOR bytes, or JSON.
4. Test upstream vectors, insertion-order independence, add/remove cancellation,
   updates as remove-old/add-new, and persistence round-trips. The repository
   layer must prevent duplicate additions; LtHash does not enforce set membership.

**Done when:** A unit test or small CLI demonstration hashes the same records
in different orders and produces identical state and digest, matching upstream.
The empty digest must be
`e5a00aa9991ac8a5ee3109844d84a55583bd20572ad3ffcd42792f3c36b183ad`.

---

## Step 2: Describe a space repo without a server

**T-shirt size:** Small-Medium

**Reading:** Proposal sections on addressing, space repos, and commit digest;
upstream [repo-commit.ts](https://github.com/bluesky-social/atproto/blob/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/space/src/repo-commit.ts).

**Coding concepts:** Typed identifiers, canonical encoding, state transitions.

**Source code examples:** `src\pds\spaces\space_uri.rs`,
`src\repo\dag_cbor.rs`, `src\repo\cid.rs`, and
`src\pds\user_repo.rs` for existing JSON/CBOR conversion.

**Instructions:**

1. Replace the legacy parser/formatter with validated space references
   `at:space/{spaceDid}/{spaceType}/{skey}` and record URIs
   `at:space/{spaceDid}/{spaceType}/{skey}/{authorDid}/{collection}/{rkey}`.
   Both DIDs must be DIDs, not handles; reject partial/extra paths and invalid
   NSIDs, keys, or DIDs. Both forms are `at-uri`; only the first is `space-ref`.
   Keep public `at://` URIs unchanged and adapt shared `at-uri` validation.
   Keep library code independent of PDS state.
2. Define commit `{ ver: 1, repo, space, rev, data }` and operation
   `{ rev, collection, rkey, cid, prev }` models. `data` is 32 bytes, `cid` is
   null for deletion, and `prev` is null for creation.
3. Build a pure repo-state helper that folds records/operations through Step 1
   and produces a commit. Multiple operations may share one revision.
4. Check existing DAG-CBOR/CID handling against canonical DRISL/DASL requirements,
   including byte strings and links. Reuse compatible behavior; add focused
   adapters or validation where necessary rather than rewriting the public repo.
5. Test both URI lengths and `did:web` examples, malformed parts, handles,
   partial paths, strict rejection of old forms, and unchanged public URIs.
   For alpha-era record values only, provide a temporary lenient reader that
   recognizes and normalizes valid old references; never use it for new input.
   Also test create/update/delete folding against full recomputation, wrong
   commit version, and mismatched repo/space.

**Done when:** An in-memory record set becomes a canonical commit with the same
`data` as upstream. A space commit has **no embedded signature**, `ikm`, or `mac`;
a matching digest establishes consistency, not authenticity.

---

## Step 3: Read and write STAR archives

**T-shirt size:** Medium

**Reading:**

- [STAR specification](https://dasl.ing/star.html) and
  [DRISL](https://dasl.ing/drisl.html).
- November 5 guide; proposal sections on repo serialization and space meta archives.
- Upstream [STAR implementation and interop fixtures](https://github.com/bluesky-social/atproto/tree/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/star).

**Coding concepts:** Streaming I/O, varints, bounded parsing, canonical ordering.

**Source code examples:** `src\repo\varint.rs`, `src\repo\dag_cbor.rs`;
proposed generic `src\star` and Spaces-specific archive adapters.

**Instructions:**

1. Implement generic streaming STAR framing: magic `2a 6c 01 00`, length-prefixed
   metadata, `0x01` before each key/value pair, and a final `0x00`.
   Reject truncation, invalid controls, and bytes after the terminator.
2. Add the `atproto-space-repo` profile: commit metadata, strictly increasing
   record-path keys, and canonical record bytes. Recompute CIDs and the set
   hash while reading; do not accept duplicate/out-of-order keys or bad digests.
3. Enforce profile limits before allocating: 64 KiB metadata, 4 KiB keys, and
   1 MiB values. Keep those separate from generic STAR framing.
4. Separate structural/digest verification from authenticated download
   verification; the latter gains response-MAC verification in Step 5.
5. Add the `atproto-space-meta` profile envelope now, with policy/state/writer
   validation completed alongside Steps 7 and 10. Use `repoRev`, not the old
   writer field `rev`. Serve/import both profiles as `application/vnd.dasl.star`.

**Done when:** Rust reads upstream valid fixtures and rejects invalid fixtures;
upstream reads a Rust-written empty and populated repo. An offline inspect tool
can print metadata and records and report digest consistency without claiming
the archive is authenticated. Do not support the obsolete pre-November-5 framing.

---

## Step 4: Sign HTTP messages

**T-shirt size:** Medium-Large

**Reading:** [RFC 9421](https://www.rfc-editor.org/rfc/rfc9421),
[RFC 9651 structured fields](https://www.rfc-editor.org/rfc/rfc9651), and
November 12 guide section 5; upstream
[http-signature.ts](https://github.com/bluesky-social/atproto/blob/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/space/src/http-signature.ts).

**Coding concepts:** Structured headers, canonical signature bases, ECDSA.

**Source code examples:** `src\pds\auth\signer.rs` for key encoding;
proposed pure HTTP-signature helpers. Leave `src\pds\oauth\dpop.rs` intact.

**Instructions:**

1. Evaluate Rust RFC 9421/structured-field libraries against the exact profiles
   before choosing dependencies. Implement only missing profile glue.
2. Support signing and verification of the credential profile covering exactly
   `authorization` then `atproto-space-audience`. Select exactly one signature by
   `tag="atproto-space-credential"`, under any label, not a hard-coded label.
3. Use P-256/SHA-256 and 64-byte `r || s` signatures; accept high-S and low-S,
   reject DER, check optional `alg` and `keyid`, and include supplied signature
   parameters in the signed base even when their semantics are ignored.
4. Preserve the Authorization value exactly for signing. Recognize the
   `Atproto-Space-Credential` scheme case-insensitively and reject duplicate
   authorization/audience fields. The expected audience comes from the operation:
   repo DID for repo reads, authority DID for host reads.
5. Add the HMAC-SHA256/structured-dictionary capability needed by Step 5.
   Test arbitrary labels, ambiguous tags, altered covered headers, optional
   parameters, and standard-base64 interoperability.

**Done when:** Rust verifies upstream credential signatures and vice versa,
without running a PDS. Ordinary OAuth continues to use DPoP; only Spaces
credential use changes to HTTP Message Signatures.

---

## Step 5: Establish shared secrets and authenticate responses

**T-shirt size:** Medium-Large

**Reading:** [RFC 9180 secret export](https://www.rfc-editor.org/rfc/rfc9180#section-6.2),
November 12 guide section 6, and the proposal's shared-secret/MAC sections and
non-HPKE appendix. Upstream:
[kem.ts](https://github.com/bluesky-social/atproto/blob/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/space/src/kem.ts)
and [mac.ts](https://github.com/bluesky-social/atproto/blob/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/space/src/mac.ts).

**Coding concepts:** HPKE export, key agreement, HKDF, authenticated metadata.

**Source code examples:** Existing multikey handling in
`src\pds\auth\signer.rs`; proposed `src\space\kem.rs` and `mac.rs`.

**Instructions:**

1. Choose an HPKE library supporting base-mode secret export with P-256 DHKEM
   (`0x0010`), HKDF-SHA256 (`0x0001`), and export-only AEAD (`0xffff`).
   Use `info="atproto-mac"`, empty exporter context, and 32 exported bytes.
   If unavailable, implement the proposal's equivalent composition using vetted
   ECDH/HKDF primitives and prove equivalence with RFC vectors.
2. Convert ephemeral public keys between compressed `did:key` wire form and
   uncompressed HPKE form. Use a fresh ephemeral key/secret on every request,
   including retries and pagination. Reject malformed or wrong-curve keys.
3. Implement `Accept-Signature` requests and tagged response MACs for
   `atproto-space-commit` and `atproto-space-record`. These use Step 4's HTTP
   signature base, not the superseded custom MAC headers/context construction.
4. Require the exact profile, covered structured header, and matching optional
   key ID. Verify commit headers against the body/STAR metadata and requested
   repo/space; verify record URI and recomputed CID against the record header.
5. Test RFC export vectors, upstream cross-language vectors, both MAC profiles,
   wrong keys, altered bodies/headers, and missing/duplicate profile requests.

**Done when:** A local sender/receiver fixture authenticates a commit, record,
and STAR snapshot end to end. These HMACs are deliberately not transferable
proof of authorship. `InvalidSpaceMac` is the current protocol error.

**Library milestone:** Steps 1-5 can land without changing a route or live
database. They supply useful standalone tools and the wire-format foundation
for every server step below.

---

## Step 6: Give stored space records real repo state

**T-shirt size:** Large

**Reading:** Proposal sections on repo lifecycle, incremental sync, blob sync,
and the current repo-method Lexicons.

**Coding concepts:** SQLite transactions, operation logs, concurrency, recovery.

**Source code examples:** `src\pds\db\pds_db.rs`,
`src\pds\db\entities.rs`, `src\pds\spaces\create_space_record.rs`,
`src\pds\spaces\put_space_record.rs`, `src\pds\user_repo.rs`,
`src\pds\blob_db.rs`, and `tests\pds_db_tests.rs`.

**Instructions:**

1. Add versioned schema migrations for repo metadata (revision/full LtHash
   state), ordered operations, blob associations, and pending write notices.
   rustproto is single-account: the author is currently implicit in the DB.
   Keep `(author DID, space)` explicit in the model without adding multi-user hosting.
2. Atomically and repeatably migrate old URI keys to the new form in `Space`,
   `SpaceRepoRecord`, and any retained `SpaceNotifyRegistration` state, plus
   other owned indexes, caches, or queues that actually store references. Detect
   malformed values and collisions before changing state; fail without dropping
   data. Use a migration-only legacy parser, not lenient protocol validation.
   Migrate extracted backlink/reference indexes if present. Do not convert
   obsolete registrations into confirmed webhooks.
3. Backfill state from each space's existing canonical record bytes/CIDs.
   Create valid initial revisions; do not invent historical oplog entries.
   Unknown/inconsistent stored bytes must fail migration rather than be dropped.
   Do not rewrite URI strings inside record values: that changes their CIDs.
   Normalize embedded old references when comparing or indexing alpha records,
   while keeping their original bytes. Record CIDs and commit `data` do not
   change merely because a space URI changes.
4. In one transaction, update records, subtract old/add new elements, advance
   the repo TID, append operations, update blob references, and persist the
   outgoing notice. Serialize concurrent writes so no hash/revision is lost.
5. Complete `createRecord`, `putRecord`, `deleteRecord`, `applyWrites`, and
   `clearRecords`, including conditional-write fields defined by the Lexicons.
   First write implicitly creates a repo, not a space. If this account is the
   authority, the space must exist; remote-authority writes need no local space row.
6. Add own-account reads: `getRecord`, `listRecords`, `getLatestCommit`,
   `getRepo`, `listRepoOps`, `getBlob`, and `listBlobs`. A commit is included on
   every oplog page; a full page has a cursor even if the next page is empty.
   Inline only current record values. Do not add removed `reverse`/`since`
   parameters to `listRecords`/`listBlobs`.
7. Keep these records out of the public MST, public CAR exports, and firehose.
   Blob access must check references in the requested space; knowing a CID is
   not authorization. Audit public blob paths for private-only blob exposure.

Scope enforcement is a prerequisite for exposing these routes; build/test the
storage engine first and wire it publicly only with Step 7.

**Done when:** Transaction rollback and concurrent-write tests preserve
records/hash/oplog/outbox consistency. A restart preserves state; recomputation
matches incremental hashes; space data does not leak through public exports.
An upgrade preserves record bytes/CIDs, finds records by new URI, survives a
retry, and fails safely on invalid or conflicting stored keys.

**Checkpoints:** schema/backfill; transactional mutations; own-account reads and
blobs. Clearing advances the revision and clears the oplog, preserves
system-published configuration records, and does not delete the space.

---

## Step 7: Create spaces with current policies and OAuth permissions

**T-shirt size:** Large

**Reading:** October 29 and November 12 policy changes; proposal sections on
baseline policies, published records, and OAuth scopes.

**Coding concepts:** Policy evaluation, permission matching, batched updates,
safe configuration migration.

**Source code examples:** `src\pds\spaces\create_space.rs`,
`src\pds\spaces\get_space.rs`, `src\pds\oauth\scope_resolution.rs`,
`src\pds\oauth\authorize_get.rs`, `src\pds\admin\spaces.rs`.

**Instructions:**

1. Replace simplespace configuration with the three required baseline policies:
   `com.atproto.personalspace.policy`, `com.atproto.memberspace.policy`, and
   `com.atproto.managedspace.policy`. Keep read and write decisions independent.
   The authority's own account is allowed to read/write under every policy.
2. Migrate the actual legacy single-policy plus `appAccess` rows rustproto
   stores, not upstream's later `readPolicy`/`writePolicy` schema. Map public and
   member-list configurations to memberspace, and managing-app configurations
   to managedspace. Preserve allow-lists where equivalent; require an explicit
   decision for non-equivalent configurations instead of broadening access.
3. Implement `com.atproto.space.createSpace`, `updateSpace`, `getSpace`, and
   `deleteSpace`; use `spaceType` input and `space` output. `getSpace` is now
   an owner-only session method (`read_self` for OAuth), not a space-credential
   method.
   Update the admin UI and callers with the API/schema changes.
4. Add memberspace `putMembers`, `removeMembers`, and `listMembers`.
   Mutations accept at most 100 entries atomically; duplicate puts use the last
   entry, removing a non-member is a no-op, and policy mismatches are errors.
   Apply writer-removal side effects where required and sequence them in Step 9.
5. Implement managedspace `checkAccess` calls with optional `repo` and attested
   `clientId`, returning both read/write decisions. Require a `did#serviceId`
   managing app, validate service auth audiences/`lxm`, and use SSRF-safe fetching.
   Resolution failures, errors, and the 10-second timeout produce
   `UpstreamFailure`, not an access denial.
6. Enforce `space:` scope matching at every handler: authority/type/key,
   record actions/collections, and separate management grants. `read_self`
   cannot mint delegation tokens; `read` includes it. `repo:` or generic OAuth
   authentication alone must not grant space access. Default authority is `self`.
   Support named `spaceType` and positional syntax, permission-set expansion,
   and space declarations' optional `title` on consent screens.
7. Respect declaration-based default collections, including their specified
   dynamic update behavior; do not freeze those defaults merely because
   permission-set expansion currently occurs at session creation. Collections
   in published space type declarations must resolve to record Lexicons.
8. Maintain published policy/member records in the authority's space repo using
   the transactional engine. Reject direct CRUD of reserved configuration
   collections, withdraw records when flags clear, and discard old policy state
   on policy-type changes without silently rebuilding the writer set.

**Done when:** Personal, member-only, public-read/public-write, allowed-app, and
managed spaces pass focused allow/deny tests. Test narrow scopes, owner
exceptions, policy replacement, publications, and legacy migration with saved
data. Reject app-password sessions, including privileged app passwords, for
account-authenticated space methods with `InvalidToken` / `Bad token scope`;
full account sessions and appropriately scoped OAuth remain supported.

**Checkpoints:** scope matcher; policy migration/lifecycle; membership; managed
checks; published records. A member list is not the writer set, and accepting a
user's own write is distinct from the authority agreeing to track that writer.

---

## Step 8: Issue credentials and serve authenticated private reads

**T-shirt size:** Large

**Reading:** Credential sections of all migration guides, with November 24
taking precedence; proposal sections on token types, key binding, and revocation.

**Coding concepts:** JWT claims, replay protection, DID key resolution,
proof of possession, credential lifecycle.

**Source code examples:** `src\pds\spaces\get_delegation_token.rs`,
`src\pds\spaces\get_space_credential.rs`, `src\pds\auth\auth_flow.rs`,
`src\pds\auth\signer.rs`, `src\pds\installer.rs`.

**Instructions:**

1. Update delegation/attestation audiences to `<authority DID>#atproto_pds`.
   Resolve authorities through `#atproto_pds` and verify their JWTs with
   `#atproto`; remove dedicated space-host key/service assumptions.
   Preserve P-256 and secp256k1 JWT verification. Credential binding and HPKE,
   in contrast, specifically require P-256 keys.
2. Replace `getSpaceCredential` with `authorizeSpaceSync`, accepting
   `{ space, credentialKey, delegationToken?, clientAttestation?, notify? }`.
   Return `{ credential, spaceRev? }`. Assertions belong in the body, not the
   Authorization header. Validate supplied assertions even when optional, retain
   delegation replay prevention, and support public access according to policy.
3. Bind JWTs with `cnf.kid`, not `cnf.jkt`; keep their credential `typ`, require
   `kid="#atproto"`, and do not add a JWT audience. Default lifetime is 600
   seconds, maximum 3,600, with validated numeric times and a nonempty `jti`.
   Apply the specified skew rules. Confirm deletion before authorization checks;
   preserve private-space existence hiding and current error names.
4. Replace only the SpaceCredential auth path with Step 4. Keep acceptance
   opt-in per endpoint, validate the credential's space against the requested
   space, and derive the correct audience DID. Reads accept a full own-account
   session, covering OAuth, or a space credential; account mutations accept a
   full session or covering OAuth, never an app-password session.
5. Provision a separate persistent P-256 `#atproto_kem` key for this account,
   publish it in DID documents/recommended credentials, and retain its private
   key on the repo host. Backfill existing accounts safely, including a manual
   did:web path; do not reuse the account signing key.
6. Wire Step 5 response MACs into `getLatestCommit`, `getRepo`, `listRepoOps`,
   and `getRecord`. Require valid profile requests for space-credential callers;
   do not require MACs for own-account OAuth reads. Set `Vary: Accept-Signature`
   and expose covered/signature headers through CORS.
7. Receive service-authenticated `notifyCredentialsRevoked`; persist
   `(space, jti)` idempotently and reject revoked credentials. Retain revocations
   for the maximum credential lifetime plus skew (3,610 seconds with 5-second
   skew on each side). Early authority fanout is optional; receipt/enforcement
   by a repo host is not.

The optional `notify` branch depends on Step 9's confirmation/registration
implementation; do not silently accept it before it works.
Issue tokens with new-form space subjects and reject old-form subjects at
protocol boundaries. Existing signed tokens cannot be rewritten: obtain new
delegations and credentials during the coordinated peer upgrade.

**Done when:** An independent client obtains a credential, signs a read, and
verifies the response MAC. Bad space/audience/key/expiry/replay/revocation cases
fail correctly, while existing OAuth DPoP behavior remains unchanged.

**Checkpoints:** tokens; credential auth; DID/KEM provisioning; MAC-protected
reads; revocation receipt. Do not advertise complete compatibility yet.

---

## Step 9: Connect writers, authorities, and syncers

**T-shirt size:** Extra Large

**Reading:** November 12 notification and writer-lifecycle changes; proposal
sections on sync, writer sets, and space deletion. Upstream examples:
[space tests](https://github.com/bluesky-social/atproto/tree/d4def6e9f521896ed8222f576d54923d00b5fbeb/packages/pds/tests/space).

**Coding concepts:** Transactional outbox, retries, monotonic sequencing,
webhook confirmation, durable checkpoints.

**Source code examples:** `src\pds\background_jobs.rs`,
`src\pds\spaces\register_notify.rs`, `src\pds\spaces\unregister_notify.rs`,
`src\pds\server.rs`, and the Step 6 state/outbox.

**Instructions:**

1. Deliver repo-host `notifyRepoUpdate { space, repo, repoRev, data }` using the
   writer's service auth and the authority's resolved PDS. Coalesce pending work
   to the newest revision per repo/space; persist retries with backoff and
   permanent/transient failure handling. A crash after a committed write must
   not lose its pending notice. Local-authority writes take the same state path.
2. At the authority, authorize the writer, ignore stale/duplicate `repoRev`,
   reject excessively future revisions, and atomically update writer metadata
   with a strictly increasing space TID. Sequence removals as well as writes.
3. Implement `listWriters` using exclusive space-revision cursors and typed
   `repoWrite` / `repoRemoved` entries. Cursors are opaque strings on the wire,
   not newly constrained TID fields. Return latest writer states, not a history;
   page until empty, and never move a checkpoint backward.
4. Implement `removeWriter` and the required membership side effects. Keep
   removed entries visible until re-added or safely collected; an authorized
   later write may re-add them. Removing a writer does not delete their records.
5. Replace old registrations with `authorizeSpaceSync.notify { endpoint, token }`.
   Confirm new registrations via `confirmRegistration` challenge echo, key them
   by space/endpoint/token, and renew through reauthorization. Bound token size,
   protect tokens from logs, and fetch untrusted endpoints with SSRF protection.
   Do not turn old service-ID registrations into unconfirmed webhooks.
6. Send token-authenticated `notifySpaceUpdate`, not service-authenticated
   forwarded writes. Use the exact update union: shared `defs#repoWrite` and
   `defs#repoRemoved`, plus `notifySpaceUpdate#registrationExpired` and
   `#spaceDeleted`. Writes/removals are best effort; expiry/deletion notices
   need durable retries for at least 72 hours, including lapsed registrations.
   Retire registrations on permanent rejection/revocation/denied reauthorization,
   but not simply because the authority is suspended.
7. Add a minimal syncer CLI/test harness, not a production AppView. Discover
   writers, pull from their repo hosts, verify MACs and hashes, and persist safe
   checkpoints. Apply oplog pages through the terminal page and recover with
   STAR replacement on digest mismatch, including removal of absent records.
   Normalize legacy embedded references before comparison or indexing without
   modifying record bytes; use the new form for all protocol requests and state.
   Handle duplicates, gaps, concurrent writes, cleared repos, and missing repos.
8. Exercise registration renewal, stale credentials, key changes, and authority
   migration. Reauthorize at the new PDS; refresh DID documents and retry with
   fresh ephemeral keys after MAC failures. A checkpoint older than 72 hours
   requires a full writer-set reconciliation, not just cursor catch-up.
9. Persist deletion state and delivery work. A deletion notice prompts
   reauthorization; only confirmed `SpaceDeleted` authorizes the syncer to purge
   its space copy/derived data. Authority deletion clears its own repo, not
   every member's records. Ordinary outages and suspension are not deletion.

**Done when:** Separate writer, authority, and syncer processes converge after
missed notifications and restarts. Include a reference-PDS peer in both
directions so a shared Rust-side mistake cannot make the test pass.

**Checkpoints:** repo outbox delivery; sequenced writer listing/removal;
confirmed registrations; expiry/deletion delivery; sync/recovery harness.
Receiving syncer webhooks belongs to the harness/application, not automatically
to every PDS. That role distinction should stay visible in the route checklist.

---

## Step 10: Move accounts safely and make the feature operable

**T-shirt size:** Large

**Reading:** October 22 key lifecycle, October 29 account migration, November 5
takedowns, and November 12/24 archive and lifecycle changes; proposal sections on
account migration, moderation, and application access control.

**Coding concepts:** Validated import, atomic replacement, key lifecycle,
operational recovery.

**Source code examples:** `src\pds\installer.rs`, `src\pds\db\pds_db.rs`,
`src\pds\auth\signer.rs`, `src\pds\admin\spaces.rs`,
`src\pds\background_jobs.rs`, and Steps 3/6 archive and transaction helpers.

**Instructions:**

1. Implement paginated `listSpaces`, including both authority-owned spaces and
   contributed repos. Use `spaceDid`/`spaceType` filters and current entries
   `{ space, repoRev?, data?, spaceRev? }`, with covering OAuth permissions.
2. Finish `exportSpaceMeta`, `importSpaceMeta`, and `importRepo`. Validate
   profile/version/identity, canonical encoding, ordering, limits, CIDs, hashes,
   and all grants before atomically replacing state. Space-meta sections are
   policy, policy-defined state, then writers; writer values use `repoRev`.
   Reject unknown policy-state keys rather than silently dropping access state.
   Do not export removed writers. Require full space access for `importRepo`:
   `read` plus `create`, `update`, and `delete` on every collection for OAuth.
   Release blob references for removed records.
3. Migrate hosted space metadata before the authority's own repo, then repos
   and referenced blobs. Imports preserve data but give repos new revisions and
   reset their oplogs; metadata preserves writer space revisions. Imports must
   be retryable and must not partially apply an out-of-scope archive.
4. Carry `atproto_kem` through recommended credentials and PLC submission checks.
   Verify sync after key/host changes; explicitly handle private-key provisioning
   and publication rather than assuming old credentials/registrations transfer.
   Registrations, revocations, and deletion tombstones are not migration exports.
   For activation without an entryway, check the published key matches the
   hosted key; `did:web` may omit it. If supporting an entryway that constructs
   a genesis PLC operation, implement `reserveKemKey` (`{ did? }` to
   `{ kemKey }`) so it can publish the reserved key before account creation.
5. Apply account-status semantics: suspended authorities issue no credentials
   or writer listings, but keep accepting writer updates/registrations as
   specified. Suspended accounts cannot manage/write; authorized takedown
   sessions can still export their own data. Do not confuse these with deletion.
6. Add operational visibility for pending notices, retry age, registrations,
   revoked credentials, migration failures, and KEM publication readiness.
   Never log tokens, private keys, or private record contents.
7. Test a real legacy rustproto DB upgrade and rollback/restore procedure.
   Use rustproto schema versions, not upstream Node PDS migration numbers.
   Remove obsolete routes when switching the feature to the new protocol.
   Old-form archive metadata is not accepted by upgraded hosts; re-export from
   an upgraded source rather than silently importing or rewriting an archive.
8. Only after end-to-end checks, expose the alpha capability indicator used by
   `describeServer` and document the supported compatibility revision. Keep
   public broadcast behavior unchanged and the feature explicitly opt-in.

**Done when:** An account with owned and remote spaces migrates between hosts
without losing policies, members, writer discovery, records, or blobs; peers
resume sync. A malformed import leaves the destination unchanged.

**Checkpoints:** export profiles; validated imports; identity/key migration;
status handling; upgrade/restore and interoperability sign-off.

---

## Implementation order and validation

| Milestone | Steps | Observable result |
|---|---|---|
| Offline library toolbox | 1-5 | Hash records, inspect STAR, interoperate on signatures and MACs |
| Own-account local Spaces | 6-7 | Persistent repos, complete CRUD, current policies and scoped OAuth |
| Cross-account reads | 8 | Current credentials, published KEM keys, authenticated private responses |
| Federated Spaces | 9 | Writer discovery, reliable host delivery, webhook hints, recoverable sync |
| Alpha-ready PDS | 10 | Migration, account status, upgrade/restore, interoperability |

The default order is sequential. Actual prerequisites: Step 2 uses 1; Step 3
uses 1-2; Step 4 is independent; Step 5 uses 2-4; Step 6 uses 1-3; Step 7 uses
6; Step 8 uses 4-7; Step 9 uses 6-8; Step 10 integrates 3 and 6-9.
The Step 8 notification option is completed in Step 9. These are delivery
checkpoints, not reasons to activate an incomplete protocol halfway through.

For each implementation change, run the smallest relevant `cargo test` filter
first. Reuse inline unit tests and the existing DB tests; add tests only for
the behavior being introduced. Run the existing project formatting/lint checks
where applicable, then the full `cargo test` suite before enabling the feature.
Store deterministic interop fixtures with provenance; keep live peers optional
for routine unit runs. For final sign-off, test Rust-to-reference and
reference-to-Rust with independent authority/repo-host identities.

Final regression gates: public repo/OAuth behavior unchanged; no private data
in public exports or blob reads; no writes beyond granted space scopes; legacy
DB upgrade preserves content/access intent; interrupted delivery and sync heal;
invalid archives and MACs never become accepted state.

URI cutover gate: all hosts and peers use new-form parameters, token subjects,
MAC headers, STAR metadata, and returned record URIs; strict boundaries reject
old forms. Alpha record values remain byte-for-byte intact and are normalized
only when read or indexed. Coordinate peer upgrades and reissue credentials;
pre-upgrade archives require re-export, while existing record CIDs and set
hashes do not require a resync solely due to the URI change.

**Intentionally outside the first compatibility target:** a production syncer
or AppView, a Spaces UI, encrypted-at-rest/E2EE features, relays for private data,
general-purpose multi-account hosting, and a new labeler/moderation product.
The proposal's moderation and application access-control guidance still applies:
a space credential grants a service whole-space access, not permission to show
that data to every one of its users.
