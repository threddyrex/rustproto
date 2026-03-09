# XRPC Endpoint Reference

Quick reference for all XRPC endpoints supported by rustproto.

---

## 1. Foundation

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/_health` | GET | Returns server health status. Used by monitoring and load balancers. |
| `/hello` | GET | Simple test endpoint to verify the server is running. |
| `/xrpc/com.atproto.server.describeServer` | GET | Returns server metadata including available features, DID, and invite code configuration. |

## 2. Identity

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/com.atproto.identity.resolveHandle` | GET | Resolves an AT Protocol handle to its corresponding DID. |

## 3. Authentication (Sessions)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/com.atproto.server.createSession` | POST | Authenticates a user with identifier and password, returning access and refresh tokens. |
| `/xrpc/com.atproto.server.getSession` | GET | Returns info about the current authenticated session (DID, handle, email). |
| `/xrpc/com.atproto.server.refreshSession` | POST | Exchanges a refresh token for a new access/refresh token pair. |
| `/xrpc/com.atproto.server.getServiceAuth` | GET | Creates a short-lived service authentication token for inter-service calls. |
| `/xrpc/com.atproto.server.checkAccountStatus` | GET | Returns the current status of the authenticated account (active, deactivated, etc.). |
| `/xrpc/com.atproto.server.activateAccount` | POST | Reactivates a previously deactivated account. |
| `/xrpc/com.atproto.server.deactivateAccount` | POST | Deactivates the authenticated account, making it temporarily inaccessible. |

## 4. Repo Operations (Core CRUD)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/com.atproto.repo.describeRepo` | GET | Returns metadata about a repo including DID, handle, and collections. |
| `/xrpc/com.atproto.repo.getRecord` | GET | Retrieves a single record by repo, collection, and rkey. |
| `/xrpc/com.atproto.repo.listRecords` | GET | Lists records in a collection with cursor-based pagination. |
| `/xrpc/com.atproto.repo.createRecord` | POST | Creates a new record in the specified collection. Returns the record URI and CID. |
| `/xrpc/com.atproto.repo.putRecord` | POST | Creates or updates a record at a specific rkey. Used for singleton records like profiles. |
| `/xrpc/com.atproto.repo.deleteRecord` | POST | Deletes a record by repo, collection, and rkey. |
| `/xrpc/com.atproto.repo.applyWrites` | POST | Applies multiple create/update/delete operations atomically in a single transaction. |

## 5. Blobs

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/com.atproto.repo.uploadBlob` | POST | Uploads a blob (image, video, etc.) and returns a blob reference for use in records. |
| `/xrpc/com.atproto.sync.listBlobs` | GET | Lists all blob CIDs stored for a given DID, with optional cursor pagination. |
| `/xrpc/com.atproto.sync.getBlob` | GET | Downloads a blob by DID and CID. Returns the raw blob bytes. |

## 6. Sync (Federation)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/com.atproto.sync.getRepo` | GET | Exports a full repo as a CAR file for federation and backup. |
| `/xrpc/com.atproto.sync.getRecord` | GET | Retrieves a single record as a CAR file, including its Merkle proof. |
| `/xrpc/com.atproto.sync.listRepos` | GET | Lists all hosted repos on this PDS with cursor-based pagination. |
| `/xrpc/com.atproto.sync.getRepoStatus` | GET | Returns the sync status of a repo (active, deactivated, taken down). |
| `/xrpc/com.atproto.sync.subscribeRepos` | WebSocket | Real-time firehose of repo commits. Used by relays and other downstream consumers. |

## 7. App.Bsky (User Preferences)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/xrpc/app.bsky.actor.getPreferences` | GET | Retrieves the authenticated user's saved preferences (feeds, content filters, etc.). |
| `/xrpc/app.bsky.actor.putPreferences` | POST | Saves updated user preferences. |
| `/xrpc/app.bsky.*` (fallback) | * | All other `app.bsky.*` calls are proxied to the configured AppView service. |
| `/xrpc/chat.bsky.*` (fallback) | * | All `chat.bsky.*` calls are proxied to the configured AppView service. |

## 8. OAuth

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/.well-known/oauth-protected-resource` | GET | OAuth protected resource metadata (RFC 9728). Declares the authorization server. |
| `/.well-known/oauth-authorization-server` | GET | OAuth authorization server metadata (RFC 8414). Advertises supported grants and endpoints. |
| `/oauth/jwks` | GET | Publishes the server's public JSON Web Key Set for token signature verification. |
| `/oauth/par` | POST | Pushed Authorization Request (RFC 9126). Client pre-registers an authorization request. |
| `/oauth/authorize` | GET | Renders the authorization/login page for the end user. |
| `/oauth/authorize` | POST | Processes the authorization form submission (credential validation). |
| `/oauth/token` | POST | Token endpoint. Exchanges authorization codes or refresh tokens for access tokens. |
| `/oauth/passkeyauthenticationoptions` | POST | Returns WebAuthn options for initiating passkey-based authentication. |
| `/oauth/authenticatepasskey` | POST | Validates a passkey authentication assertion and completes the OAuth flow. |
