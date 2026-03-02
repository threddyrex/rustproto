# XRPC Endpoints Porting Plan

Porting XRPC endpoints from dnproto to rustproto.

Reference: `dnproto/src/pds/xrpc/` and `dnproto/src/pds/Pds.cs` (MapEndpoints method)

## Status Legend
- ⬜ Not started
- 🟡 In progress
- ✅ Complete

---

## 1. Foundation (Start Here)
Essential for basic server operation:

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ✅ | `/xrpc/_health` | GET | Health check |
| ✅ | `/hello` | GET | Simple test endpoint |
| ✅ | `/xrpc/com.atproto.server.describeServer` | GET | Server metadata |

## 2. Identity

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ✅ | `/xrpc/com.atproto.identity.resolveHandle` | GET | Handle resolution |

## 3. Authentication (Sessions)
Core auth before anything else works:

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ✅ | `/xrpc/com.atproto.server.createSession` | POST | Login |
| ✅ | `/xrpc/com.atproto.server.getSession` | GET | Get current session |
| ✅ | `/xrpc/com.atproto.server.refreshSession` | POST | Refresh tokens |
| ✅ | `/xrpc/com.atproto.server.getServiceAuth` | GET | Service authentication |
| ✅ | `/xrpc/com.atproto.server.checkAccountStatus` | GET | Account status |
| ✅ | `/xrpc/com.atproto.server.activateAccount` | POST | Activate account |
| ✅ | `/xrpc/com.atproto.server.deactivateAccount` | POST | Deactivate account |

## 4. Repo Operations (Core CRUD)

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ✅ | `/xrpc/com.atproto.repo.describeRepo` | GET | Repo metadata |
| ✅ | `/xrpc/com.atproto.repo.getRecord` | GET | Read single record |
| ✅ | `/xrpc/com.atproto.repo.listRecords` | GET | List records |
| ✅ | `/xrpc/com.atproto.repo.createRecord` | POST | Create record |
| ✅ | `/xrpc/com.atproto.repo.putRecord` | POST | Update record |
| ✅ | `/xrpc/com.atproto.repo.deleteRecord` | POST | Delete record |
| ✅ | `/xrpc/com.atproto.repo.applyWrites` | POST | Batch operations |

## 5. Blobs

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ✅ | `/xrpc/com.atproto.repo.uploadBlob` | POST | Upload blob (async) |
| ✅ | `/xrpc/com.atproto.sync.listBlobs` | GET | List blobs (async) |
| ✅ | `/xrpc/com.atproto.sync.getBlob` | GET | Download blob (async) |

## 6. Sync (Federation)

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ⬜ | `/xrpc/com.atproto.sync.getRepo` | GET | Export full repo (async) |
| ⬜ | `/xrpc/com.atproto.sync.getRecord` | GET | Get single record as CAR (async) |
| ⬜ | `/xrpc/com.atproto.sync.listRepos` | GET | List all repos |
| ⬜ | `/xrpc/com.atproto.sync.getRepoStatus` | GET | Repo status |
| ⬜ | `/xrpc/com.atproto.sync.subscribeRepos` | GET | **WebSocket** firehose |

## 7. App.Bsky (User Preferences)

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ⬜ | `/xrpc/app.bsky.actor.getPreferences` | GET | Get user prefs |
| ⬜ | `/xrpc/app.bsky.actor.putPreferences` | POST | Set user prefs (async) |
| ⬜ | **Fallback** `/xrpc/app.bsky.*` | * | Proxy to AppView |

## 8. OAuth

| Status | Endpoint | Method | Notes |
|--------|----------|--------|-------|
| ⬜ | `/.well-known/oauth-protected-resource` | GET | OAuth metadata |
| ⬜ | `/.well-known/oauth-authorization-server` | GET | OAuth AS metadata |
| ⬜ | `/oauth/jwks` | GET | Public keys |
| ⬜ | `/oauth/par` | POST | Pushed authorization request |
| ⬜ | `/oauth/authorize` | GET | Authorization page |
| ⬜ | `/oauth/authorize` | POST | Authorization form submit |
| ⬜ | `/oauth/token` | POST | Token exchange |
| ⬜ | `/oauth/passkeyauthenticationoptions` | POST | Passkey auth options |
| ⬜ | `/oauth/authenticatepasskey` | POST | Passkey authentication |

---

## Implementation Notes

### Suggested Order
1. **Foundation** (3) - Get server responding
2. **Identity** (1) - Handle resolution
3. **Authentication** (7) - Sessions, so you can test authenticated endpoints
4. **Repo Operations** (7) - Core CRUD for records
5. **Blobs** (3) - Image/media support
6. **Sync** (5) - Federation, includes WebSocket firehose
7. **App.Bsky** (2+proxy) - User preferences and AppView proxy
8. **OAuth** (9) - Full OAuth flow for client apps

### dnproto Reference Files
- XRPC handlers: `dnproto/src/pds/xrpc/*.cs`
- OAuth handlers: `dnproto/src/pds/oauth/*.cs`
- Route mapping: `dnproto/src/pds/Pds.cs` (MapEndpoints method)
- Helper utilities: `dnproto/src/pds/XrpcHelpers.cs`
