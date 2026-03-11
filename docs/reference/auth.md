# Authentication Reference

This document describes all authentication and authorization mechanisms implemented in the rustproto PDS.

## Table of Contents

- [Overview](#overview)
- [Session Types](#session-types)
  - [Legacy Sessions](#legacy-sessions)
  - [OAuth Sessions](#oauth-sessions)
  - [Admin Sessions](#admin-sessions)
  - [Service Auth](#service-auth)
- [Legacy Auth Flow](#legacy-auth-flow)
  - [Create Session](#create-session)
  - [Refresh Session](#refresh-session)
  - [Get Session](#get-session)
  - [Token Format](#legacy-token-format)
- [OAuth Flow](#oauth-flow)
  - [Server Metadata](#oauth-server-metadata)
  - [Pushed Authorization Request (PAR)](#pushed-authorization-request-par)
  - [User Authorization](#user-authorization)
  - [Token Exchange](#token-exchange)
  - [Token Refresh](#oauth-token-refresh)
  - [Token Revocation](#token-revocation)
  - [DPoP (Proof of Possession)](#dpop-proof-of-possession)
  - [OAuth Scopes](#oauth-scopes)
  - [PKCE](#pkce)
- [Admin Auth Flow](#admin-auth-flow)
- [Service Auth Flow](#service-auth-flow)
  - [Requesting a Service Auth Token](#requesting-a-service-auth-token)
  - [Validating Inbound Service Auth](#validating-inbound-service-auth)
- [Auth Detection Logic](#auth-detection-logic)
- [Endpoint Auth Requirements](#endpoint-auth-requirements)
- [Password Hashing](#password-hashing)
- [Token Lifetime Summary](#token-lifetime-summary)
- [Configuration Properties](#configuration-properties)
- [Dependencies](#dependencies)
- [Source Code Map](#source-code-map)

---

## Overview

The rustproto PDS supports four authentication mechanisms:

| Type | Algorithm | Transport | Use Case |
|---|---|---|---|
| **Legacy** | HS256 (symmetric) | `Authorization: Bearer` | Traditional atproto client login (handle + password) |
| **OAuth** | HS256 + DPoP | `Authorization: DPoP` | OAuth 2.0 clients with proof-of-possession binding |
| **Admin** | Cookie | `adminSessionId` cookie | Admin dashboard access |
| **Service Auth** | ES256 (asymmetric) | `Authorization: Bearer` | Inter-service requests (e.g., AppView → PDS) |

All session state is stored in SQLite via the `PdsDb` layer. Tokens are JWTs except for OAuth refresh tokens (opaque `refresh-{uuid}` strings) and admin sessions (opaque `{uuid}` cookies).

---

## Session Types

### Legacy Sessions

Traditional atproto authentication using handle/email + password login. Produces a short-lived access JWT and a long-lived refresh JWT, both signed with HS256.

**Database entity** — [`LegacySession`](../../src/pds/db/entities.rs):

| Field | Description |
|---|---|
| `created_date` | ISO 8601 creation timestamp |
| `access_jwt` | The access JWT (2-hour lifetime) |
| `refresh_jwt` | The refresh JWT (90-day lifetime) |
| `ip_address` | Client IP at session creation |
| `user_agent` | Client User-Agent at session creation |

**Database operations** — [`pds_db.rs`](../../src/pds/db/pds_db.rs):
- `create_legacy_session()` — Insert a new session
- `legacy_session_exists_for_access_jwt()` — Check if an access token has an active session
- `legacy_session_exists_for_refresh_jwt()` — Check if a refresh token has an active session
- `delete_legacy_session_for_refresh_jwt()` — Invalidate a session by refresh token
- `delete_all_legacy_sessions()` — Clear all legacy sessions
- `get_all_legacy_sessions()` — List all sessions (admin)

### OAuth Sessions

OAuth 2.0 sessions with DPoP token binding. The access token is bound to the client's public key via a `cnf.jkt` (JWK Thumbprint) claim, preventing token theft.

**Database entity** — [`OauthSession`](../../src/pds/db/entities.rs):

| Field | Description |
|---|---|
| `session_id` | Unique session identifier (UUID) |
| `client_id` | OAuth client ID |
| `scope` | Space-separated granted scopes |
| `dpop_jwk_thumbprint` | RFC 7638 JWK Thumbprint binding the session to a client key |
| `refresh_token` | Opaque refresh token (`refresh-{uuid}`) |
| `refresh_token_expires_date` | ISO 8601 expiry (default 90 days) |
| `created_date` | ISO 8601 creation timestamp |
| `ip_address` | Client IP at session creation |
| `auth_type` | How the user authenticated (e.g., `"Legacy"`, `"Passkey"`) |

**Database operations** — [`pds_db.rs`](../../src/pds/db/pds_db.rs):
- `insert_oauth_session()` — Create a new session
- `get_oauth_session_by_session_id()` — Lookup by session ID
- `get_oauth_session_by_refresh_token()` — Lookup by refresh token
- `has_valid_oauth_session_by_dpop_thumbprint()` — Check if a DPoP key has an active session
- `get_oauth_session_by_dpop_thumbprint()` — Lookup by DPoP key thumbprint
- `update_oauth_session()` — Rotate the refresh token
- `delete_oauth_session_by_refresh_token()` — Revoke by refresh token
- `delete_oauth_session_by_session_id()` — Revoke by session ID
- `delete_old_oauth_sessions()` — Cleanup expired sessions
- `delete_all_oauth_sessions()` — Clear all OAuth sessions
- `get_all_oauth_sessions()` — List all sessions (admin)

### Admin Sessions

Cookie-based sessions for the admin dashboard. Validated against client IP to prevent session hijacking.

**Database entity** — [`AdminSession`](../../src/pds/db/entities.rs):

| Field | Description |
|---|---|
| `session_id` | UUID session identifier |
| `ip_address` | Client IP (used for IP-based validation) |
| `user_agent` | Client User-Agent |
| `created_date` | ISO 8601 creation timestamp |
| `auth_type` | Authentication method (e.g., `"Legacy"`) |

**Database operations** — [`pds_db.rs`](../../src/pds/db/pds_db.rs):
- `insert_admin_session()` — Create a session
- `get_valid_admin_session()` — Lookup by ID with IP verification
- `get_valid_admin_session_any_ip()` — Lookup by ID without IP check
- `delete_stale_admin_sessions()` — Cleanup by timeout
- `delete_admin_session()` — Delete by ID
- `delete_all_admin_sessions()` — Clear all admin sessions
- `get_all_admin_sessions()` — List all sessions (admin)

### Service Auth

Stateless inter-service authentication using ES256-signed JWTs. No server-side session state — the token itself is the proof of authorization. Used when one service (e.g., a PDS) needs to make authenticated requests to another service (e.g., an AppView or relay).

Service auth tokens are signed with the user's P-256 private key. The receiving service validates the token by resolving the issuer's DID document and extracting the `#atproto` verification method public key.

---

## Legacy Auth Flow

### Create Session

**Endpoint**: `POST /xrpc/com.atproto.server.createSession`
**Source**: [`create_session.rs`](../../src/pds/xrpc/create_session.rs)
**Auth**: None (public endpoint)

**Request**:
```json
{
  "identifier": "handle.example.com",
  "password": "user-password"
}
```

**Flow**:
1. Resolve the actor using `BlueskyClient` (handle → DID resolution)
2. Verify password against stored hash using PBKDF2-SHA256 (`verify_password()`)
3. Generate an access JWT (2-hour lifetime) and a refresh JWT (90-day lifetime)
4. Store the session in the `LegacySession` table with client IP and User-Agent
5. Return tokens to the client

**Response** (success):
```json
{
  "did": "did:plc:abc123",
  "handle": "user.example.com",
  "accessJwt": "eyJ...",
  "refreshJwt": "eyJ..."
}
```

**Response** (failure): Returns HTTP 200 with empty token strings — does not leak whether the user exists.

### Refresh Session

**Endpoint**: `POST /xrpc/com.atproto.server.refreshSession`
**Source**: [`refresh_session.rs`](../../src/pds/xrpc/refresh_session.rs)
**Auth**: Bearer token containing the refresh JWT

**Flow**:
1. Extract the refresh JWT from the `Authorization: Bearer` header
2. Validate JWT signature, expiry, and scope (`com.atproto.refresh`)
3. Verify the `sub` claim matches the configured `UserDid`
4. Check the session exists in the database
5. **Delete the old session** and create a new one with fresh tokens (token rotation)
6. Return new access and refresh JWTs

**Concurrency**: Uses a static `Mutex` lock to prevent race conditions from concurrent refresh requests.

**Response**:
```json
{
  "did": "did:plc:abc123",
  "handle": "user.example.com",
  "accessJwt": "eyJ...(new)...",
  "refreshJwt": "eyJ...(new)..."
}
```

### Get Session

**Endpoint**: `GET /xrpc/com.atproto.server.getSession`
**Source**: [`get_session.rs`](../../src/pds/xrpc/get_session.rs)
**Auth**: Legacy, OAuth, or Service Auth

**Response**:
```json
{
  "did": "did:plc:abc123",
  "handle": "user.example.com",
  "email": "user@example.com",
  "emailConfirmed": true
}
```

### Legacy Token Format

**Access JWT** — generated by [`jwt.rs`](../../src/pds/auth/jwt.rs) → `generate_access_jwt()`:

| Field | Value |
|---|---|
| Algorithm | HS256 |
| Header `typ` | `at+jwt` |
| Claim `scope` | `com.atproto.access` |
| Claim `sub` | User DID |
| Claim `aud` | PDS DID |
| Claim `jti` | Random identifier |
| Lifetime | 2 hours |

**Refresh JWT** — generated by [`jwt.rs`](../../src/pds/auth/jwt.rs) → `generate_refresh_jwt()`:

| Field | Value |
|---|---|
| Algorithm | HS256 |
| Header `typ` | `refresh+jwt` |
| Claim `scope` | `com.atproto.refresh` |
| Claim `sub` | User DID |
| Claim `aud` | PDS DID |
| Claim `jti` | 32 random bytes, base64-encoded |
| Lifetime | 90 days |

Both tokens are signed using the `JwtSecret` config property (a 32-character hex string).

**Validation** — [`jwt.rs`](../../src/pds/auth/jwt.rs) → `validate_access_jwt()`:
1. Verify HS256 signature against `JwtSecret`
2. Confirm `scope` = `com.atproto.access`
3. Confirm `sub` matches the expected user DID
4. Check expiration (optionally skipped)
5. Verify a `LegacySession` exists for this access token in the database

---

## OAuth Flow

The OAuth implementation follows multiple RFCs:
- **RFC 9126** — Pushed Authorization Requests (PAR)
- **RFC 9449** — DPoP (Demonstrating Proof of Possession)
- **RFC 7636** — PKCE (Proof Key for Code Exchange)
- **RFC 7009** — Token Revocation
- **RFC 8414** — Authorization Server Metadata
- **RFC 8707** — Protected Resource Metadata

OAuth must be enabled via the `FeatureEnabled_Oauth` config property.

### OAuth Server Metadata

**Endpoint**: `GET /.well-known/oauth-authorization-server`
**Source**: [`authorization_server.rs`](../../src/pds/oauth/authorization_server.rs)

Published metadata:

```json
{
  "issuer": "https://pds.example.com",
  "authorization_endpoint": "https://pds.example.com/oauth/authorize",
  "token_endpoint": "https://pds.example.com/oauth/token",
  "revocation_endpoint": "https://pds.example.com/oauth/revoke",
  "pushed_authorization_request_endpoint": "https://pds.example.com/oauth/par",
  "jwks_uri": "https://pds.example.com/oauth/jwks",
  "scopes_supported": ["atproto", "transition:email", "transition:generic", "transition:chat.bsky"],
  "require_pushed_authorization_requests": true,
  "dpop_signing_alg_values_supported": ["RS256", "ES256", ...],
  "grant_types_supported": ["authorization_code", "refresh_token"],
  "response_types_supported": ["code"],
  "code_challenge_methods_supported": ["S256"]
}
```

**Key point**: `require_pushed_authorization_requests: true` — clients **must** use PAR. Direct authorization requests are not supported.

**Protected resource metadata** — `GET /.well-known/oauth-protected-resource`
**Source**: [`protected_resource.rs`](../../src/pds/oauth/protected_resource.rs)

```json
{
  "resource": "https://pds.example.com",
  "authorization_servers": ["https://pds.example.com"],
  "scopes_supported": [],
  "bearer_methods_supported": ["header"]
}
```

**JWKS** — `GET /oauth/jwks`
**Source**: [`jwks.rs`](../../src/pds/oauth/jwks.rs)

Returns `{ "keys": [] }` because all token signing uses HS256 (symmetric). There are no asymmetric public keys to expose.

### Pushed Authorization Request (PAR)

**Endpoint**: `POST /oauth/par`
**Source**: [`par.rs`](../../src/pds/oauth/par.rs)

The client initiates the OAuth flow by pushing the authorization request parameters to the server.

**Request**:
```http
POST /oauth/par HTTP/1.1
DPoP: <DPoP proof JWT>
Content-Type: application/x-www-form-urlencoded

client_id=https://client.example.com/client-metadata.json
&scope=atproto transition:generic
&redirect_uri=https://client.example.com/callback
&code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM
&code_challenge_method=S256
&state=abc123
&response_type=code
```

**Flow**:
1. Validate that OAuth is enabled
2. Validate the DPoP proof header (required)
3. Validate `redirect_uri` against the `OauthAllowedRedirectUris` allowlist
4. Store the request in the `OauthRequest` table with a 5-minute expiry
5. Return a request URI that the client uses to redirect the user

**Response**:
```json
{
  "request_uri": "urn:ietf:params:oauth:request_uri:550e8400-e29b-41d4-a716-446655440000",
  "expires_in": 300
}
```

**Transient state** — `OauthRequest` entity ([`entities.rs`](../../src/pds/db/entities.rs)):

| Field | Description |
|---|---|
| `request_uri` | `urn:ietf:params:oauth:request_uri:{uuid}` |
| `expires_date` | ISO 8601 (5 minutes from creation) |
| `dpop` | The DPoP proof JWT from the PAR request |
| `body` | Complete request parameters |
| `authorization_code` | Set after user authorizes (`authcode-{uuid}`) |
| `auth_type` | Authentication method used by the user |

### User Authorization

**Display form**: `GET /oauth/authorize?client_id=...&request_uri=...`
**Source**: [`authorize_get.rs`](../../src/pds/oauth/authorize_get.rs)

Loads the `OauthRequest` from the database and presents an HTML login form with:
- Username/password fields (legacy auth)
- Passkey authentication button (if `FeatureEnabled_Passkeys` is enabled)
- Scope display

**Submit authorization**: `POST /oauth/authorize`
**Source**: [`authorize_post.rs`](../../src/pds/oauth/authorize_post.rs)

**Flow**:
1. Load `OauthRequest` by `request_uri`
2. Authenticate the user (resolve actor via `BlueskyClient`, verify password)
3. Validate `redirect_uri` against allowlist
4. Generate authorization code: `authcode-{uuid}`
5. Store the code in `OauthRequest.authorization_code`
6. Redirect the user: `{redirect_uri}?code={code}&state={state}&iss={issuer}`

### Token Exchange

**Endpoint**: `POST /oauth/token`
**Source**: [`token.rs`](../../src/pds/oauth/token.rs)

#### Authorization Code Grant

```http
POST /oauth/token HTTP/1.1
DPoP: <DPoP proof JWT>
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code
&code=authcode-550e8400-...
&code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk
&redirect_uri=https://client.example.com/callback
&client_id=https://client.example.com/client-metadata.json
```

**Validation**:
1. Validate DPoP proof (required)
2. Verify authorization code exists and is not expired
3. Verify PKCE: `base64url(sha256(code_verifier)) == stored code_challenge`
4. Verify `redirect_uri` and `client_id` match the original PAR request

**Result**:
- Creates an `OauthSession` with 90-day refresh token expiry
- Deletes the used `OauthRequest`
- Returns tokens

**Response**:
```json
{
  "access_token": "eyJ...",
  "token_type": "DPoP",
  "expires_in": 3600,
  "refresh_token": "refresh-550e8400-...",
  "scope": "atproto transition:generic",
  "sub": "did:plc:abc123"
}
```

**OAuth Access Token format** — generated by `generate_oauth_access_token()` in [`token.rs`](../../src/pds/oauth/token.rs):

| Field | Value |
|---|---|
| Algorithm | HS256 |
| Header `typ` | `at+jwt` |
| Claim `iss` | `https://{hostname}` |
| Claim `sub` | User DID |
| Claim `aud` | `https://{hostname}` (PDS issuer) |
| Claim `scope` | Granted scopes (space-separated) |
| Claim `client_id` | OAuth client ID |
| Claim `cnf` | `{ "jkt": "<dpop_jwk_thumbprint>" }` (DPoP binding) |
| Claim `jti` | Random identifier |
| Lifetime | 1 hour (3600 seconds) |

### OAuth Token Refresh

#### Refresh Token Grant

```http
POST /oauth/token HTTP/1.1
DPoP: <DPoP proof JWT>
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token
&refresh_token=refresh-550e8400-...
```

**Validation**:
1. Validate DPoP proof (required)
2. Verify refresh token exists in `OauthSession` and has not expired
3. Verify DPoP JWK thumbprint matches the session's `dpop_jwk_thumbprint` (token binding check)

**Result**: Issues a new access token (1-hour) and a new refresh token (90-day expiry). Updates the `OauthSession`.

### Token Revocation

**Endpoint**: `POST /oauth/revoke`
**Source**: [`revoke.rs`](../../src/pds/oauth/revoke.rs)

```http
POST /oauth/revoke HTTP/1.1
Content-Type: application/x-www-form-urlencoded

token=refresh-550e8400-...
```

Deletes the `OauthSession` by refresh token. Always returns HTTP 200 per RFC 7009.

### DPoP (Proof of Possession)

**Source**: [`dpop.rs`](../../src/pds/oauth/dpop.rs)
**Specification**: RFC 9449

DPoP binds OAuth tokens to a client's key pair, preventing token theft. Every OAuth request must include a DPoP proof — a JWT signed by the client's private key.

**DPoP proof structure**:

Header:
```json
{
  "typ": "dpop+jwt",
  "alg": "ES256",
  "jwk": { "kty": "EC", "crv": "P-256", "x": "...", "y": "..." }
}
```

Payload:
```json
{
  "jti": "unique-id",
  "htm": "POST",
  "htu": "https://pds.example.com/oauth/token",
  "iat": 1709000000
}
```

**Validation** — `validate_dpop()` in [`dpop.rs`](../../src/pds/oauth/dpop.rs):
1. Parse JWT structure (3 dot-separated parts)
2. Validate header: `typ` = `dpop+jwt`, asymmetric `alg`, `jwk` present
3. Validate payload: `jti` required, `htm` matches expected HTTP method (case-insensitive), `htu` matches expected URI (path comparison, ignores query/fragment), `iat` not in future (60-second clock skew allowed), not older than `max_age_seconds` (default 300)
4. Verify signature using the embedded JWK public key
5. Calculate JWK Thumbprint (RFC 7638, SHA-256) for token binding

**Supported DPoP algorithms**: RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES256K, ES384, ES512

**Supported key types for signature verification**: EC P-256 (ES256), EC secp256k1 (ES256K), RSA (RS256, PS256, etc.)

**Token binding**: The JWK Thumbprint from the DPoP proof is stored in `OauthSession.dpop_jwk_thumbprint` and embedded in the access token's `cnf.jkt` claim. On every authenticated request, the PDS verifies that the DPoP proof's JWK Thumbprint matches the access token's `cnf.jkt`. This ensures the token can only be used by the key holder.

### OAuth Scopes

Scopes declared in authorization server metadata:

| Scope | Description |
|---|---|
| `atproto` | Full access to atproto endpoints |
| `transition:email` | Email-related transition scope |
| `transition:generic` | Generic capability transition scope |
| `transition:chat.bsky` | Bluesky chat service scope |

Scopes are stored in the `OauthSession` and included in the access token's `scope` claim. The current implementation passes scopes through from the authorization request to the token without per-endpoint enforcement.

### PKCE

PKCE (RFC 7636) is required for all OAuth authorization code grants.

**Method**: S256 only (`code_challenge_method=S256`)

**Flow**:
1. Client generates a random `code_verifier`
2. Client computes `code_challenge = base64url(sha256(code_verifier))`
3. `code_challenge` is sent in the PAR request
4. `code_verifier` is sent in the token exchange request
5. Server recomputes the challenge from the verifier and compares

---

## Admin Auth Flow

**Source**: [`login.rs`](../../src/pds/admin/login.rs), [`sessions.rs`](../../src/pds/admin/sessions.rs)

Requires `FeatureEnabled_AdminDashboard` to be enabled.

**Login** — `POST /admin/login`:
1. User submits username (`admin`) and password
2. Password verified against `AdminHashedPassword` config (PBKDF2-SHA256)
3. On success, creates an `AdminSession` in the database
4. Sets `adminSessionId` cookie:
   - `HttpOnly` — not accessible to JavaScript
   - `Secure` — HTTPS only
   - `SameSite=Strict` — no cross-site requests
   - `Max-Age=3600` — 1-hour lifetime

**Session validation** — `is_authenticated()` in [`mod.rs`](../../src/pds/admin/mod.rs):
1. Extract `adminSessionId` from cookies
2. Load session from `AdminSession` table
3. Check session exists and is not stale
4. Verify client IP matches session IP (prevents session hijacking)

**Logout** — `GET /admin/logout`:
Deletes the admin session by ID.

**Admin pages** (all require authentication):
- `/admin` — Dashboard home
- `/admin/sessions` — View/manage all sessions (legacy, OAuth, admin)
- `/admin/config` — View configuration
- `/admin/passkeys` — Manage passkeys (if enabled)
- `/admin/stats` — PDS statistics

---

## Service Auth Flow

Service auth enables inter-service communication in the atproto network. A PDS signs a short-lived JWT with the user's private key, which the receiving service validates by resolving the signer's DID document.

### Requesting a Service Auth Token

**Endpoint**: `GET /xrpc/com.atproto.server.getServiceAuth`
**Source**: [`get_service_auth.rs`](../../src/pds/xrpc/get_service_auth.rs)
**Auth**: Legacy or OAuth (user must be authenticated)

**Request**:
```
GET /xrpc/com.atproto.server.getServiceAuth?aud=did:web:appview.example.com&lxm=com.atproto.repo.uploadBlob&exp=1709000060
```

| Parameter | Required | Description |
|---|---|---|
| `aud` | Yes | DID of the service that will receive the token |
| `lxm` | No | Lexicon method NSID to bind the token to a specific endpoint |
| `exp` | No | Unix timestamp expiry (clamped to max 300 seconds from now) |

**Flow**:
1. Validate the user is authenticated
2. Load `UserPrivateKeyMultibase` from config (P-256 private key, multibase-encoded)
3. Sign a JWT using ES256: issuer = user DID, audience = requested service DID

**Response**:
```json
{
  "token": "eyJ..."
}
```

**Token format** — generated by [`signer.rs`](../../src/pds/auth/signer.rs) → `sign_service_auth_token()`:

| Field | Value |
|---|---|
| Algorithm | ES256 (ECDSA P-256, asymmetric) |
| Claim `iss` | User DID (the signer) |
| Claim `aud` | Target service DID |
| Claim `lxm` | Lexicon method NSID (optional) |
| Claim `jti` | Random identifier |
| Default lifetime | 60 seconds |

**Key format**: The private key is stored as a multibase string (prefix `z` = base58btc) with a P-256 compressed key prefix (`0x80 0x26`). Signatures are normalized to low-S form (BIP-62 compliance).

### Validating Inbound Service Auth

**Source**: [`auth_helpers.rs`](../../src/pds/xrpc/auth_helpers.rs) → `validate_service_auth_token()` / `validate_service_auth_token_async()`

When the PDS receives a request with service auth, it validates the token:

1. Extract Bearer token and decode the JWT payload (without signature verification)
2. Validate `aud` matches this PDS's DID (`PdsDid` config)
3. Validate `lxm` matches the expected lexicon method (if specified by the endpoint)
4. Check the token is not expired
5. **DID resolution**: Fetch the issuer's DID document via `BlueskyClient::resolve_actor_info()` with `resolve_did_doc: true`
6. Extract the public key from the DID document's `verificationMethod` array — the entry with an `id` ending in `#atproto`
7. Decode the `publicKeyMultibase` field to get the raw P-256 public key
8. Verify the JWT's ES256 signature against the extracted public key

There are two implementations:
- `validate_service_auth_token()` — Synchronous (uses `tokio::runtime::Handle::block_on()`)
- `validate_service_auth_token_async()` — Async (properly awaits DID resolution)

---

## Auth Detection Logic

**Source**: [`auth_helpers.rs`](../../src/pds/xrpc/auth_helpers.rs)

The `AuthType` enum:
```rust
pub enum AuthType {
    Legacy,    // Bearer token with HS256 JWT
    Oauth,     // DPoP token with at+jwt type
    Service,   // Bearer token with ES256 JWT + lxm claim
}
```

When a request arrives, the PDS determines the auth type by inspecting headers and token structure:

**1. OAuth** — `is_oauth_token_request()`:
- `DPoP` header is present
- `Authorization` header uses the `DPoP` scheme (not `Bearer`)
- Token's JWT header has `typ` = `at+jwt`

**2. Service Auth** — `is_service_auth_request()`:
- `Authorization: Bearer` scheme
- Token's JWT header has `alg` = `ES256` (asymmetric)
- Token payload contains an `lxm` claim
- Token payload `iss` starts with `did:`

**3. Legacy** — fallback:
- `Authorization: Bearer` scheme
- Token's JWT header has `alg` = `HS256`
- None of the Service Auth signals are present

**Main entry point** — `check_user_auth()`:
1. Check if request is OAuth → call `check_oauth_auth()`
2. Check if request is Service Auth → call `check_service_auth()` / `check_service_auth_async()`
3. Otherwise, treat as Legacy → call `check_legacy_auth()`

Each endpoint specifies which auth types it accepts via the `allowed_auth_types` parameter. The default is `[Legacy, Oauth]`.

---

## Endpoint Auth Requirements

### Public Endpoints (no auth required)

| Endpoint | Method | Source |
|---|---|---|
| `_health` | GET | [`health.rs`](../../src/pds/xrpc/health.rs) |
| `hello` | GET | [`hello.rs`](../../src/pds/xrpc/hello.rs) |
| `com.atproto.server.createSession` | POST | [`create_session.rs`](../../src/pds/xrpc/create_session.rs) |
| `com.atproto.server.describeServer` | GET | [`describe_server.rs`](../../src/pds/xrpc/describe_server.rs) |
| `com.atproto.identity.resolveHandle` | GET | [`resolve_handle.rs`](../../src/pds/xrpc/resolve_handle.rs) |
| `com.atproto.repo.describeRepo` | GET | [`describe_repo.rs`](../../src/pds/xrpc/describe_repo.rs) |
| `com.atproto.repo.getRecord` | GET | [`get_record.rs`](../../src/pds/xrpc/get_record.rs) |
| `com.atproto.repo.listRecords` | GET | [`list_records.rs`](../../src/pds/xrpc/list_records.rs) |
| `com.atproto.sync.getRepo` | GET | [`sync_get_repo.rs`](../../src/pds/xrpc/sync_get_repo.rs) |
| `com.atproto.sync.getRepoStatus` | GET | [`sync_get_repo_status.rs`](../../src/pds/xrpc/sync_get_repo_status.rs) |
| `com.atproto.sync.listRepos` | GET | [`sync_list_repos.rs`](../../src/pds/xrpc/sync_list_repos.rs) |
| `com.atproto.sync.getRecord` | GET | [`sync_get_record.rs`](../../src/pds/xrpc/sync_get_record.rs) |
| `com.atproto.sync.getBlob` | GET | [`get_blob.rs`](../../src/pds/xrpc/get_blob.rs) |
| `com.atproto.sync.listBlobs` | GET | [`list_blobs.rs`](../../src/pds/xrpc/list_blobs.rs) |
| `com.atproto.sync.subscribeRepos` | WebSocket | [`subscribe_repos.rs`](../../src/pds/xrpc/subscribe_repos.rs) |
| `.well-known/atproto-did` | GET | [`well_known_atproto_did.rs`](../../src/pds/xrpc/well_known_atproto_did.rs) |
| `.well-known/did.json` | GET | [`well_known_did.rs`](../../src/pds/xrpc/well_known_did.rs) |
| `.well-known/oauth-authorization-server` | GET | [`authorization_server.rs`](../../src/pds/oauth/authorization_server.rs) |
| `.well-known/oauth-protected-resource` | GET | [`protected_resource.rs`](../../src/pds/oauth/protected_resource.rs) |

### Authenticated Endpoints (Legacy + OAuth)

| Endpoint | Method | Source |
|---|---|---|
| `com.atproto.server.getSession` | GET | [`get_session.rs`](../../src/pds/xrpc/get_session.rs) |
| `com.atproto.server.refreshSession` | POST | [`refresh_session.rs`](../../src/pds/xrpc/refresh_session.rs) |
| `com.atproto.server.getServiceAuth` | GET | [`get_service_auth.rs`](../../src/pds/xrpc/get_service_auth.rs) |
| `com.atproto.server.activateAccount` | POST | [`activate_account.rs`](../../src/pds/xrpc/activate_account.rs) |
| `com.atproto.server.deactivateAccount` | POST | [`deactivate_account.rs`](../../src/pds/xrpc/deactivate_account.rs) |
| `com.atproto.server.checkAccountStatus` | GET | [`check_account_status.rs`](../../src/pds/xrpc/check_account_status.rs) |
| `com.atproto.repo.createRecord` | POST | [`create_record.rs`](../../src/pds/xrpc/create_record.rs) |
| `com.atproto.repo.putRecord` | POST | [`put_record.rs`](../../src/pds/xrpc/put_record.rs) |
| `com.atproto.repo.deleteRecord` | POST | [`delete_record.rs`](../../src/pds/xrpc/delete_record.rs) |
| `com.atproto.repo.applyWrites` | POST | [`apply_writes.rs`](../../src/pds/xrpc/apply_writes.rs) |
| `com.atproto.actor.getPreferences` | GET | [`get_preferences.rs`](../../src/pds/xrpc/get_preferences.rs) |
| `com.atproto.actor.putPreferences` | POST | [`put_preferences.rs`](../../src/pds/xrpc/put_preferences.rs) |
| `app.bsky.*` (proxy) | * | [`app_bsky_proxy.rs`](../../src/pds/xrpc/app_bsky_proxy.rs) |

### Authenticated Endpoints (Legacy + OAuth + Service Auth)

| Endpoint | Method | lxm | Source |
|---|---|---|---|
| `com.atproto.repo.uploadBlob` | POST | `com.atproto.repo.uploadBlob` | [`upload_blob.rs`](../../src/pds/xrpc/upload_blob.rs) |

### Admin Endpoints (Cookie auth)

| Endpoint | Method | Source |
|---|---|---|
| `/admin/login` | GET/POST | [`login.rs`](../../src/pds/admin/login.rs) |
| `/admin/logout` | GET | [`login.rs`](../../src/pds/admin/login.rs) |
| `/admin` | GET | [`mod.rs`](../../src/pds/admin/mod.rs) |
| `/admin/sessions` | GET/POST | [`sessions.rs`](../../src/pds/admin/sessions.rs) |
| `/admin/config` | GET | Admin config page |
| `/admin/passkeys` | GET | Admin passkeys page |
| `/admin/stats` | GET | Admin stats page |

---

## Password Hashing

**Source**: [`password.rs`](../../src/pds/auth/password.rs)

| Parameter | Value |
|---|---|
| Algorithm | PBKDF2-SHA256 |
| Iterations | 100,000 (OWASP standard) |
| Salt size | 16 bytes (128 bits) |
| Hash size | 32 bytes (256 bits) |
| Storage format | `base64(salt ∥ hash)` = 64 characters |

**Verification**: `verify_password(stored_hash, password)` uses constant-time comparison to prevent timing attacks.

---

## Token Lifetime Summary

| Token | Algorithm | Lifetime | Stored in DB | Scope/Binding |
|---|---|---|---|---|
| Legacy access JWT | HS256 | 2 hours | `LegacySession` | `com.atproto.access` |
| Legacy refresh JWT | HS256 | 90 days | `LegacySession` | `com.atproto.refresh` |
| OAuth access token | HS256 | 1 hour | Not stored | Custom scopes + `cnf.jkt` DPoP binding |
| OAuth refresh token | Opaque | 90 days | `OauthSession` | Bound to DPoP JWK Thumbprint |
| Service auth JWT | ES256 | 60 seconds (default) | Not stored | `aud` + optional `lxm` binding |
| Admin session cookie | Opaque | 1 hour | `AdminSession` | IP-bound |
| OAuth PAR request | N/A | 5 minutes | `OauthRequest` | Transient |

---

## Configuration Properties

### Required

| Property | Description |
|---|---|
| `UserDid` | The PDS user's DID |
| `PdsDid` | The PDS's own DID |
| `JwtSecret` | 32-character hex string for HS256 token signing |
| `UserPrivateKeyMultibase` | P-256 private key (multibase `z` prefix, base58btc) for service auth |
| `UserHashedPassword` | PBKDF2-SHA256 hash for legacy password authentication |
| `AdminHashedPassword` | PBKDF2-SHA256 hash for admin dashboard login |

### Feature Flags

| Property | Default | Description |
|---|---|---|
| `FeatureEnabled_Oauth` | `false` | Enable OAuth 2.0 endpoints |
| `FeatureEnabled_Passkeys` | `false` | Enable WebAuthn passkey authentication in OAuth flow |
| `FeatureEnabled_AdminDashboard` | `false` | Enable admin dashboard |

### OAuth Settings

| Property | Description |
|---|---|
| `OauthAllowedRedirectUris` | Comma-separated allowlist of OAuth redirect URIs |
| `PdsHostname` | Hostname for OAuth issuer URIs (e.g., `pds.example.com`) |

---

## Dependencies

Auth-related crate dependencies from [`Cargo.toml`](../../Cargo.toml):

| Crate | Version | Purpose |
|---|---|---|
| `jsonwebtoken` | 9.3 | JWT signing and verification (HS256, ES256) |
| `p256` | 0.13 | P-256 ECDSA signing for service auth |
| `k256` | 0.13 | secp256k1 ECDSA for DPoP verification |
| `rsa` | 0.9 | RSA signature verification for DPoP |
| `pbkdf2` | 0.12 | Password hashing |
| `sha2` | 0.10 | SHA-256 hashing |
| `hmac` | 0.12 | HMAC for HS256 |
| `base64` | 0.22 | Base64 encoding/decoding |
| `uuid` | 1.0 | Token and session identifiers |
| `rand` | 0.8 | Random number generation |
| `chrono` | 0.4 | Timestamp handling |
| `rusqlite` | 0.32 | Session storage (SQLite) |

---

## Source Code Map

```
src/pds/
├── auth/
│   ├── mod.rs              # Module exports
│   ├── jwt.rs              # Legacy JWT generation & validation (HS256)
│   ├── password.rs         # PBKDF2-SHA256 password hashing & verification
│   └── signer.rs           # ES256 service auth token signing
├── oauth/
│   ├── mod.rs              # Module organization
│   ├── dpop.rs             # DPoP proof validation (RFC 9449)
│   ├── par.rs              # Pushed Authorization Requests (RFC 9126)
│   ├── authorize_get.rs    # Authorization form display
│   ├── authorize_post.rs   # Authorization form submission
│   ├── token.rs            # Token endpoint (auth code + refresh grants)
│   ├── revoke.rs           # Token revocation (RFC 7009)
│   ├── jwks.rs             # JWKS endpoint
│   ├── authorization_server.rs  # OAuth server metadata (RFC 8414)
│   ├── protected_resource.rs    # Protected resource metadata (RFC 8707)
│   ├── authenticate_passkey.rs  # WebAuthn passkey authentication
│   ├── passkey_auth_options.rs  # WebAuthn challenge options
│   └── helpers.rs          # OAuth utilities
├── xrpc/
│   ├── auth_helpers.rs     # Core auth detection, validation, and dispatch
│   ├── create_session.rs   # Legacy login
│   ├── refresh_session.rs  # Legacy token refresh
│   ├── get_session.rs      # Get current session info
│   └── get_service_auth.rs # Service auth token generation
├── admin/
│   ├── login.rs            # Admin login/logout
│   ├── sessions.rs         # Admin session management UI
│   └── mod.rs              # Admin routing and auth check
└── db/
    ├── entities.rs         # Session data types
    └── pds_db.rs           # Database operations for all session types
```
