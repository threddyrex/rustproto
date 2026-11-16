# OAuth in rustproto

This article describes the OAuth flow implemented by rustproto's PDS, how its
endpoints map to OAuth concepts, and where the implementation is deliberately
or currently incomplete. It describes the code in `src/pds/oauth/` and
`src/pds/auth/auth_flow.rs`; advertised metadata is not, by itself, proof that
every advertised feature is implemented.

## Which version of OAuth?

rustproto implements **OAuth 2.0** (the authorization-code grant from
[RFC 6749](https://www.rfc-editor.org/rfc/rfc6749)), with later OAuth
extensions and the AT Protocol OAuth profile. It is not a general-purpose
OAuth server or an implementation of OAuth 2.1.

The key extensions in the flow are:

| Concept | Specification | How rustproto uses it |
|---|---|---|
| Authorization-code grant | [RFC 6749](https://www.rfc-editor.org/rfc/rfc6749) | A client obtains a short-lived code, then exchanges it for tokens. |
| PKCE | [RFC 7636](https://www.rfc-editor.org/rfc/rfc7636) | The token endpoint checks the `S256` challenge against the client's `code_verifier`. |
| Pushed Authorization Requests (PAR) | [RFC 9126](https://www.rfc-editor.org/rfc/rfc9126) | The client submits the authorization parameters directly to `/oauth/par`; the browser is sent a short-lived `request_uri`. |
| DPoP | [RFC 9449](https://www.rfc-editor.org/rfc/rfc9449) | Proofs bind the client to a key, and access tokens are bound to that key. |
| Authorization-server metadata | [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414) | `/.well-known/oauth-authorization-server` publishes endpoints and capabilities. |
| Protected-resource metadata | [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728) | `/.well-known/oauth-protected-resource` identifies the PDS and its authorization server. |
| Token revocation | [RFC 7009](https://www.rfc-editor.org/rfc/rfc7009) | `/oauth/revoke` can revoke a session by its refresh token. |

AT Protocol's OAuth profile and permission vocabulary are described in the
[OAuth specification](https://atproto.com/specs/oauth) and
[permission specification](https://atproto.com/specs/permission).

## OAuth vocabulary in this app

- **Authorization server (AS):** the PDS itself. It authenticates the user and
  issues tokens.
- **Client:** the app requesting access. It supplies a `client_id`, redirect
  URI, requested scopes, PKCE challenge, and DPoP proofs.
- **Resource server:** also the PDS. It accepts OAuth access tokens on
  authenticated API requests.
- **Resource owner:** the PDS account owner. The current implementation
  authenticates the configured account rather than selecting among multiple
  local users.
- **Authorization grant:** the one-time authorization code returned to the
  client after the user authenticates.
- **Access token:** a one-hour JWT used to call the PDS. It is DPoP-bound and
  sent with the `DPoP` authorization scheme.
- **Refresh token:** a longer-lived, rotating credential used to obtain another
  access token without repeating the browser flow.
- **Scope:** space-separated AT Protocol permission strings. `include:<nsid>`
  permission-set references are expanded for display and token issuance when
  their Lexicon can be resolved.

## The flow, step by step

### 1. Discover the PDS and its OAuth server

The client can fetch:

- `GET /.well-known/oauth-protected-resource` to discover the resource and
  authorization server.
- `GET /.well-known/oauth-authorization-server` to discover the PAR,
  authorization, token, revocation, and JWKS endpoints.

OAuth must be enabled in the PDS configuration (`FeatureEnabled_Oauth`). The
PDS also has a configured allowlist of redirect URIs
(`OauthAllowedRedirectUris`).

### 2. Create a PKCE challenge and push the request

The client generates a high-entropy `code_verifier`, computes
`BASE64URL(SHA256(code_verifier))`, and sends that value as
`code_challenge` with `code_challenge_method=S256`.

It then posts the authorization parameters to `POST /oauth/par`, including
`client_id`, `redirect_uri`, `response_type=code`, `scope`, `state`, and the
PKCE challenge. The request also carries a DPoP proof for the PAR endpoint.

The PDS validates that proof and checks a supplied redirect URI against its
configured allowlist. It stores the request for five minutes and returns a
`request_uri` of the form
`urn:ietf:params:oauth:request_uri:...`. This pushed request is the server-side
record used by the browser and later code exchange.

### 3. Authenticate and authorize in the browser

The client sends the browser to `/oauth/authorize` with `client_id` and
`request_uri`. The PDS loads the pushed request and displays the requested
scope. If an `include:<nsid>` permission set resolves, the page also shows the
concrete permissions it expands to.

The user authenticates using the configured PDS password, or a registered
passkey if passkeys are enabled (`FeatureEnabled_Passkeys`). On successful
authentication, the PDS stores an authorization code with the pushed request
and redirects to the configured callback with `code`, the original `state`,
and the issuer (`iss`). The code is single-use and expires with the five-minute
pushed request.

The client must compare the returned `state` with the value it generated before
continuing. The server echoes it; the client is responsible for this
client-side CSRF check.

### 4. Exchange the code for tokens

The client posts to `POST /oauth/token` with
`grant_type=authorization_code`, the code, `client_id`, the same
`redirect_uri`, and the original `code_verifier`. It includes a DPoP proof for
the token endpoint.

The PDS checks the code, PKCE S256 challenge, client ID, redirect URI, and DPoP
proof. It creates a session bound to the DPoP key and consumes the code. The
response includes:

- an access token that expires in one hour;
- a refresh token that initially expires in 90 days;
- the effective scope and the account DID (`sub`).

The access token is an HS256 JWT with type `at+jwt`. It includes the issuer,
subject, audience, expiry, client ID, scope, and `cnf.jkt` (the DPoP key's JWK
thumbprint).

### 5. Call the PDS with DPoP

For OAuth-authenticated API requests, the client sends the access token as
`Authorization: DPoP <access-token>` and includes a fresh `DPoP` proof header.
The PDS validates the proof's signature, `htm` (HTTP method), `htu` (HTTP
target URI), `iat` timestamp, and embedded public key. It checks that the
proof's JWK thumbprint matches the token's `cnf.jkt`, then validates the token
and session.

The JWT is locally signed with the PDS's symmetric `JwtSecret`; the client does
not need to verify it. `/oauth/jwks` therefore currently returns an empty key
set.

### 6. Refresh or revoke

To refresh, the client posts `grant_type=refresh_token` and its refresh token
to `/oauth/token`, with a DPoP proof made by the same key. The server checks
that key binding, rotates the refresh token, and issues another one-hour
access token. The new refresh token expires in 30 days.

To revoke, the client posts the token to `/oauth/revoke`. In practice, the
handler deletes a session by its refresh token; it does not revoke a JWT by
itself. API authentication also requires a valid session, so deleting the
session makes its access token unusable.

## Endpoints

| Endpoint | Role |
|---|---|
| `/.well-known/oauth-protected-resource` | Protected-resource metadata |
| `/.well-known/oauth-authorization-server` | Authorization-server metadata |
| `/oauth/jwks` | JWKS endpoint; currently empty because access tokens use HS256 |
| `/oauth/par` | Pushed authorization request |
| `/oauth/authorize` (GET and POST) | Browser login and authorization-code creation |
| `/oauth/passkeyauthenticationoptions` | WebAuthn options for passkey login |
| `/oauth/authenticatepasskey` | Verifies the passkey assertion and completes authorization |
| `/oauth/token` | Authorization-code exchange and refresh-token grant |
| `/oauth/revoke` | Session revocation by refresh token |

## Important limitations and metadata caveats

This is an OAuth 2.0 authorization-code flow with selected extensions, not
complete support for every capability listed in the metadata:

- **Single configured account:** password login checks the configured PDS
  password and resolves the submitted username; issued tokens use the
  configured `UserDid`. This is not a multi-user account or client-management
  system.
- **No client authentication at the token endpoint:** metadata lists `none`
  and `private_key_jwt`, but the token handler does not validate client
  assertions. It checks that the submitted `client_id` matches the pushed
  request. `client_id_metadata_document_supported` is advertised, but the
  implementation does not fetch or validate client metadata documents.
- **Only two grants and no OpenID Connect:** the token endpoint supports
  `authorization_code` and `refresh_token`. It does not issue ID tokens or
  implement OpenID Connect, client-credentials, device, or implicit flows.
- **Fixed browser response behavior:** the code path returns an authorization
  code using a query-string redirect. Although metadata lists several response
  modes and prompt values, those modes and prompts are not implemented as
  alternatives by the authorization handler.
- **Request objects are not processed:** the metadata contains request-object
  fields, but PAR stores submitted form parameters; it does not validate a
  signed or encrypted request object.
- **Partial DPoP algorithm and replay support:** DPoP proofs are checked for a
  recent `iat`, method, URI, and signature, but the `jti` is not tracked to
  reject proof replays. The metadata's algorithm list is broader than the
  verifier: for example, the RSA verifier implements RS256, not every
  advertised RSA or PSS algorithm.
- **Scopes are not a general authorization engine:** requested and resolved
  scopes are displayed, stored, and included in tokens. The generic OAuth
  authentication check validates the token, DPoP binding, account, and
  session; it does not itself enforce each scope against each XRPC operation.
  Do not assume that a scope string restricts an endpoint unless that
  endpoint's own authorization logic enforces it.
- **Limited revocation request:** revocation deletes a session by refresh
  token. Supplying an access token does not directly revoke its session, and
  the endpoint currently returns a JSON body rather than RFC 7009's empty
  success response.

For endpoint-level behavior and current route coverage, see the
[XRPC endpoint reference](../xrpc-reference.md) and the implementation in
[`src/pds/oauth/`](/src/pds/oauth/).
