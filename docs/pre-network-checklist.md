# Pre-Network Testing Checklist

Things to verify before requesting a crawl from Bluesky and connecting to the network.

## Prerequisites

- [x] Actor can be resolved
- [x] Repo can be retrieved
- [x] Repo can be printed/inspected

## Checklist

### 1. Firehose/WebSocket (`subscribeRepos`)
The relay will connect to your firehose after a crawl request.

- [x] Connect to firehose locally using `goat firehose --host wss://your-pds.example.com`
- [ ] Verify events are emitted when records are created/updated/deleted

### 2. Authentication Flow
Test the full session cycle:

- [x] `createSession` - login with handle + app password
- [ ] `getSession` - verify session token works
- [ ] `refreshSession` - confirm token refresh works

### 3. Service Auth (`getServiceAuth`)
Critical for AppView proxying.

- [ ] Test that PDS can generate valid service auth tokens
- [ ] Verify Bluesky's AppView accepts the tokens

### 4. OAuth Flow (if using web app login)

- [ ] Visit `/oauth/authorize` in a browser
- [ ] Complete the login flow
- [ ] Verify token exchange works at `/oauth/token`

### 5. Record Operations Under Auth
Test CRUD operations with a valid session:

- [x] Create a test post (`app.bsky.feed.post`)
- [x] Read it back via `getRecord`
- [x] Delete it via `deleteRecord`
- [x] Verify the firehose emits corresponding events

### 6. Blob Upload

- [x] Upload a test image via `uploadBlob`
- [x] Verify `getBlob` returns it correctly
- [ ] Bluesky app needs this for profile pictures/media

### 7. DID Document
Confirm your `did.json` (for `did:web`) or PLC entry points to the correct PDS endpoint.

- [x] Run `goat resolve your-handle.example.com`
- [x] Verify `serviceEndpoint` matches your PDS URL

### 8. TLS/HTTPS
Bluesky only connects to HTTPS endpoints.

- [x] Verify Caddy config is working
- [x] Verify TLS certs are valid

## Final Steps

After all checks pass:

1. Run `request_crawl.sh` to notify Bluesky's relay
2. Login via bsky.app
3. Create a test post through the Bluesky web app
