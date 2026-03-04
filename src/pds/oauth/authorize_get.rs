//! OAuth Authorization endpoint (GET).
//!
//! GET /oauth/authorize
//!
//! Displays the authorization form for the user to approve or deny access.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    Json,
};
use serde::Deserialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::helpers::{get_caller_info, get_form_value, html_encode, is_oauth_enabled, is_passkeys_enabled};

/// Query parameters for authorization request.
#[derive(Deserialize)]
pub struct AuthorizeParams {
    /// OAuth client identifier.
    client_id: Option<String>,
    /// Request URI from PAR.
    request_uri: Option<String>,
}

/// GET /oauth/authorize
///
/// Displays the OAuth authorization form.
pub async fn oauth_authorize_get(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
    Query(params): Query<AuthorizeParams>,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let (ip_address, user_agent) = get_caller_info(&headers);
    let stat_key = StatisticKey {
        name: "oauth/authorize GET".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate required parameters
    let client_id = match params.client_id {
        Some(id) if !id.is_empty() => id,
        _ => {
            state.log.warning("[OAUTH] authorize GET: Missing client_id");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let request_uri = match params.request_uri {
        Some(uri) if !uri.is_empty() => uri,
        _ => {
            state.log.warning("[OAUTH] authorize GET: Missing request_uri");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Load OAuth request
    let oauth_request = match state.db.get_oauth_request(&request_uri) {
        Ok(req) => req,
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] authorize GET: OAuth request not found or expired. request_uri={} error={}",
                request_uri, e
            ));
            return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
        }
    };

    // Get scope from the original request body
    let scope = get_form_value(&oauth_request.body, "scope").unwrap_or_default();

    // Generate HTML form
    let html = generate_auth_form(&request_uri, &client_id, &scope, false, is_passkeys_enabled(&state.db));
    Html(html).into_response()
}

/// Generate the HTML authorization form.
pub fn generate_auth_form(
    request_uri: &str,
    client_id: &str,
    scope: &str,
    failed: bool,
    passkeys_enabled: bool,
) -> String {
    let safe_request_uri = html_encode(request_uri);
    let safe_client_id = html_encode(client_id);
    let safe_scope = html_encode(scope);

    let failed_message = if failed {
        r#"<p class="auth-failed">Authentication failed. Please try again.</p>"#
    } else {
        ""
    };

    let passkey_section = if passkeys_enabled {
        format!(
            r#"
        <div id="passkey-section">
            <button type="button" id="passkey-btn" class="passkey-btn" onclick="loginWithPasskey()">Authorize with Passkey</button>
            <div id="passkey-error" class="error-msg"></div>
        </div>
        
        <div class="divider"><span>or</span></div>
        "#
        )
    } else {
        String::new()
    };

    let passkey_script = if passkeys_enabled {
        format!(
            r#"
        const requestUri = '{safe_request_uri}';
        const clientId = '{safe_client_id}';
        
        async function loginWithPasskey() {{
            const btn = document.getElementById('passkey-btn');
            const errorDiv = document.getElementById('passkey-error');
            errorDiv.style.display = 'none';
            btn.disabled = true;
            btn.textContent = 'Authenticating...';
            
            try {{
                // Fetch authentication options from server
                const optionsResponse = await fetch('/oauth/passkeyauthenticationoptions', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ request_uri: requestUri, client_id: clientId }})
                }});
                
                if (!optionsResponse.ok) {{
                    const err = await optionsResponse.json();
                    throw new Error(err.error || 'Failed to get authentication options');
                }}
                
                const options = await optionsResponse.json();
                
                // Convert base64url strings to ArrayBuffers
                options.challenge = base64urlToBuffer(options.challenge);
                if (options.allowCredentials) {{
                    options.allowCredentials = options.allowCredentials.map(cred => ({{
                        ...cred,
                        id: base64urlToBuffer(cred.id)
                    }}));
                }}
                
                // Get credential using WebAuthn API
                const assertion = await navigator.credentials.get({{ publicKey: options }});
                
                // Prepare assertion data for server
                const assertionData = {{
                    id: bufferToBase64url(assertion.rawId),
                    rawId: bufferToBase64url(assertion.rawId),
                    type: assertion.type,
                    request_uri: requestUri,
                    client_id: clientId,
                    response: {{
                        clientDataJSON: bufferToBase64url(assertion.response.clientDataJSON),
                        authenticatorData: bufferToBase64url(assertion.response.authenticatorData),
                        signature: bufferToBase64url(assertion.response.signature),
                        userHandle: assertion.response.userHandle ? bufferToBase64url(assertion.response.userHandle) : null
                    }}
                }};
                
                // Send assertion to server for verification
                const authResponse = await fetch('/oauth/authenticatepasskey', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify(assertionData)
                }});
                
                if (authResponse.ok) {{
                    const result = await authResponse.json();
                    if (result.redirect_url) {{
                        window.location.href = result.redirect_url;
                    }} else {{
                        throw new Error('No redirect URL in response');
                    }}
                }} else {{
                    const err = await authResponse.json();
                    throw new Error(err.error || 'Authentication failed');
                }}
            }} catch (err) {{
                console.error('Passkey authentication error:', err);
                errorDiv.textContent = err.message || 'Passkey authentication failed';
                errorDiv.style.display = 'block';
                btn.disabled = false;
                btn.textContent = 'Authorize with Passkey';
            }}
        }}
        
        function base64urlToBuffer(base64url) {{
            const base64 = base64url.replace(/-/g, '+').replace(/_/g, '/');
            const padding = '='.repeat((4 - base64.length % 4) % 4);
            const binary = atob(base64 + padding);
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) {{
                bytes[i] = binary.charCodeAt(i);
            }}
            return bytes.buffer;
        }}
        
        function bufferToBase64url(buffer) {{
            const bytes = new Uint8Array(buffer);
            let binary = '';
            for (let i = 0; i < bytes.length; i++) {{
                binary += String.fromCharCode(bytes[i]);
            }}
            const base64 = btoa(binary);
            return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
        }}
        
        // Check if passkeys are supported and hide button if not
        if (!window.PublicKeyCredential) {{
            document.getElementById('passkey-section').style.display = 'none';
            document.querySelector('.divider').style.display = 'none';
        }}
        "#
        )
    } else {
        String::new()
    };

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Authorize {safe_client_id}</title>
<style>
    body {{ background-color: #16181c; color: #e7e9ea; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 40px 20px; }}
    .container {{ max-width: 500px; margin: 0 0 0 40px; }}
    h1 {{ color: #8899a6; margin-bottom: 24px; }}
    p {{ margin-bottom: 16px; line-height: 1.5; }}
    code {{ background-color: #2f3336; padding: 2px 6px; border-radius: 4px; }}
    a {{ color: #1d9bf0; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    label {{ display: block; margin-bottom: 6px; color: #8899a6; }}
    input[type="text"], input[type="password"] {{ width: 100%; padding: 12px; margin-bottom: 16px; background-color: #2f3336; border: 1px solid #3d4144; border-radius: 6px; color: #e7e9ea; font-size: 16px; box-sizing: border-box; }}
    input:focus {{ outline: none; border-color: #1d9bf0; }}
    button {{ background-color: #4caf50; color: white; border: none; padding: 12px 24px; border-radius: 6px; font-size: 16px; font-weight: bold; cursor: pointer; }}
    button:hover {{ background-color: #388e3c; }}
    .passkey-btn {{ background-color: #4caf50; width: 100%; margin-bottom: 16px; }}
    .passkey-btn:hover {{ background-color: #388e3c; }}
    .passkey-btn:disabled {{ background-color: #2f3336; color: #8899a6; cursor: not-allowed; }}
    .divider {{ display: flex; align-items: center; margin: 24px 0; color: #8899a6; }}
    .divider::before, .divider::after {{ content: ''; flex: 1; border-bottom: 1px solid #3d4144; }}
    .divider span {{ padding: 0 16px; }}
    .error-msg {{ color: #f44336; margin-bottom: 16px; display: none; }}
    .auth-failed {{ color: #f44336; margin-bottom: 16px; }}
</style>
</head>
<body>
<div class="container">
<h1>Authorize {safe_client_id}</h1>
{failed_message}
<p><strong>{safe_client_id}</strong> is requesting access to your account.</p>
<p>Requested permissions: <code>{safe_scope}</code></p>

{passkey_section}

<form method="post" action="/oauth/authorize">
    <input type="hidden" name="request_uri" value="{safe_request_uri}" />
    <input type="hidden" name="client_id" value="{safe_client_id}" />
    <label for="username">Username</label>
    <input type="text" id="username" name="username" />
    <label for="password">Password</label>
    <input type="password" id="password" name="password" />
    <button type="submit">Authorize with Password</button>
</form>
</div>
<script>
{passkey_script}
</script>
</body>
</html>"#
    )
}
