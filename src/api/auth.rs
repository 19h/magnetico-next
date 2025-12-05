use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use bcrypt::{hash, verify, DEFAULT_COST};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // Subject (username)
    pub exp: usize,  // Expiry time
}

pub struct AuthManager {
    credentials: Arc<RwLock<HashMap<String, String>>>, // username -> bcrypt hash
    jwt_secret: String,
}

impl AuthManager {
    pub fn new(credentials_file: Option<&Path>, jwt_secret: String) -> Result<Self, std::io::Error> {
        let credentials = if let Some(path) = credentials_file {
            Self::load_credentials(path)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            credentials: Arc::new(RwLock::new(credentials)),
            jwt_secret,
        })
    }

    fn load_credentials(path: &Path) -> Result<HashMap<String, String>, std::io::Error> {
        let content = fs::read_to_string(path)?;
        let mut credentials = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((username, hash)) = line.split_once(':') {
                credentials.insert(username.to_string(), hash.to_string());
            }
        }

        Ok(credentials)
    }

    pub async fn verify_credentials(&self, username: &str, password: &str) -> bool {
        let credentials = self.credentials.read().await;

        if let Some(hash) = credentials.get(username) {
            verify(password, hash).unwrap_or(false)
        } else {
            false
        }
    }

    pub fn generate_token(&self, username: &str) -> Result<String, jsonwebtoken::errors::Error> {
        let expiration = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::hours(24))
            .unwrap()
            .timestamp() as usize;

        let claims = Claims {
            sub: username.to_string(),
            exp: expiration,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.jwt_secret.as_bytes()),
        )
    }

    pub fn verify_token(&self, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.jwt_secret.as_bytes()),
            &Validation::default(),
        )
        .map(|data| data.claims)
    }

    pub async fn reload_credentials(&self, path: &Path) -> Result<(), std::io::Error> {
        let new_credentials = Self::load_credentials(path)?;
        let mut credentials = self.credentials.write().await;
        *credentials = new_credentials;
        Ok(())
    }
}

pub async fn auth_middleware(
    auth_manager: Arc<AuthManager>,
    request: Request,
    next: Next,
) -> Response {
    // Extract Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    if let Some(auth_header) = auth_header {
        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            if auth_manager.verify_token(token).is_ok() {
                return next.run(request).await;
            }
        }
    }

    // Unauthorized
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Body::from("Unauthorized"))
        .unwrap()
}

pub fn hash_password(password: &str) -> Result<String, bcrypt::BcryptError> {
    hash(password, DEFAULT_COST)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auth_manager() {
        let manager = AuthManager::new(None, "test_secret".to_string()).unwrap();

        let token = manager.generate_token("testuser").unwrap();
        let claims = manager.verify_token(&token).unwrap();

        assert_eq!(claims.sub, "testuser");
    }

    #[test]
    fn test_hash_password() {
        let password = "test123";
        let hash = hash_password(password).unwrap();

        assert!(verify(password, &hash).unwrap());
        assert!(!verify("wrong", &hash).unwrap());
    }
}
