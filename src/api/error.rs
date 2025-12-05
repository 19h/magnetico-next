use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

/// RFC 7807 Problem Details for HTTP APIs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub type_: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Not found: {0}")]
    NotFound(String),
    
    #[error("Bad request: {0}")]
    BadRequest(String),
    
    #[error("Unauthorized")]
    Unauthorized,
    
    #[error("Internal server error: {0}")]
    Internal(String),
}

impl ApiError {
    pub fn to_problem_details(&self, instance: &str) -> ProblemDetails {
        match self {
            ApiError::NotFound(msg) => ProblemDetails {
                type_: "/errors/not-found".to_string(),
                title: "Not Found".to_string(),
                status: 404,
                detail: msg.clone(),
                instance: instance.to_string(),
            },
            ApiError::BadRequest(msg) => ProblemDetails {
                type_: "/errors/bad-request".to_string(),
                title: "Bad Request".to_string(),
                status: 400,
                detail: msg.clone(),
                instance: instance.to_string(),
            },
            ApiError::Unauthorized => ProblemDetails {
                type_: "/errors/unauthorized".to_string(),
                title: "Unauthorized".to_string(),
                status: 401,
                detail: "Authentication required".to_string(),
                instance: instance.to_string(),
            },
            ApiError::Internal(msg) => ProblemDetails {
                type_: "/errors/internal".to_string(),
                title: "Internal Server Error".to_string(),
                status: 500,
                detail: msg.clone(),
                instance: instance.to_string(),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        
        let problem = self.to_problem_details("");
        
        (status, Json(problem)).into_response()
    }
}

