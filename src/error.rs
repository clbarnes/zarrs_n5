pub type Result<T, E = Error> = std::result::Result<T, E>;

/// zarrs_n5 error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    General(String),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Wrapped(Box<dyn std::error::Error>),
}

impl Error {
    /// Create a general error with a message.
    pub fn general(message: impl Into<String>) -> Self {
        Self::General(message.into())
    }

    /// Wrap some other error.
    pub fn wrap(error: impl std::error::Error + 'static) -> Self {
        Self::Wrapped(Box::new(error))
    }
}
