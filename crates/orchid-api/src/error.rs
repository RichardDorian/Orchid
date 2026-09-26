use std::fmt;

/// An invalid field.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{field}: {message}")]
pub struct ValidationError {
    /// Path of the field, e.g. `spec.containers[0].image`.
    pub field: String,
    pub message: String,
}

impl ValidationError {
    pub fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }

    /// Prepends `prefix` to the field path.
    #[must_use]
    pub fn prefixed(mut self, prefix: &str) -> Self {
        self.field = join_path(prefix, &self.field);
        self
    }
}

/// Every invalid field of an object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidationErrors(Vec<ValidationError>);

impl ValidationErrors {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.0.push(ValidationError::new(field, message));
    }

    /// Adds `errors`, prepending `prefix` to their field path.
    pub fn extend_prefixed(&mut self, prefix: &str, errors: ValidationErrors) {
        self.0
            .extend(errors.0.into_iter().map(|e| e.prefixed(prefix)));
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &ValidationError> {
        self.0.iter()
    }

    /// `Ok(())` if there is no error.
    pub fn into_result(self) -> Result<(), Self> {
        if self.is_empty() { Ok(()) } else { Err(self) }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, error) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}

impl From<ValidationError> for ValidationErrors {
    fn from(error: ValidationError) -> Self {
        Self(vec![error])
    }
}

impl IntoIterator for ValidationErrors {
    type Item = ValidationError;
    type IntoIter = std::vec::IntoIter<ValidationError>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

fn join_path(prefix: &str, field: &str) -> String {
    match (prefix.is_empty(), field.is_empty()) {
        (true, _) => field.to_owned(),
        (_, true) => prefix.to_owned(),
        // Indexes are appended without a dot: `containers` + `[0].image`
        _ if field.starts_with('[') => format!("{prefix}{field}"),
        _ => format!("{prefix}.{field}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_field_paths() {
        let error = ValidationError::new("image", "must not be empty")
            .prefixed("[0]")
            .prefixed("containers")
            .prefixed("spec");
        assert_eq!(error.field, "spec.containers[0].image");
    }

    #[test]
    fn displays_every_error() {
        let mut errors = ValidationErrors::new();
        errors.push("a", "bad");
        errors.push("b", "worse");
        assert_eq!(errors.to_string(), "a: bad; b: worse");
    }
}
