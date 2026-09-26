/// Maximum length of a resource name.
pub const MAX_NAME_LEN: usize = 63;

/// Checks that `name` is a valid resource name (DNS label): 1 to 63 lowercase
/// alphanumerics or `-`, starting and ending with an alphanumeric.
///
/// Returns the reason why the name is invalid.
pub fn validate_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("must not be empty");
    }
    if name.len() > MAX_NAME_LEN {
        return Err("must be at most 63 characters");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err("must only contain lowercase alphanumerics and '-'");
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("must start and end with an alphanumeric");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_dns_labels() {
        for name in ["a", "my-app", "node-01", "0abc", &"a".repeat(63)] {
            assert_eq!(validate_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn rejects_invalid_names() {
        for name in ["", "-a", "a-", "My-App", "a_b", "a.b", "é", &"a".repeat(64)] {
            assert!(validate_name(name).is_err(), "{name}");
        }
    }
}
