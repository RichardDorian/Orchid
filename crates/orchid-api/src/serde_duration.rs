//! Serializes a [`Duration`] as a human readable string (`"500ms"`, `"15s"`, `"5m"`).
//!
//! Use with `#[serde(with = "orchid_api::serde_duration")]`.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer, de};

pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(&humantime::format_duration(*duration))
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(de::Error::custom)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper(#[serde(with = "super")] Duration);

    #[test]
    fn round_trips() {
        for (text, duration) in [
            ("\"500ms\"", Duration::from_millis(500)),
            ("\"15s\"", Duration::from_secs(15)),
            ("\"5m\"", Duration::from_secs(300)),
        ] {
            assert_eq!(serde_json::to_string(&Wrapper(duration)).unwrap(), text);
            assert_eq!(
                serde_json::from_str::<Wrapper>(text).unwrap(),
                Wrapper(duration)
            );
        }
    }

    #[test]
    fn rejects_missing_unit() {
        assert!(serde_json::from_str::<Wrapper>("\"15\"").is_err());
    }
}
