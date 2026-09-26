//! Resource quantities.
//!
//! Quantities are written as strings in configuration files and in the CLI:
//! - CPU: whole or decimal cores (`"12"`, `"0.5"`) or millicores (`"300m"`),
//! - memory: bytes (`"1048576"`), binary suffixes (`"Ki"`, `"Mi"`, `"Gi"`, `"Ti"`)
//!   or decimal suffixes (`"K"`, `"M"`, `"G"`, `"T"`). Decimal numbers are allowed
//!   as long as they amount to a whole number of bytes (`"1.5Gi"`).
//!
//! Internally they are stored in their base unit (millicores, bytes).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} quantity {input:?}: {reason}")]
pub struct ParseQuantityError {
    kind: &'static str,
    input: String,
    reason: &'static str,
}

/// CPU amount in millicores (1000 = 1 core).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MilliCpu(pub u64);

/// Memory amount in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes(pub u64);

macro_rules! arithmetic {
    ($ty:ident) => {
        impl $ty {
            pub const ZERO: Self = Self(0);

            pub fn checked_add(self, other: Self) -> Option<Self> {
                self.0.checked_add(other.0).map(Self)
            }

            pub fn checked_sub(self, other: Self) -> Option<Self> {
                self.0.checked_sub(other.0).map(Self)
            }

            pub fn saturating_sub(self, other: Self) -> Self {
                Self(self.0.saturating_sub(other.0))
            }

            pub fn is_zero(self) -> bool {
                self.0 == 0
            }
        }

        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
                s.parse().map_err(de::Error::custom)
            }
        }
    };
}

arithmetic!(MilliCpu);
arithmetic!(Bytes);

impl MilliCpu {
    /// `cores` whole cores.
    pub const fn from_cores(cores: u64) -> Self {
        Self(cores.saturating_mul(1000))
    }
}

impl FromStr for MilliCpu {
    type Err = ParseQuantityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s.strip_suffix('m') {
            Some(millis) => parse_scaled(millis, 1),
            None => parse_scaled(s, 1000),
        };
        result.map(Self).map_err(|reason| ParseQuantityError {
            kind: "cpu",
            input: s.to_owned(),
            reason,
        })
    }
}

impl fmt::Display for MilliCpu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_multiple_of(1000) {
            write!(f, "{}", self.0 / 1000)
        } else {
            write!(f, "{}m", self.0)
        }
    }
}

const KI: u64 = 1 << 10;
const MI: u64 = 1 << 20;
const GI: u64 = 1 << 30;
const TI: u64 = 1 << 40;
const K: u64 = 1_000;
const M: u64 = 1_000_000;
const G: u64 = 1_000_000_000;
const T: u64 = 1_000_000_000_000;

/// Memory suffixes. Binary suffixes come first so that parsing matches "Mi"
/// before "M".
const MEMORY_SUFFIXES: [(&str, u64); 8] = [
    ("Ti", TI),
    ("Gi", GI),
    ("Mi", MI),
    ("Ki", KI),
    ("T", T),
    ("G", G),
    ("M", M),
    ("K", K),
];

impl FromStr for Bytes {
    type Err = ParseQuantityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (number, multiplier) = MEMORY_SUFFIXES
            .iter()
            .find_map(|(suffix, multiplier)| {
                s.strip_suffix(suffix).map(|number| (number, *multiplier))
            })
            .unwrap_or((s, 1));

        parse_scaled(number, multiplier)
            .map(Self)
            .map_err(|reason| ParseQuantityError {
                kind: "memory",
                input: s.to_owned(),
                reason,
            })
    }
}

impl fmt::Display for Bytes {
    /// Uses the largest suffix that divides the value exactly (`2G` rather
    /// than `1953125Ki`), plain bytes otherwise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let suffix = MEMORY_SUFFIXES
            .iter()
            .filter(|(_, multiplier)| self.0 != 0 && self.0.is_multiple_of(*multiplier))
            .max_by_key(|(_, multiplier)| *multiplier);
        match suffix {
            Some((suffix, multiplier)) => write!(f, "{}{suffix}", self.0 / multiplier),
            None => write!(f, "{}", self.0),
        }
    }
}

/// Parses a non negative decimal number and multiplies it by `multiplier`.
/// The result must be a whole number that fits in a `u64`.
fn parse_scaled(number: &str, multiplier: u64) -> Result<u64, &'static str> {
    let (int, frac) = match number.split_once('.') {
        Some((int, frac)) => (int, frac),
        None => (number, ""),
    };
    if int.is_empty() || (number.contains('.') && frac.is_empty()) {
        return Err("expected a number");
    }
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return Err("expected a number");
    }
    // A u128 holds 38 digits, keep a margin for the multiplication.
    if int.len() + frac.len() > 30 {
        return Err("too large");
    }

    let digits: u128 = format!("{int}{frac}")
        .parse()
        .map_err(|_| "expected a number")?;
    let denominator = 10u128.pow(frac.len() as u32);
    let scaled = digits
        .checked_mul(u128::from(multiplier))
        .ok_or("too large")?;
    if scaled % denominator != 0 {
        return Err("too precise");
    }
    u64::try_from(scaled / denominator).map_err(|_| "too large")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu() {
        let cases = [
            ("12", 12_000),
            ("0.5", 500),
            ("1.25", 1_250),
            ("300m", 300),
            ("0", 0),
            ("0m", 0),
        ];
        for (input, millis) in cases {
            assert_eq!(input.parse::<MilliCpu>(), Ok(MilliCpu(millis)), "{input}");
        }
    }

    #[test]
    fn rejects_invalid_cpu() {
        for input in ["", "m", "-1", "1.", ".5", "0.0001", "1.5m", "1 core", "12c"] {
            assert!(input.parse::<MilliCpu>().is_err(), "{input}");
        }
    }

    #[test]
    fn displays_cpu() {
        assert_eq!(MilliCpu(12_000).to_string(), "12");
        assert_eq!(MilliCpu(300).to_string(), "300m");
        assert_eq!(MilliCpu(1_500).to_string(), "1500m");
        assert_eq!(MilliCpu(0).to_string(), "0");
    }

    #[test]
    fn parses_memory() {
        let cases = [
            ("1048576", MI),
            ("500Mi", 500 * MI),
            ("16Gi", 16 * GI),
            ("1.5Gi", 3 * GI / 2),
            ("2Ti", 2 * TI),
            ("1Ki", KI),
            ("2G", 2 * G),
            ("500M", 500 * M),
            ("1K", K),
            ("3T", 3 * T),
        ];
        for (input, bytes) in cases {
            assert_eq!(input.parse::<Bytes>(), Ok(Bytes(bytes)), "{input}");
        }
    }

    #[test]
    fn rejects_invalid_memory() {
        for input in [
            "",
            "Gi",
            "-1Gi",
            "1.5",
            "0.1Ki",
            "1GB",
            "1gi",
            "1 Gi",
            "99999999999Ti",
        ] {
            assert!(input.parse::<Bytes>().is_err(), "{input}");
        }
    }

    #[test]
    fn displays_memory() {
        assert_eq!(Bytes(16 * GI).to_string(), "16Gi");
        assert_eq!(Bytes(500 * MI).to_string(), "500Mi");
        assert_eq!(Bytes(1_536 * MI).to_string(), "1536Mi");
        assert_eq!(Bytes(2 * G).to_string(), "2G");
        assert_eq!(Bytes(1_000).to_string(), "1K");
        assert_eq!(Bytes(1_023).to_string(), "1023");
        assert_eq!(Bytes(0).to_string(), "0");
    }

    #[test]
    fn display_round_trips() {
        for value in [
            0,
            1,
            999,
            1_000,
            1_024,
            1_536 * MI,
            16 * GI,
            7 * T,
            u64::MAX,
        ] {
            let bytes = Bytes(value);
            assert_eq!(bytes.to_string().parse::<Bytes>(), Ok(bytes));
        }
        for value in [0, 1, 999, 1_000, 1_500, u64::MAX] {
            let cpu = MilliCpu(value);
            assert_eq!(cpu.to_string().parse::<MilliCpu>(), Ok(cpu));
        }
    }

    #[test]
    fn serializes_as_strings() {
        assert_eq!(serde_json::to_string(&Bytes(16 * GI)).unwrap(), r#""16Gi""#);
        assert_eq!(
            serde_json::from_str::<MilliCpu>(r#""300m""#).unwrap(),
            MilliCpu(300)
        );
        assert!(serde_json::from_str::<MilliCpu>("300").is_err());
    }
}
