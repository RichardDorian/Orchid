use serde::{Deserialize, Serialize};

use crate::{Bytes, MilliCpu};

/// Amount of compute resources.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Resources {
    pub cpu: MilliCpu,
    pub memory: Bytes,
}

impl Resources {
    pub const ZERO: Self = Self {
        cpu: MilliCpu::ZERO,
        memory: Bytes::ZERO,
    };

    pub const fn new(cpu: MilliCpu, memory: Bytes) -> Self {
        Self { cpu, memory }
    }

    /// `None` on overflow.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            cpu: self.cpu.checked_add(other.cpu)?,
            memory: self.memory.checked_add(other.memory)?,
        })
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            cpu: self.cpu.saturating_sub(other.cpu),
            memory: self.memory.saturating_sub(other.memory),
        }
    }

    /// Whether both CPU and memory are lower than or equal to `capacity`.
    pub fn fits_in(self, capacity: Self) -> bool {
        self.cpu <= capacity.cpu && self.memory <= capacity.memory
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fits_in_requires_both_resources() {
        let capacity = Resources::new(MilliCpu(1000), Bytes(1000));
        assert!(Resources::new(MilliCpu(1000), Bytes(1000)).fits_in(capacity));
        assert!(!Resources::new(MilliCpu(1001), Bytes(10)).fits_in(capacity));
        assert!(!Resources::new(MilliCpu(10), Bytes(1001)).fits_in(capacity));
    }

    #[test]
    fn checked_add_detects_overflow() {
        let max = Resources::new(MilliCpu(u64::MAX), Bytes(1));
        assert_eq!(max.checked_add(Resources::new(MilliCpu(1), Bytes(1))), None);
    }
}
