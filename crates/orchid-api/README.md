# orchid-api

Domain types shared by every Orchid component.

- **Resource model**: `Pod`, `Node`, `ClusterConfig` and their parts (spec, binding, info, status).
- **Quantities**: `MilliCpu` and `Bytes`, parsed from and displayed as strings (`"300m"`, `"0.5"`, `"16Gi"`, `"2G"`).
  Durations use `serde_duration` (`"15s"`, `"500ms"`).
- **Validation**: `PodSpec::validate`, `NodeInfo::validate`, `ClusterConfig::validate` and `validate_name` report every invalid field with its path (`spec.containers[1].image`).
- **Protobuf conversions** (`proto` module): domain to protobuf conversions are infallible, protobuf to domain conversions check the structure of messages (required fields, known enum values, parsable CIDRs).

```rust
use orchid_api::{Bytes, MilliCpu};

let cpu: MilliCpu = "1.5".parse()?;   // MilliCpu(1500)
let memory: Bytes = "16Gi".parse()?;  // Bytes(17179869184)
```

The types derive `serde`, the same representation is used in configuration files and in etcd.
