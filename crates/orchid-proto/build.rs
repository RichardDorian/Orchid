const PROTOS: &[&str] = &[
    "proto/orchid/v1/common.proto",
    "proto/orchid/v1/cluster.proto",
    "proto/orchid/v1/pod.proto",
    "proto/orchid/v1/node.proto",
    "proto/orchid/v1/agent.proto",
    "proto/orchid/v1/scheduler.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure().compile_protos(PROTOS, &["proto"])?;

    Ok(())
}
