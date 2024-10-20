use prost_build::Config;
use std::env;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn build_attribute_protos() -> Result<()> {
    let mut config = Config::new();

    let protos = &["../proto/attribute.proto"];
    let includes = &["../proto/"];

    prost_reflect_build::Builder::new()
        .file_descriptor_set_bytes("crate::pb::FILE_DESCRIPTOR_SET")
        .configure(&mut config, protos, includes)?;

    let file_descriptor_path =
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("file_descriptor_set.attribute.bin");
    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_path)
        .compile_with_config(config, protos, includes)?;
    Ok(())
}

fn build_mavlink_protos() -> Result<()> {
    let mut config = Config::new();

    let protos = &["proto/mavlink.proto"];
    let includes = &["proto/", "../proto/"];

    prost_reflect_build::Builder::new()
        .file_descriptor_set_bytes("crate::pb::mavlink::FILE_DESCRIPTOR_SET")
        .configure(&mut config, protos, includes)?;

    let file_descriptor_path =
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("file_descriptor_set.mavlink.bin");
    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_path)
        .compile_with_config(config, protos, includes)?;
    Ok(())
}

fn main() -> Result<()> {
    build_attribute_protos()?;
    build_mavlink_protos()?;

    Ok(())
}
