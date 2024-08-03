use prost_build::Config;
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();

    let protos = &["proto/mavlink.proto", "../proto/attribute.proto"];
    let includes = &["proto/", "../proto/"];

    prost_reflect_build::Builder::new()
        .file_descriptor_set_bytes("crate::pb::FILE_DESCRIPTOR_SET")
        .configure(&mut config, protos, includes)?;

    let file_descriptor_path =
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("file_descriptor_set.bin");
    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_path)
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize)]\
             #[serde(rename_all = \"snake_case\", deny_unknown_fields)]",
        )
        .compile_with_config(config, protos, includes)?;
    Ok(())
}
