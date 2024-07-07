fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize)]\
             #[serde(rename_all = \"snake_case\", deny_unknown_fields)]",
        )
        .compile(&["proto/attribute.proto"], &["proto/"])?;
    Ok(())
}
