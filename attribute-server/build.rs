fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &["proto/internal.proto", "../proto/attribute.proto"],
        &["proto/", "../proto"],
    )?;
    Ok(())
}
