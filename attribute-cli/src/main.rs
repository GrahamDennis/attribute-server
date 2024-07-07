use anyhow::format_err;
use attribute_grpc_api::grpc::attribute_store_client::AttributeStoreClient;
use attribute_grpc_api::grpc::{PingRequest, QueryEntitiesRequest};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use tonic::transport::{Channel, Endpoint};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Endpoint to connect to
    #[arg(short, long, default_value = "http://[::1]:50051")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send ping request
    Ping,
    /// Query for entities
    QueryEntities {
        #[clap(short, long, default_value = "content")]
        json: PathBuf,
    },
    /// Generate shell completions script
    GenerateCompletions {
        /// shell to generate completions for
        #[clap(short, long)]
        shell: Option<Shell>,
    },
}

fn print_completions<G: clap_complete::Generator>(gen: G, cmd: &mut clap::Command) {
    clap_complete::generate(gen, cmd, cmd.get_name().to_string(), &mut std::io::stdout());
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Commands::Ping => {
            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;
            let response = attribute_store_client.ping(PingRequest {}).await?;
            println!("response: {:?}", response);

            Ok(())
        }
        Commands::QueryEntities { json } => {
            let file = File::open(json)?;
            let reader = BufReader::new(file);
            let query_entities_request: QueryEntitiesRequest = serde_json::from_reader(reader)?;

            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;
            let response = attribute_store_client
                .query_entities(query_entities_request)
                .await?;
            let query_entities_response = response.into_inner();
            println!("{}", serde_json::to_string(&query_entities_response)?);

            Ok(())
        }
        Commands::GenerateCompletions { shell } => Ok(print_completions(
            shell
                .or_else(|| Shell::from_env())
                .ok_or_else(|| format_err!("specify shell with `--shell`"))?,
            &mut Cli::command(),
        )),
    }
}

async fn create_attribute_store_client(
    endpoint: &str,
) -> anyhow::Result<AttributeStoreClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_string())?
        .connect()
        .await?;

    Ok(AttributeStoreClient::new(channel))
}
