use anyhow::format_err;
use attribute_grpc_api::pb::attribute_store_client::AttributeStoreClient;
use attribute_grpc_api::pb::{PingRequest, QueryEntitiesRequest, WatchEntitiesRequest};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufReader;
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tonic_types::{ErrorDetail, StatusExt};
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
        #[clap(short, long)]
        json: String,
    },
    WatchEntities {
        #[clap(short, long)]
        json: String,
    },
    /// Generate shell completions script
    GenerateCompletions {
        /// shell to generate completions for
        #[clap(short, long)]
        shell: Option<Shell>,
    },
}

#[derive(Error, Debug)]
pub struct StatusError {
    status: Status,
    error_details: Vec<ErrorDetail>,
}

impl From<Status> for StatusError {
    /// Convert a grpc Status into something that we can display a better error for
    fn from(value: Status) -> Self {
        let error_details = value.get_error_details_vec();
        StatusError {
            status: value,
            error_details,
        }
    }
}

impl Display for StatusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "status: {:?}, message: {:?}, metadata: {:?}",
            self.status.code(),
            self.status.message(),
            self.status.metadata(),
        )?;

        if !self.error_details.is_empty() {
            write!(f, "\nDetails:")?;
            for error_detail in self.error_details.iter() {
                write!(f, "\n * {:?}", error_detail)?;
            }
        }

        Ok(())
    }
}

fn print_completions<G: clap_complete::Generator>(gen: G, cmd: &mut clap::Command) {
    clap_complete::generate(gen, cmd, cmd.get_name().to_string(), &mut std::io::stdout());
}

fn parse_from_json_argument<T: serde::de::DeserializeOwned>(
    json_argument: &str,
) -> anyhow::Result<T> {
    let parsed = if let Some(json_file) = json_argument.strip_prefix('@') {
        serde_json::from_reader(BufReader::new(File::open(json_file)?))?
    } else {
        serde_json::from_str(json_argument)?
    };

    Ok(parsed)
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
            let query_entities_request: QueryEntitiesRequest = parse_from_json_argument(json)?;

            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;
            let response = attribute_store_client
                .query_entities(query_entities_request)
                .await
                .map_err(StatusError::from)?;
            let query_entities_response = response.into_inner();
            println!("{}", serde_json::to_string(&query_entities_response)?);

            Ok(())
        }
        Commands::WatchEntities { json } => {
            let watch_entities_request: WatchEntitiesRequest = parse_from_json_argument(json)?;

            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;
            let response = attribute_store_client
                .watch_entities(watch_entities_request)
                .await
                .map_err(StatusError::from)?;
            let mut stream = response.into_inner();
            while let Some(event) = stream.message().await? {
                println!("{}", serde_json::to_string(&event)?);
            }

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
