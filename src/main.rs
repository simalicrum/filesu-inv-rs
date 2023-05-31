use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use url::Url;

use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let credential = DefaultAzureCredential::default();
    let response = credential.get_token("https://management.azure.com").await?;
    let subscription_id = env::var("AZURE_SUBSCRIPTION_ID")?;
    let url = Url::parse(&format!(
        "https://management.azure.com/subscriptions/{}/providers/Microsoft.Storage/storageAccounts?api-version=2019-06-01",
        subscription_id))?;
    let response = reqwest::Client::new()
        .get(url)
        .header(
            "Authorization",
            format!("Bearer {}", response.token.secret()),
        )
        .send()
        .await?
        .text()
        .await?;

    println!("{:?}", response);
    Ok(())
}
