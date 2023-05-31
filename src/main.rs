use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use url::Url;

use std::error::Error;

async fn list_blobs(
    container: &str,
    account: &str,
    token: &str,
    client: &reqwest::Client,
) -> Result<String, Box<dyn Error>> {
    let url = Url::parse(&format!(
        "https://{}.blob.core.windows.net/{}?restype=container&comp=list",
        account, container,
    ))?;
    let res = client
        .get(url)
        .header("Authorization", format!("Bearer {}", token))
        .header("x-ms-version", "2020-04-08")
        .send()
        .await?
        .text()
        .await?;
    Ok(res)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let credential = DefaultAzureCredential::default();
    let token_res = credential.get_token("https://storage.azure.com/").await?;
    let token = token_res.token.secret();
    let client = reqwest::Client::new();
    let container = "atlas";
    let account = "bccrcprccatlassa";
    let res = list_blobs(container, account, token, &client).await?;

    println!("{:?}", res);
    Ok(())
}
