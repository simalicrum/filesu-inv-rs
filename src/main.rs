use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use url::Url;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let credential = DefaultAzureCredential::default();
    let response = credential.get_token("https://storage.azure.com/").await?;
    let url = Url::parse(&format!(
        "https://bccrcprccatlassa.blob.core.windows.net/atlas?restype=container&comp=list"
    ))?;
    let response = reqwest::Client::new()
        .get(url)
        .header(
            "Authorization",
            format!("Bearer {}", response.token.secret()),
        )
        .header("x-ms-version", "2020-04-08")
        .send()
        .await?
        .text()
        .await?;

    println!("{:?}", response);
    Ok(())
}
