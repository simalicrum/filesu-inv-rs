use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use clap::Parser;
use console::style;
use csv::Writer as csvWriter;
use indicatif::MultiProgress;
use indicatif::{ProgressBar, ProgressStyle};
use quick_xml::events::BytesStart;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use quick_xml::Writer;
use serde::{Deserialize, Serialize};
use serde_xml_rs::from_str;
use std::error::Error;
use std::io::BufRead;
use std::time::Duration;
use url::Url;

/// Takes a Azure Storage account and container name and returns all the blobs in the container in CSV format
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Storage account container to list blobs
    #[arg(short, long)]
    account: String,

    /// Azure Storage account
    #[arg(short, long)]
    container: String,

    /// Azure Storage account
    #[arg(short, long)]
    output: String,
}

async fn list_blobs(
    container: &str,
    account: &str,
    token: &str,
    marker: Option<&str>,
    client: &reqwest::Client,
) -> Result<String, Box<dyn Error>> {
    let url;
    match marker {
        None => {
            url = Url::parse(&format!(
                "https://{}.blob.core.windows.net/{}?restype=container&comp=list",
                account, container,
            ))?
        }
        Some(m) => {
            url = Url::parse(&format!(
                "https://{}.blob.core.windows.net/{}?restype=container&comp=list&marker={}",
                account, container, m,
            ))?;
        }
    };
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

fn read_to_end_into_buffer<R: BufRead>(
    reader: &mut Reader<R>,
    start_tag: &BytesStart,
    junk_buf: &mut Vec<u8>,
) -> Result<Vec<u8>, quick_xml::Error> {
    let mut depth = 0;
    let mut output_buf: Vec<u8> = Vec::new();
    let mut w = Writer::new(&mut output_buf);
    let tag_name = start_tag.name();
    w.write_event(Event::Start(start_tag.clone()))?;
    loop {
        junk_buf.clear();
        let event = reader.read_event_into(junk_buf)?;
        w.write_event(&event)?;

        match event {
            Event::Start(e) if e.name() == tag_name => depth += 1,
            Event::End(e) if e.name() == tag_name => {
                if depth == 0 {
                    return Ok(output_buf);
                }
                depth -= 1;
            }
            Event::Eof => {
                panic!("oh no")
            }
            _ => {}
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Properties {
    #[serde(rename = "Creation-Time")]
    creation_time: String,
    #[serde(rename = "Last-Modified")]
    last_modified: String,
    #[serde(rename = "Content-Length")]
    content_length: String,
    #[serde(rename = "Content-Type")]
    content_type: String,
    #[serde(rename = "Content-MD5")]
    content_md5: String,
    #[serde(rename = "BlobType")]
    blobtype: String,
    #[serde(rename = "AccessTier")]
    accesstier: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Blob {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Properties")]
    properties: Properties,
}

#[derive(serde::Serialize)]
struct Row {
    name: String,
    creation_time: String,
    last_modified: String,
    content_length: String,
    content_type: String,
    content_md5: String,
    blobtype: String,
    accesstier: String,
}

async fn list_thread(
    container: &str,
    account: &str,
    token: &str,
    client: &reqwest::Client,
    m: &MultiProgress,
    output: &str,
) -> Result<(), Box<dyn Error>> {
    println!("Spawned a thread");
    let pb = m.add(ProgressBar::new_spinner());
    pb.enable_steady_tick(Duration::from_millis(120));
    let sty = ProgressStyle::with_template("{spinner:.blue} {msg}")
        .unwrap()
        .tick_strings(&[
            "⠋",
            "⠙",
            "⠹",
            "⠸",
            "⠼",
            "⠴",
            "⠦",
            "⠧",
            "⠇",
            "⠏",
            &style("✔").green().to_string(),
        ]);
    pb.set_style(sty);
    let mut count: u64 = 0;
    let mut marker: Option<&str> = None;
    let mut next_marker: String;
    'list: loop {
        let res = list_blobs(container, account, token, marker, &client).await?;
        let mut reader = Reader::from_str(&res);
        reader.trim_text(true);
        let mut buf = Vec::new();
        let mut junk_buf: Vec<u8> = Vec::new();
        let mut wtr = csvWriter::from_path(output)?;
        loop {
            match reader.read_event_into_async(&mut buf).await {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"Blob" => {
                        let release_bytes =
                            read_to_end_into_buffer(&mut reader, &e, &mut junk_buf).unwrap();
                        let str = std::str::from_utf8(&release_bytes).unwrap();
                        let blob: Blob = from_str(str).unwrap();
                        wtr.serialize(Row {
                            name: blob.name,
                            creation_time: blob.properties.creation_time,
                            last_modified: blob.properties.last_modified,
                            content_length: blob.properties.content_length,
                            content_type: blob.properties.content_type,
                            content_md5: blob.properties.content_md5,
                            blobtype: blob.properties.blobtype,
                            accesstier: blob.properties.accesstier,
                        })?;
                        count += 1;
                    }
                    b"NextMarker" => {
                        let release_bytes =
                            read_to_end_into_buffer(&mut reader, &e, &mut junk_buf).unwrap();
                        let str = std::str::from_utf8(&release_bytes).unwrap();
                        next_marker = from_str(str).unwrap();
                        pb.set_message(format!("{} blobs found", count));
                        marker = Some(&next_marker);
                    }
                    b"EnumerationResults" => {
                        pb.set_message(format!("{} blobs found", count));
                        marker = None;
                    }
                    _ => (),
                },
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                Ok(Event::Eof) => break,
                _ => (),
            }
            buf.clear();
        }
        if marker.is_none() {
            break 'list;
        }
    }
    pb.finish_with_message(format!("{} blobs found", count));
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    print!("Connecting to Azure Storage using Azure Default Credentials..");
    let credential = DefaultAzureCredential::default();
    let token_res = credential.get_token("https://storage.azure.com/").await?;
    let token: &str = token_res.token.secret();
    println!("{}", style("done").green().dim());
    let client = reqwest::Client::new();
    println!(
        "Writing blob properties to {}:",
        style(&args.output).green()
    );
    let m = MultiProgress::new();
    let mut fut = Vec::new();
    for _i in 0..3 {
        let client = client.clone();
        // let wtr = wtr;
        // let pb = pb.clone();
        let container = args.container.to_owned();
        let account = args.account.to_owned();
        let token = token.to_owned();
        let output = args.output.to_owned();
        let m = m.clone();
        let t = tokio::spawn(async move {
            let _result = list_thread(&container, &account, &token, &client, &m, &output).await;
        });
        fut.push(t);
    }
    for f in fut {
        f.await?;
    }
    Ok(())
}
