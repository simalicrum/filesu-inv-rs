use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use quick_xml::events::BytesStart;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use quick_xml::Writer;
use serde;
use serde::Deserializer;
use std::io::BufRead;
use url::Url;

use std::error::Error;

async fn list_blobs(
    container: &str,
    account: &str,
    token: &str,
    client: &reqwest::Client,
) -> Result<String, Box<dyn Error>> {
    let url = Url::parse(&format!(
        "https://{}.blob.core.windows.net/{}?restype=container&comp=list&maxresults=5",
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let credential = DefaultAzureCredential::default();
    let token_res = credential.get_token("https://storage.azure.com/").await?;
    let token = token_res.token.secret();
    let client = reqwest::Client::new();
    let container = "atlas";
    let account = "bccrcprccatlassa";
    let res = list_blobs(container, account, token, &client).await?;
    let mut reader = Reader::from_str(&res);
    reader.trim_text(true);
    let mut count = 0;
    let mut buf = Vec::new();
    // let mut txt = Vec::new();
    let mut junk_buf: Vec<u8> = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.name().as_ref() {
                b"Name" => {
                    let release_bytes =
                        read_to_end_into_buffer(&mut reader, &e, &mut junk_buf).unwrap();
                    let str = std::str::from_utf8(&release_bytes).unwrap();
                    let mut deserializer = Deserializer::from_str(str);
                    let release = Release::deserialize(&mut deserializer).unwrap();
                    println!("{}", str);
                }
                _ => (),
            },
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            Ok(Event::Eof) => break,
            _ => (),
        }
        buf.clear();
    }
    // println!("{:?}", txt);
    Ok(())
}
