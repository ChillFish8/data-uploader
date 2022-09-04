use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{fs, mem};
use std::path::Path;
use std::time::{Duration, Instant};
use anyhow::{bail, Result};
use serde::{Serialize, Deserialize};
use clap::Parser;
use reqwest::{Client, StatusCode, Url};
use tracing::{info, warn};

#[derive(Parser, Debug)]
pub struct Options {
    /// The target file path to load. Expects line delimited JSON.
    folder: String,

    host: String,

    index_name: String,
}


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let options: Options = Options::parse();
    let (tx, rx) = flume::bounded(50000);

    let url: Url = format!("http://{}/indexes/{}/documents", options.host, options.index_name).parse()?;
    let client = Client::new();

    let fp = options.folder;
    tokio::task::spawn_blocking(move || start_reading_file(tx, fp));

    let total_time = Instant::now();
    let mut counter: usize = 0;
    let mut comments_block = vec![];
    while let Ok(comment) = rx.recv_async().await {
        comments_block.push(comment);

        if comments_block.len() >= 250_000 {
            let len = comments_block.len();
            counter += len;
            let start = Instant::now();
            send_block(&url, &client, mem::take(&mut comments_block)).await?;

            info!("Send {} documents in {:?}. Current total: {} docs.", len, start.elapsed(), counter);
        }
    }

    if !comments_block.is_empty() {
        let len = comments_block.len();
        counter += len;

        let start = Instant::now();
        send_block(&url, &client, comments_block).await?;
        info!("Send {} documents in {:?}. Current total: {} docs.", len, start.elapsed(), counter);
    }

    info!(
        "Upload took: {} total to upload {} docs.",
        humantime::format_duration(total_time.elapsed()),
        counter,
    );

    Ok(())
}


async fn send_block(url: &Url, client: &Client, comments: Vec<Comment>) -> Result<()> {
    for _ in 0..3 {
        let resp = client.post(url.clone())
            .timeout(Duration::from_secs(500))
            .json(&comments)
            .send()
            .await?;

        if resp.status() != StatusCode::OK {
            warn!("Upload skipped due to error: {}, retrying...", resp.status());
            continue;
        }
    }

    bail!("Unable to correct error after retries!");
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Comment {
    id: String,
    name: String,
    subreddit: String,
    subreddit_id: String,
    parent_id: String,
    link_id: String,
    author: String,
    created_utc: String,
    body: String,
    ups: u64,
    downs: u64,
    score: u64,
    controversiality: u64,
}


fn start_reading_file(tx: flume::Sender<Comment>, folder: String) -> Result<()> {
    let list_dir = fs::read_dir(&folder)?;

    for file in list_dir {
        let file = file?;

        if file.path().is_dir() {
            continue
        }

        read_file(&tx, &file.path())?;
    }

    Ok(())
}

fn read_file(tx: &flume::Sender<Comment>, fp: &Path) -> Result<()> {
    let file = File::open(fp)?;
    let mut reader = BufReader::with_capacity(512 << 20, file);

    let mut s = String::new();
    loop {
        let n = reader.read_line(&mut s)?;

        if n == 0 {
            break;
        }

        let comment: Comment = serde_json::from_str(&s)?;
        if tx.send(comment).is_err() {
            break;
        }
    }

    Ok(())
}