use std::fs::File;
use std::io::{BufRead, BufReader};
use std::{fs, mem};
use std::path::Path;
use std::time::{Duration, Instant};
use anyhow::{bail, Result};
use serde::{Serialize, Deserialize};
use clap::Parser;
use reqwest::{Client, StatusCode, Url};
use tracing::{error, info, warn};

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

        let status = resp.status();
        if status != StatusCode::OK {
            let msg = resp.text().await?;

            warn!("Upload skipped due to error: {}, {:?} retrying...", status, msg);
            continue;
        } else {
            return Ok(())
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
    score: i64,
    controversiality: i64,
}


fn start_reading_file(tx: flume::Sender<Comment>, folder: String) -> Result<()> {
    let list_dir = fs::read_dir(&folder)?;

    for file in list_dir {
        let file = file?;
        info!("Reading {:?} file...", file.path());
        if let Err(e) = read_file(&tx, &file.path()) {
            error!("Failed to read file: {}", e);
        }
    }

    Ok(())
}

fn read_file(tx: &flume::Sender<Comment>, fp: &Path) -> Result<()> {
    let file = File::open(fp)?;
    let mut reader = BufReader::with_capacity(512 << 20, file);

    let mut s = String::new();
    loop {
        s.clear();
        let n = reader.read_line(&mut s)?;

        if n == 0 {
            break;
        }

        let mut comment: Comment = serde_json::from_str(s.trim())
            .map_err(|e| {
                error!("Data: {}", s.trim());
                e
            })?;
        comment.subreddit.insert(0, '/');

        if tx.send(comment).is_err() {
            break;
        }
    }

    Ok(())
}