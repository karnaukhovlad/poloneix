use std::env;
use anyhow::{Context, Result};
use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use governor::clock::QuantaClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use reqwest;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use dotenv::dotenv;
use tokio::task::JoinSet;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use once_cell::sync::Lazy;

const POLONIEX_WS_URL: &str = "wss://ws.poloniex.com/ws/public";
// const PG_CONNECTION: &str = "postgres://myuser:mypassword@localhost/poloneix";
const PAIRS: [&str; 5] = ["BTC_USDT", "TRX_USDT", "ETH_USDT", "DOGE_USDT", "BCH_USDT"];
const INTERVALS: [&str; 4] = ["MINUTE_1", "MINUTE_15", "HOUR_1", "DAY_1"];
static START_DATA: Lazy<i64> = Lazy::new(|| {
    // Parse the date string into NaiveDate (no time zone)
    let date_str = "2024-12-01";
    let naive_date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();

    // Convert to DateTime<Utc> (UTC timezone)
    let datetime = Utc.from_utc_datetime(&NaiveDateTime::from(naive_date)); // Midnight time

    // Return the timestamp in milliseconds
    datetime.timestamp_millis()
});
#[derive(Debug, Serialize, Deserialize)]
struct RecentTrade {
    tid: String,
    pair: String,
    price: String,
    amount: String,
    side: String,
    timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct VBS {
    buy_base: f64,
    sell_base: f64,
    buy_quote: f64,
    sell_quote: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Kline {
    pair: String,
    time_frame: String,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    utc_begin: i64,
    volume_bs: VBS,
}
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TradeIntervalDataDTO {
    pub low: String,
    pub high: String,
    pub open: String,
    pub close: String,
    pub amount: String,
    pub quantity: String,
    pub buy_taker_amount: String,
    pub buy_taker_quantity: String,
    pub trade_count: i32,
    pub ts: i64,
    pub weighted_average: String,
    pub interval: String,
    pub start_time: i64,
    pub close_time: i64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct StreamResponseDTO {
    pub symbol: String,
    pub amount: String,
    pub taker_side: String,
    pub quantity: String,
    pub create_time: i64,
    pub price: String,
    pub id: i64,
    pub ts: i64,
}


fn into_klines(dto: TradeIntervalDataDTO, pair: &str) -> Kline {
    let buy_quote = dto.buy_taker_amount.parse::<f64>().unwrap_or(0.0);
    let sell_quote = dto.amount.parse::<f64>().unwrap_or(0.0);
    let buy_base = dto.buy_taker_quantity.parse::<f64>().unwrap_or(0.0);
    let sell_base = dto.quantity.parse::<f64>().unwrap_or(0.0);

    Kline {
        pair: pair.to_string(), // You may want to set the actual pair dynamically
        time_frame: dto.interval.clone(),
        o: dto.open.parse::<f64>().unwrap_or(0.0),
        h: dto.high.parse::<f64>().unwrap_or(0.0),
        l: dto.low.parse::<f64>().unwrap_or(0.0),
        c: dto.close.parse::<f64>().unwrap_or(0.0),
        utc_begin: dto.start_time,
        volume_bs: VBS {
            buy_base,
            sell_base,
            buy_quote,
            sell_quote,
        },
    }
}

impl From<StreamResponseDTO> for RecentTrade {
    fn from(dto: StreamResponseDTO) -> Self {
        RecentTrade {
            tid: dto.id.to_string(),  // Converting `id` to String for `tid`
            pair: dto.symbol,         // Using `symbol` for `pair`
            price: dto.price,         // Directly map `price` field
            amount: dto.amount,       // Directly map `amount` field
            side: dto.taker_side,     // Use `taker_side` for `side`
            timestamp: dto.create_time, // Use `create_time` as `timestamp`
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event")]
enum WebSocketIn {
    #[serde(rename = "subscribe")]
    SubscribeIn {channel: Vec<String>},
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "trades")]
    Trades {data: Vec<StreamResponseDTO>},
    #[serde(rename = "error")]
    Error {message: String},
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event")]
enum WebSocketOut {
    #[serde(rename = "subscribe")]
    SubscribeOut {channel: Vec<String>, symbols: Vec<String>},
}

async fn store_candles(transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>, kline: Kline) -> Result<()>
{

    // Step 1: Insert VBS data into the 'vbs' table and get the inserted ID
    let vbs_id: i32 = sqlx::query!(
        r#"
        INSERT INTO vbs (buy_base, sell_base, buy_quote, sell_quote)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        "#,
        kline.volume_bs.buy_base,
        kline.volume_bs.sell_base,
        kline.volume_bs.buy_quote,
        kline.volume_bs.sell_quote
    )
        .fetch_one(&mut **transaction)
        .await.context("Failed to store vbs data")?
        .id;

    // Step 2: Insert Kline data into the 'klines' table with the vbs_id reference
    sqlx::query!(
        r#"
        INSERT INTO klines (pair, time_frame, o, h, l, c, utc_begin, vbs_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        kline.pair,
        kline.time_frame,
        kline.o,
        kline.h,
        kline.l,
        kline.c,
        kline.utc_begin,
        vbs_id
    )
        .execute(&mut **transaction)
        .await.context("Failed to store kline data")?;
    Ok(())
}

async fn store_trade(pool: &sqlx::PgPool, trade: RecentTrade) -> Result<()> {
    sqlx::query!(
        "INSERT INTO recent_trades (tid, pair, price, amount, side, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (tid) DO NOTHING",
        trade.tid,
        trade.pair,
        trade.price,
        trade.amount,
        trade.side,
        trade.timestamp
    )
    .execute(pool)
    .await
    .context("Failed to store trade data")?;
    Ok(())
}

async fn fetch_candles(
    pool: &sqlx::PgPool,
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware>>,
) -> Result<()> {
    let mut futures = FuturesUnordered::new();

    for pair in PAIRS.iter() {
        let pool = pool.clone();
        let limiter = limiter.clone();

        futures.push(tokio::spawn(async move {
            limiter.until_ready().await;

            let url = format!(
                "https://api.poloniex.com/markets/{}/candles?interval=MINUTE_5&limit=500&startTime={}",
                pair,
                *START_DATA
            );
            let response = reqwest::get(&url).await?.json::<Vec<TradeIntervalDataDTO>>().await?;
            println!("Fetched Candles for {}", pair);
            for dto in response {
                let mut transaction = pool.begin().await?;
                store_candles(&mut transaction, into_klines(dto, &pair)).await?;
                transaction.commit().await?;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    while let Some(result) = futures.next().await {
        if let Err(e) = result {
            eprintln!("Error fetching candles: {}", e);
        }
    }
    Ok(())
}

async fn listen_trades(
    pool: &sqlx::PgPool,
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware>>,
) -> Result<()> {
    let url = POLONIEX_WS_URL.into_client_request()?;
    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = WebSocketOut::SubscribeOut { channel: vec!["trades".to_string()], symbols: PAIRS.into_iter().map( |v| v.to_string()).collect() };
    println!("Sending new msg");
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&subscribe_msg).unwrap().into(),
        ))
        .await?;
    println!("Send new msg");
    while let Some(msg) = read.next().await {
        println!("Received new msg");
        let limiter = limiter.clone();
        let pool_3 = pool.clone();
        tokio::spawn(async move {
            limiter.until_ready().await;
            let data = msg?.into_data();
            println!("{}", String::from_utf8(data.to_vec()).unwrap());
            let json_msg = serde_json::from_slice(&data).unwrap();
            match json_msg {
                WebSocketIn::Ping => {
                    println!("Received ping, sending pong");
                    // let pong_msg = json!({ "event": "pong" });
                    // if let Err(e) = write.send(Message::Text(pong_msg.to_string())).await {
                    //     eprintln!("Error sending pong message: {:?}", e);
                    // }
                }
                WebSocketIn::Pong => {
                    println!("Received pong");
                }
                WebSocketIn::SubscribeIn { channel } => {
                    println!("Subscription request: channel = {:?}", channel);
                }
                WebSocketIn::Trades { data } => {
                    // Process the trades data
                    for trade in data {

                        println!("Received trade: {:?}", trade);
                        if let Err(e) = store_trade(&pool_3, trade.into()).await {
                            eprintln!("Failed to store trade: {:?}", e);
                        }
                    }
                }
                WebSocketIn::Error { message } => {
                    eprintln!("Error: {}", message);
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let pg_connection = env::var("DATABASE_URL")?;
    let pool = sqlx::PgPool::connect(&pg_connection).await.context("Failed to connect to database")?;
    let pool_2 = pool.clone();


    let mut tasks = JoinSet::new();

    // tasks.spawn(async move {
    //
    //     let rest_limit = NonZeroU32::new(200).unwrap();
    //     let rest_limiter = Arc::new(RateLimiter::direct( Quota::per_second(rest_limit).allow_burst(rest_limit),
    //     ));
    //     loop {
    //         if let Err(e) = fetch_candles(&pool_2.clone(), rest_limiter.clone()).await {
    //             eprintln!("Error fetching candles: {}", e);
    //         }
    //         tokio::time::sleep(Duration::from_secs(300)).await;
    //     }
    // });

    tasks.spawn(async move{
        let ws_limit = NonZeroU32::new(500).unwrap();
        let ws_limiter = Arc::new(RateLimiter::direct(
            Quota::per_second(ws_limit).allow_burst(ws_limit),
        ));
        loop {
            if let Err(e) = listen_trades(&pool.clone(), ws_limiter.clone()).await {
                eprintln!("Error fetching trades: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(300)).await;
        }
    });
    tasks.join_all().await;
    Ok(())
}
