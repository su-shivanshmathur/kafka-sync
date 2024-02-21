#![allow(dead_code)]

use chrono::{Datelike, Local};
use dotenv::dotenv;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use rusoto_core::{Region, HttpClient};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{S3, S3Client, PutObjectRequest};
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::Path;
use std::fs::{self, File};
use std::thread;
use std::time::{Duration, Instant};
use std::env;
use std::io::Read;
use serde::{Serialize, Deserialize};
use time::OffsetDateTime;

async fn kafka_to_sync(topic: &str, filename: &str, duration: u64) -> Result<(), Box<dyn std::error::Error>> {
    let kafka_file = format!("{topic}-{filename}-{duration}.json");
    let path = Path::new(&kafka_file);
    let data = path.display();
    let current_date = Local::now();
    let bucket_path = format!("{}/{}/{}/{}/{}", env::var("CLOUD_BUCKET").unwrap_or_else(|_| "eu-kafka-backfill-bucket".to_string()) ,current_date.year(), current_date.month(), current_date.day(), &topic);
    println!("{}",bucket_path);
    let mut file = match File::create(&path) {
        Err(why) => panic!("couldn't create {}: {}", data, why),
        Ok(file) => file,
    };
    let start_time = Instant::now();
    let mut partiton_offset_hs: HashMap<i32, i64> = HashMap::new();
    let mut consumer = match Consumer::from_hosts(vec!(env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string()).to_owned()))
                .with_topic(topic.to_owned())
                .with_fallback_offset(FetchOffset::Earliest)
                .with_group(env::var("KAFKA_CONSUMER_GROUP").unwrap_or_else(|_| "kafka-sync".to_string()).to_owned())
                .with_offset_storage(GroupOffsetStorage::Kafka)
                .create() {
                    Ok(consumer) => consumer,
                    Err(err) => {
                        if let Err(why) = fs::remove_file(&path
                        ) {
                            log_state("Error in Consumer creation code",&format!("Couldn't delete {}: {}", path.display(), why));
                        } else {
                            log_state("Error in Consumer creation code",&format!("File {:?} deleted successfully", path.display()));
                        }
                        log_state("Error in Consumer creation code", &format!("{:?}",err));
                        return Err(Box::new(err));
                    }
            };
        
    let sync_time = Duration::from_secs(duration);
    while (Instant::now() - start_time) < sync_time {
                for message_set in consumer.poll().unwrap().iter() {
                    for message in message_set.messages() {
                        partiton_offset_hs.insert(message_set.partition(), message.offset);
                        println!("HashMap -> {:?}",partiton_offset_hs);
                        println!("{}", String::from_utf8(message.value.to_owned()).unwrap());
                        let formated_data = format!("{}\n" , (String::from_utf8(message.value.to_owned()).unwrap())).into_bytes();
                        match file.write_all(&formated_data) {
                            Err(why) => panic!("couldn't write to {}: {}", data, why),
                            Ok(_) => println!("successfully wrote to {} at {:?}", data, Instant::now()),
                        }
                    }
                    consumer.consume_messageset(message_set);
                }
            thread::sleep(Duration::from_nanos(1));
        }
    
    if let Err(err) = upload_to_s3(&bucket_path ,&kafka_file,&format!("{}-{}.json",&filename,hashmap_to_string(&partiton_offset_hs))).await {
                eprintln!("Error: {}", err);
     }
    consumer.commit_consumed().unwrap();
    // return kafkaFile;
    if let Err(why) = fs::remove_file(&path) {
        eprintln!("Couldn't delete {}: {}", path.display(), why);
    } else {
        println!("File {} deleted successfully", path.display());
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let env = env::var("ENVIORNMENT").unwrap_or_else(|_| "local".to_string());
    if env == "local" {
        dotenv().ok();
    }
    let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    log_state("Server Start","INFO");
    let args: Vec<_> = env::args().collect();
    log_state("Aguments" , &(format!("{:?}",&args)));
    let duration_in_seconds:u64 = env::var("SYNC_DURATION").unwrap_or_else(|_| "10".to_string()).parse::<u64>().unwrap();
    let genericFile = format!("{}",&now);
    let sync = env::var("OBJECT_STORAGE").unwrap_or_else(|_| "AWS".to_string());
    

    let topics = env::var("KAFKA_TOPICS").unwrap_or_default();
    log_state("Kafka topics are ", &topics);
    let list_of_topics: Vec<_> = topics.split(',').map(|s| s.trim()).collect();
    log_state("Getting Kafka Topics from ENV" , &(format!("{:?}",&list_of_topics)));

    while 1 == 1 {
        let mut threads = vec![];
        for topics in &list_of_topics {
            let topic = topics.to_string();
            let duration = duration_in_seconds;
            let file = genericFile.to_string();
            let individual_thread = tokio::spawn(async move {
                if let Err(err) = kafka_to_sync(&topic, &file, duration).await {
                    log_state("Error in swapning up thread: {}", &format!("{:?}",&err));
                }
            });
            threads.push(individual_thread);
        }
        println!("Loop finished at {:?} minutes", Instant::now());
        for individual_thread in threads {
            log_state("Thread Invoke",&format!("Thread ID for {:?} " , &individual_thread));
            individual_thread.await.unwrap_or_else(|_| {
                eprintln!("Thread panicked.");
            });
        }
    }

    println!("All threads finished.");
}


// Function to upload to AWS S3
async fn upload_to_s3(bucket_name: &str, file_path: &str, object_key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut file_content = Vec::new();
    File::open(file_path)?.read_to_end(&mut file_content)?;
    let file_size = {
        let mut file = File::open(file_path)?;
        file.read_to_end(&mut file_content)?;
        file_content.len()
    };

    if file_size == 0 {
        println!("File '{}' is empty. Not uploading to S3.", file_path);
    }
    else {

        let credentials_provider = DefaultCredentialsProvider::new()?;
        let http_client = HttpClient::new()?;
        let s3_client = S3Client::new_with(http_client, credentials_provider, Region::EuCentral1);
        let request = PutObjectRequest {
            bucket: bucket_name.to_string(),
            key: object_key.to_string(),
            body: Some(file_content.into()),
            ..Default::default()
        };

        s3_client.put_object(request).await?;

        println!("File uploaded successfully to S3: s3://{}/{}", bucket_name, object_key);

            return Ok(());
    }
    Ok(())
}

fn log_state(step: &str, value: &str) {
    let timestamp = Local::now();
    let log = Log {
        timestamp: timestamp.to_string(),
        level: "".to_string() ,
        step: step.to_string(),
        value: value.to_string(),
    };
    println!("{}", serde_json::to_string(&log).unwrap()); 
}


#[derive(Serialize, Deserialize)]
struct Log {
    timestamp: String,
    level: String,
    step: String,
    value: String,
}

struct Config {
    kafka_broker: String,
    kafka_consumer_group: String,
    sync_time: u64,
}

fn hashmap_to_string(map: &HashMap<i32, i64>) -> String {
    let mut result = String::new();
    for (i, (key, value)) in map.iter().enumerate() {
        if i > 0 {
            result.push('_');
        }
        result.push_str(&format!("Par{}-Off{}", key, value));
    }
    result
}