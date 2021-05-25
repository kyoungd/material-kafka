const axios = require('axios');
const { Kafka } = require('kafkajs');
const { post, postKafkaCommand } = require('./util');

const { Pool, Client } = require("pg");
const config = require('dotenv').config()

const pool = new Pool({
    user: process.env.DB_USER || "postgres",
    host: process.env.DB_HOST || "localhost",
    database: process.env.DB_NAME || "webscrapper",
    password: process.env.DB_PASSWORD || "Service$11",
    port: process.env.DB_PORT || "5432"
});

async function get_tweets(messages) {
    const url = "http://localhost:8001/twint"
    const postPromises = messages.map(message => {
        const result = await post(url, message.data);
        await postKafkaCommand(result.message);
    });
}

async function save_tweets(messages) {
    messages.map(message => {
        const values = [
            message.tweet_text.replace("'", ""),
            message.author_id,
            message.tweet_id,
            message.retweet_count,
            message.reply_count,
            message.like_count,
            message.quote_count,
            message.user_.id,
            message.username,
            message.symbol
        ];
        let insertQuery = 'INSERT INTO tweets (tweet_text, author_id, tweet_id, retweet_count, reply_count, like_count, quote_count, user_id, user_name, symbol) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)';
        pool.query(insertQuery, values, (err, res) => {
            if (err) {
                console.log('INSERT pool.query():', err);
            }
            if (res) {
                const insertId = res.rows[0].id;
                url = "localhost:8002/sentiment";
                post(url, { id: insertId, message: message.tweet_text })
                console.log('INSERT pool.query():', res);
            }
        });
    });
}

async function get_sentiment(messages) {
    const message = messages[0];
    const url = "http://localhost:8002/sentiment"
    const result = await post(url, message);
    await postKafkaCommand(result.message);
}

async function save_sentiment(messages) {
    const message = messages[0]
    const queryUpdate = "UPDATE tweets SET sentiment_score=$2, sentiment_rated=true WHERE id=%1";
    const values = [message.id, message.score];
    pool.query(queryUpdate, values, (err, res) => {
        if (err) {
            console.log('UPDATE pool.query():', err);
        }
        if (res) {
            console.log('UPDATE pool.query():', res);
        }
    });
}

async function processKafkaTweet(messages) {
    const data = messages.map(message => {
        switch (message.key) {
            case "TWEET_GET_TWEETS":
                await get_tweets(messages);
                break;
            case "TWEET_SAVE_TWEETS":
                await save_tweets(messages);
                break;
            case "TWEET_GET_SENTIMENT":
                await get_sentiment(messages);
                break;
            case "TWEET_SAVE_SENTIMENT":
                await save_sentiment(messages);
                break;
            default:
                break;
        }
    })
}

async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "kafka_connect",
            "brokers": ["localhost:9092"]
        });
        const consumer = kafka.consumer({ "groupId": "test" });
        console.log("Connecting... ");
        await consumer.connect();
        console.log("Connected.");
        consumer.subscribe({
            "topic": "TWEET",
            "fromBeginning": false
        });
        await consumer.run({
            "eachMessage": async result => {
                processKafkaTweet(result.message.value);
            }
        });

        console.log(`message started successfully ${result}`);
    }
    catch (ex) {
        console.error(`error on creating topic ${ex}`);
    }
}

run();
