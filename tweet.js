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
    const postPromises = messages.map(async message => {
        const result = await post(url, message.data);
        await postKafkaCommand(result.message);
    });
}

async function save_tweets(message) {
    try {
        const query1 = { text: 'SELECT count(0) FROM tweets WHERE tweet_id=$1', values: [message.tweet_id] };
        const result1 = await pool.query(query1);

        if (result1.rows[0].count == 0) {
            const values = [
                message.tweet_text.replace("'", ""),
                message.author_id,
                message.tweet_id,
                message.retweet_count,
                message.reply_count,
                message.like_count,
                message.quote_count,
                message.user_id,
                message.username,
                message.symbol,
                message.tweet_json
            ];
            let insertQuery = 'INSERT INTO tweets '
                + ' (tweet_text, author_id, tweet_id, retweet_count, reply_count, like_count, quote_count, user_id, user_name, symbol, tweet_json) '
                + ' VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT DO NOTHING returning id';
            const query2 = {
                text: insertQuery,
                values
            }
            const result2 = await pool.query(query2);
            const insertId = result2.rows[0].id;
            const jsonMessage = JSON.stringify({ id: insertId, tweet_text: message.tweet_text });
            const messages = {
                "topic": "TWEET",
                "messages": [{
                    "key": "TWEETS_GET_SENTIMENT",
                    "value": jsonMessage,
                    "partition": 0
                }]
            }
            postKafkaCommand(messages);
            console.log('INSERT pool.query():', result2);
        }
    }
    catch (ex) {
        console.log(ex);
    }
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

async function processKafkaTweet(key, data) {
    switch (key) {
        case "TWEETS_GET":
            await get_tweets(data);
            break;
        case "TWEETS_SAVE":
            await save_tweets(data);
            break;
        case "TWEETS_GET_SENTIMENT":
            await get_sentiment(data);
            break;
        case "TWEETS_SAVE_SENTIMENT":
            await save_sentiment(data);
            break;
        default:
            break;
    }
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
            "eachMessage": async ({ topic, partition, message }) => {
                msg_key = message.key.toString();
                msg_value = message.value.toString()
                data = JSON.parse(msg_value)
                await processKafkaTweet(msg_key, data);
            }
        });

        console.log(`message started successfully ${result}`);
    }
    catch (ex) {
        console.error(`error on creating topic ${ex}`);
    }
}

run();
