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

const kafka = new Kafka({
    "clientId": "kafka_connect",
    "brokers": ["localhost:9092"]
});

async function get_tweets(message) {
    try {
        const url = process.env.TWEET_URL || "http://localhost:8101/tweets";
        const data = {
            "messages": [
                {
                    "key": "TWEETS_GET",
                    "value": message,
                }
            ]
        }
        const result = await post(url, JSON.stringify(data));
    }
    catch (ex) {
        console.log(ex);
    }
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
                message.tweet_json,
                message.tweet_dt,
            ];
            let insertQuery = 'INSERT INTO tweets '
                + ' (tweet_text, author_id, tweet_id, retweet_count, reply_count, like_count, quote_count, user_id, user_name, symbol, tweet_json, tweet_dt) '
                + ' VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT DO NOTHING returning id';
            const query2 = {
                text: insertQuery,
                values
            }
            const result2 = await pool.query(query2);
            const insertId = result2.rows[0].id;
            return insertId;
            console.log('INSERT pool.query():', result2);
        }
        else
            return 0;
    }
    catch (ex) {
        console.log(ex);
        return 0;
    }
}

async function add_get_sentiment_message(id, tweet_text) {
    try {
        const jsonMessage = JSON.stringify({ id, tweet_text });
        const messages = {
            "topic": "TWEET",
            "messages": [{
                "key": "TWEETS_SENTIMENT",
                "value": jsonMessage,
                "partition": 0
            }]
        }
        await postKafkaCommand(kafka, messages);
    }
    catch (ex) {
        console.log(ex);
    }
}

async function get_sentiment(text) {
    try {
        const url = process.env.SENTIMENT_URL || "http://localhost:8102/sentiment";
        const bodyData = JSON.stringify({ text });
        const result = await post(url, bodyData);

        let score_total = 0;
        let score_count = 0;
        result.data.forEach(score => {
            score_total += score.sentiment_score;
            ++score_count;
        })
        const sentiment_score = score_count != 0 ? score_total / score_count : 0;
        return sentiment_score;
    }
    catch (ex) {
        console.log(ex);
    }
    return 0;
}


async function save_sentiment_score(queryUpdate, id, sentiment_score) {
    try {
        const values = [id, sentiment_score];
        const res = await pool.query({ text: queryUpdate, values });
        console.log('UPDATE pool.query():', res);
    }
    catch (ex) {
        console.log(ex);
    }
}

async function save_sentiment(id, sentiment_score) {
    const queryUpdate = "UPDATE tweets SET sentiment_score=$2, sentiment_rated=true WHERE id=$1";
    return await save_sentiment_score(queryUpdate, id, sentiment_score);
}

async function save_yahoo_news(id, sentiment_score) {
    const queryUpdate = "UPDATE site_yahoos SET sentiment=$2 WHERE id=$1";
    return await save_sentiment_score(queryUpdate, id, sentiment_score);
}

async function processKafkaTweet(key, data) {
    switch (key) {
        case "TWEETS_GET":
            await get_tweets(data);
            break;
        case "TWEETS_SAVE":
            const insertId = await save_tweets(data);
            if (insertId > 0)
                add_get_sentiment_message(insertId, data.tweet_text);
            break;
        case "TWEETS_SENTIMENT":
            const sentiment_score = await get_sentiment(data.tweet_text);
            await save_sentiment(data.id, sentiment_score);
            break;
        case "YAHOO_NEWS":
            const news1 = data.title[0] + "." + data.description[0];
            const sentiment2 = await get_sentiment(news1);
            await save_yahoo_news(data.id, sentiment2);
            break;
        default:
            break;
    }
}

async function run() {
    try {
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
