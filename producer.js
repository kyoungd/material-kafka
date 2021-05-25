
const { Kafka } = require('kafkajs');

async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "kafka_connect",
            "brokers": ["localhost:9092"]
        });
        const producer = kafka.producer();
        console.log("Connecting... ");
        await producer.connect();
        console.log("Connected.");
        const msg = {
            type: "TWEET_START_FOLLOW",
            symbol: "MSFT",
            message: "finally embracing open source.  heading in the right direction.",
            start_date: "2021-05-21 00:00:00"
        };
        const jsonMessage = JSON.stringify(msg);
        const result = await producer.send({
            "topic": "TWEET",
            "messages": [{
                "key": "TWEET_START_FOLLOW",
                "value": jsonMessage,
                "partition": 0
            }]
        });
        console.log(`message sent successfully ${result}`);
        await producer.disconnect();
    }
    catch (ex) {
        console.error(`error on creating topic ${ex}`);
    }
    finally {
        process.exit(0);
    }
}

run();
