
const { Kafka } = require('kafkajs');

async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "kafka_connect",
            "brokers": ["localhost:9092"]
        });
        const admin = kafka.admin();
        console.log("Connecting... ");
        await admin.connect();
        console.log("Connected.");
        await admin.createTopics({
            "topics": [{
                "topic": "TWEET",
                "numPartitions": 1
            }]
        })
        console.log('Topic created');
        await admin.disconnect();
    }
    catch (ex) {
        console.error(`error on creating topic ${ex}`);
    }
    finally {
        process.exit(0);
    }
}

run();
