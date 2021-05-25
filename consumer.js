
const { Kafka } = require('kafkajs');

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
                console.log(result.topic);
                console.log(result.message.key.toString());
                console.log(result.message.value.toString());
            }
        });

        console.log(`message started successfully ${result}`);
    }
    catch (ex) {
        console.error(`error on creating topic ${ex}`);
    }
}

run();
