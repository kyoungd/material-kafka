const axios = require('axios');
const { Kafka } = require('kafkajs');

async function post(url, data) {
    try {
        const url1 = url.toLowerCase();
        const result1 = await axios.post(url, {
            data
        });
        return result1;
    }
    catch (err) {
        console.log(err);
    }
}

async function postKafkaCommand(messages) {
    const kafka = new Kafka({
        "clientId": "kafka_connect",
        "brokers": ["localhost:9092"]
    });
    const producer = kafka.producer();
    console.log("Connecting... ");
    await producer.connect();
    console.log("Connected.");
    messages.map(message => { })
    const result = await producer.send({
        "topic": "TWEET",
        "messages": messages
    });
    console.log(`message sent successfully ${result}`);
    await producer.disconnect();
}

module.exports = { post, postKafkaCommand };
