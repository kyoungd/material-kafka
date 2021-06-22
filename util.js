const axios = require('axios');
const { Kafka } = require('kafkajs');

async function post(url, data) {
    try {
        const url1 = url.toLowerCase();
        const result1 = await axios.post(url1, data, {
            headers: {
                // Overwrite Axios's automatically set Content-Type
                'Content-Type': 'application/json'
            }
        });
        return result1;
    }
    catch (err) {
        console.log(err);
    }
}

async function postKafkaCommand(kafka, messages) {
    try {
        const producer = kafka.producer();
        console.log("Connecting... ");
        await producer.connect();
        console.log("Connected.");
        // messages.map(message => { })
        const result = await producer.send(messages);
        console.log(`message sent successfully ${result}`);
        await producer.disconnect();
    }
    catch (err) {
        console.log(err);
    }

}

module.exports = { post, postKafkaCommand };
