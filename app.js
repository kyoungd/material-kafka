
const dotenv = require("dotenv")
const express = require('express');
const { Kafka } = require('kafkajs');

const { postKafkaCommand } = require('./util');
const PORT = process.env.PORT || 7101
const app = express()
const server = http.createServer(app)
dotenv.config();

app.post('/tweets', async (req, res) => {
    try {
        const data = req.body.data;
        await postKafkaCommand(commands);
    }
    catch (ex) {
        console.error(`error on tweetstart ${ex}`);
    }
})

var server = app.listen(PORT, function () {
    var host = server.address().address
    var port = server.address().port
    console.log("producer app listening at http://%s:%s", host, port)
})
