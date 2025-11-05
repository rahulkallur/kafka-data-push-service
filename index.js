const express = require("express");
const dotenv = require("dotenv");
const cors = require("cors");
const bodyParser = require("body-parser");
const { Kafka } = require("kafkajs");
const app = express();

dotenv.config();
const port = process.env.PORT || 3000;

const kafka = new Kafka({
  clientId: "test-kafka-app-" + Date.now(), // Append Current Epoch milliseconds for Random Id
  brokers: [
    process.env.KAFKA_BOOTSTRAP_SERVER_URL || 
      "my-cluster-kafka-bootstrap.kafka:9092",
  ],
});

const consumerRawData = kafka.consumer({
  groupId: "test-kafka-app-group",
});

app.use(cors()); // Enable CORS
app.use(bodyParser.json()); // Parse JSON bodies
app.use(bodyParser.urlencoded({ extended: true }));

// Middleware to parse JSON bodies
app.use(express.json());

// Sample route
app.get("/", (req, res) => {
  res.send("Hello World!");
});

const run = async () => {
  // Consuming
  await consumerRawData.connect();
  console.info("Connected to Kafka Broker.");
  await consumerRawData.subscribe({ topic: "input", fromBeginning: false });

  consumerRawData.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        let payLoadParsed = JSON.parse(message.value.toString());
        console.log("Payload: ", payLoadParsed);
      } catch (e) {
        console.error("Invalid JSON!!", e.toString());
      }
    },
  });
};

// Start the server
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});

run().catch("run error: ", console.error);

consumerRawData.on("consumer.crash", function () {
  console.log("Crash detected");
  process.exit(0);
});

consumerRawData.on("consumer.disconnect", function () {
  console.log("Disconnect detected");
  process.exit(0);
});

consumerRawData.on("consumer.stop", function () {
  console.log("Stop detected");
  process.exit(0);
});