const express = require("express");
const dotenv = require("dotenv");
const cors = require("cors");
const bodyParser = require("body-parser");
const { Kafka } = require("kafkajs");
const fs = require("fs");
const app = express();

dotenv.config();
const port = process.env.PORT || 3000;

// Optional SSL configuration (load certs if provided)
// let sslConfig = false; // Default: no SSL
// if (process.env.KAFKA_SSL === "true") {
//   sslConfig = {
//     rejectUnauthorized: process.env.KAFKA_SSL_REJECT_UNAUTHORIZED !== "false", // default true
//     ca: process.env.KAFKA_CA_PATH ? [fs.readFileSync(process.env.KAFKA_CA_PATH, "utf-8")] : undefined,
//     key: process.env.KAFKA_KEY_PATH ? fs.readFileSync(process.env.KAFKA_KEY_PATH, "utf-8") : undefined,
//     cert: process.env.KAFKA_CERT_PATH ? fs.readFileSync(process.env.KAFKA_CERT_PATH, "utf-8") : undefined,
//   };
// }

const kafka = new Kafka({
  clientId: "test-kafka-app-" + Date.now(), // unique ID per instance
  brokers: [
    process.env.KAFKA_BOOTSTRAP_SERVER_URL ||
      "my-cluster-kafka-bootstrap.kafka:9092",
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const consumerRawData = kafka.consumer({
  groupId: "test-kafka-app-group",
});

app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Hello World!");
});

const run = async () => {
  await consumerRawData.connect();
  console.info("✅ Connected to Kafka Broker (SSL Enabled).");

  await consumerRawData.subscribe({
    topic: process.env.SUBSCRIBE_TOPIC,
    fromBeginning: false,
  });

  await consumerRawData.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const payLoadParsed = JSON.parse(message.value.toString());
        console.log("Payload:", payLoadParsed);
        console.log("Publish Data:",JSON.stringify(payLoadParsed));
      } catch (e) {
        console.error("❌ Invalid JSON:", e.toString());
      }
    },
  });
};

app.listen(port, () => {
  console.log(`🚀 Server running on http://localhost:${port}`);
});

run().catch((err) => console.error("run error:", err));

consumerRawData.on("consumer.crash", () => {
  console.log("Crash detected");
  process.exit(0);
});

consumerRawData.on("consumer.disconnect", () => {
  console.log("Disconnect detected");
  process.exit(0);
});

consumerRawData.on("consumer.stop", () => {
  console.log("Stop detected");
  process.exit(0);
});
