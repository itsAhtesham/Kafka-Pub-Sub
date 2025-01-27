import express from "express";
import kafka from "./kafka.js";

const app = express();
const port = 3002;

const consumer = kafka.consumer({ groupId: 'group' + Math.random() });

(async () => {
  try {
    await consumer.connect();
    console.log('Consumer 2 connected to Kafka');
    await consumer.subscribe({ topic: 'topic', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        console.log(`Consumer 2 Received message: ${message.value.toString()} offset: ${message.offset}`);
      },
    });
  } catch (err) {
    console.error('Error with Consumer 2:', err);
  }
})();

app.listen(port, () => {
  console.log(`Consumer 2 server running at http://localhost:${port}`);
});