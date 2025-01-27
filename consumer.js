import express from "express";
import kafka from "./kafka.js";

const app = express();
const port = 3001;

const consumer = kafka.consumer({ groupId: 'group' });

(async () => {
  try {
    await consumer.connect();
    console.log('Consumer 1 connected to Kafka');
    await consumer.subscribe({ topic: 'topic', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        console.log("hii");
        
        console.log(`Consumer 1 Received message: ${message.value.toString()} offset: ${message.offset}`);
      },
    });
  } catch (err) {
    console.error('Error with Consumer 1:', err);
  }
})();

app.listen(port, () => {
  console.log(`Consumer 1 server running at http://localhost:${port}`);
});