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

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})

app.listen(port, () => {
  console.log(`Consumer 2 server running at http://localhost:${port}`);
});