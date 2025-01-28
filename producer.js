import express from 'express';
import { Kafka } from "kafkajs";

const app = new express();

const kafka = new Kafka({
    clientId: "KafkaProducer",
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

(async () => {
    await producer.connect();
    console.log("Producer connected successfully");
})()

app.get('/health', (req, res) => {
    res.status(200).json({
        msg: "Server is healthy"
    })
})

app.use(express.json());
app.post('/send-message', async (req, res) => {
    const { message } = req.body;
  
    if (!message) {
      return res.status(400).send({ error: 'Message is required' });
    }
  
    try {
      await producer.send({
        topic: 'chat-topic',
        messages: [{ value: message }],
      });

      console.log('Message sent:', message);

      res.status(200).send({ status: 'Message sent successfully' });
    } catch (err) {
      console.error('Error producing message:', err);
      res.status(500).send({ error: 'Failed to send message' });
    }
});

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})

app.listen(3000, () => {
    console.log("server listening on port 3000");
})