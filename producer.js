import kafka from "./kafka.js";
import express from 'express';

const app = new express();

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
        topic: 'topic',
        messages: [{ value: message }],
      });

      console.log('Message sent:', message);

      res.status(200).send({ status: 'Message sent successfully' });
    } catch (err) {
      console.error('Error producing message:', err);
      res.status(500).send({ error: 'Failed to send message' });
    }
});

app.listen(3000, () => {
    console.log("server listening on port 3000");
})