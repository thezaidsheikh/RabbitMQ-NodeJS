const amqp = require("amqplib");
const connection_url = "amqp://localhost:5672";
let messageId = 1;
// Connection with the rabbitmq server defaut port: 5672
let rabbitConnection = await amqp.connect(connection_url);
// Creating channel
let rabbitChannel = await rabbitConnection.createChannel();

async function startProducerServer() {
    console.log(`Started Producer Server`);
    // startCompetingServer_prod();
    // startFanoutServer_prod();
    // startDirectServer_prod();
    // startTopicServer_prod();
    // startReplyServer_prod();
    // startE2EServer_prod();
    // startHeaderServer_prod();
    // startAlternateServer_prod();
    startDeadServer_prod();
}
/** Practical 1 : Competing pattern */
async function startCompetingServer_prod() {
    setInterval(() => {
        console.log(`Sending messageId : ${messageId}`)
        rabbitChannel.assertQueue("competingConsumerPattern", { durable: true });
        rabbitChannel.publish("", "competingConsumerPattern", Buffer.from(`${messageId}`));
        rabbitChannel.publish("", "competing", Buffer.from(`${messageId}`));

        messageId += 1;
    }, 2000)
}

/** Practical 2 : Pub Sub pattern with fanout exchange */
async function startFanoutServer_prod() {
    setInterval(() => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("pubsub_fanout", "fanout");
        rabbitChannel.publish("pubsub_fanout", "", Buffer.from(`${messageId}`));
        messageId += 1;
    }, 2000)
}

/** Practical 3 : Pub Sub pattern with direct exchange */
async function startDirectServer_prod() {
    setInterval(() => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("pubsub_direct", "direct");
        rabbitChannel.publish(
            "pubsub_direct",
            "product_info",
            Buffer.from(`${messageId} for product info`)
        );
        rabbitChannel.publish("pubsub_direct", "messageId", Buffer.from(`${messageId} for message`));
        messageId += 1;
    }, 2000)
}

/** Practical 4 : Pub Sub pattern with topic exchange */
async function startTopicServer_prod() {
    setInterval(() => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("pubsub_topic", "topic");
        rabbitChannel.publish(
            "pubsub_topic",
            "user.europe",
            Buffer.from(`${messageId} from user.europe`)
        );
        rabbitChannel.publish(
            "pubsub_topic",
            "product.user.payment",
            Buffer.from(`${messageId} from product.user.payment`)
        );
        messageId += 1;
    }, 2000)
}

/** Practical 5 : Request Reply pattern */
async function startReplyServer_prod() {
    setInterval(async () => {
        console.log(`Sending messageId : ${messageId}`);
        let replyQueue = await rabbitChannel.assertQueue("", { exclusive: true });
        rabbitChannel.assertQueue("request_queue", { durable: true });
        rabbitChannel.consume(replyQueue.queue, (msg) => {
            console.log(`Received reply for queue : ${replyQueue.queue}, message : ${msg.content.toString()}`)
        });
        rabbitChannel.publish("", "request_queue", Buffer.from("I have requested a reply"), {
            replyTo: replyQueue.queue,
            correlationId: "123456",
        });
        messageId += 1;
    }, 2000)
}

/** Practical 6 : Exchange to Exchange pattern */
async function startE2EServer_prod() {
    setInterval(async () => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("firstExchange", "direct");
        rabbitChannel.assertExchange("secondExchange", "fanout");
        await rabbitChannel.bindExchange("secondExchange", "firstExchange");
        rabbitChannel.publish("firstExchange", "", Buffer.from(`${messageId}`));
        messageId += 1;
    }, 2000)
}

/** Practical 7 : Header Exchange pattern */
async function startHeaderServer_prod() {
    setInterval(async () => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("headerExchange", "headers");
        rabbitChannel.publish("headerExchange", "", Buffer.from(`${messageId}`), { headers: { name: "Zaid" } });
        messageId += 1;
    }, 2000)
}

/** Practical 8 : Alternate Exchange pattern */
async function startAlternateServer_prod() {
    setInterval(async () => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("altExchange", "fanout");
        rabbitChannel.assertExchange("mainExchange", "direct", { alternateExchange: "altExchange" });
        rabbitChannel.publish("mainExchange", "toMainQueue", Buffer.from(`${messageId}`));
        messageId += 1;
    }, 2000)
}

/** Practical 9 : Dead Letter Exchange pattern */
async function startDeadServer_prod() {
    setInterval(async () => {
        console.log(`Sending messageId : ${messageId}`);
        rabbitChannel.assertExchange("mainExchange", "direct");
        rabbitChannel.publish("mainExchange", "toMainQueue", Buffer.from(`${messageId}`));
        messageId += 1;
    }, 2000)
}

startProducerServer()