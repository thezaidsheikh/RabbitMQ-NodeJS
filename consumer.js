const amqp = require("amqplib");
const connection_url = "amqp://localhost:5672";
let rabbitConnection = null;
let rabbitChannel = null;

async function startConsumerServer() {
    console.log(`Started Consumer Server`);
    // Connection with the rabbitmq server defaut port: 5672
    rabbitConnection = await amqp.connect(connection_url);
    // Creating channel
    rabbitChannel = await rabbitConnection.createChannel();

    // startCompetingServer_cons();
    // startFanoutServer_cons();
    // startDirectServer_cons();
    // startTopicServer_cons();
    // startReplyServer_cons();
    // startE2EServer_cons();
    // startHeaderServer_cons();
    // startAlternateServer_cons();
    startDeadServer_cons();
}

/** Practical 1 : Competing pattern */
async function startCompetingServer_cons() {
    rabbitChannel.prefetch(1);
    rabbitChannel.assertQueue("competingConsumerPattern", { durable: true });
    rabbitChannel.assertQueue("competing", { durable: true });
    rabbitChannel.consume("competing", (msg) => {
        const time = Math.floor(Math.random() * 11);
        console.log(`Proccessing1 time of messageId ${Buffer.from(msg.content)} will take ${time} sec`);
        setTimeout(() => {
            rabbitChannel.ack(msg);
            console.log(`Finished processing1 of messageId ${Buffer.from(msg.content)}`);
        }, time * 1000);
    });
    rabbitChannel.consume("competingConsumerPattern", (msg) => {
        const time = Math.floor(Math.random() * 11);
        console.log(`Proccessing2 time of messageId ${Buffer.from(msg.content)} will take ${time} sec`);
        setTimeout(() => {
            rabbitChannel.ack(msg);
            console.log(`Finished processing2 of messageId ${Buffer.from(msg.content)}`);
        }, time * 1000);
    });
}

/** Practical 2 : Pub Sub pattern with fanout exchange */
async function startFanoutServer_cons() {
    rabbitChannel.assertExchange("pubsub_fanout", "fanout");
    const fanout_queue1 = await rabbitChannel.assertQueue("", { exclusive: true });
    const fanout_queue2 = await rabbitChannel.assertQueue("", { exclusive: true });
    rabbitChannel.bindQueue(fanout_queue1.queue, "pubsub_fanout", "");
    // rabbitChannel.bindQueue(fanout_queue2.queue, "pubsub_fanout", "");
    rabbitChannel.consume(
        fanout_queue1.queue,
        (msg) => {
            console.log("pubsub_fanout queue1 consumed message =====>", msg.content.toString());
        },
        { noAck: true }
    );
    rabbitChannel.consume(
        fanout_queue2.queue,
        (msg) => {
            console.log("pubsub_fanout queue2 consumed message =====>", msg.content.toString());
        },
        { noAck: true }
    );
}

/** Practical 3 : Pub Sub pattern with direct exchange */
async function startDirectServer_cons() {
    rabbitChannel.assertExchange("pubsub_direct", "direct");
    const direct_queue1 = await rabbitChannel.assertQueue("", { exclusive: true });
    const direct_queue2 = await rabbitChannel.assertQueue("", { exclusive: true });
    rabbitChannel.bindQueue(direct_queue1.queue, "pubsub_direct", "product_info");
    rabbitChannel.bindQueue(direct_queue2.queue, "pubsub_direct", "messageId");
    rabbitChannel.consume(
        direct_queue1.queue,
        (msg) => {
            console.log("pubsub_direct direct_queue1 consumed message =====>", msg.content.toString());
        },
        { noAck: true }
    );
    rabbitChannel.consume(
        direct_queue2.queue,
        (msg) => {
            console.log("pubsub_direct direct_queue2 consumed message =====>", msg.content.toString());
        },
        { noAck: true }
    );
}

/** Practical 4 : Pub Sub pattern with topic exchange */
async function startTopicServer_cons() {
    rabbitChannel.assertExchange("pubsub_topic", "topic");
    const topic_queue1 = await rabbitChannel.assertQueue("", { exclusive: true });
    const topic_queue2 = await rabbitChannel.assertQueue("", { exclusive: true });
    rabbitChannel.bindQueue(topic_queue1.queue, "pubsub_topic", "#.payment");
    rabbitChannel.bindQueue(topic_queue2.queue, "pubsub_topic", "user.*");
    rabbitChannel.consume(
        topic_queue1.queue,
        (msg) => {
            console.log("pubsub_topic topic_queue1 consumed message =====>", msg.content.toString());
        },
        { noAck: true }
    );
    rabbitChannel.consume(
        topic_queue2.queue,
        (msg) => {
            console.log("pubsub_topic topic_queue2 consumed message =====>", msg.content.toString());
        },
        { noAck: true }
    );
}

/** Practical 5 : Request Reply pattern */
async function startReplyServer_cons() {
    rabbitChannel.assertQueue("request_queue", { durable: true });
    rabbitChannel.consume(
        "request_queue",
        (msg) => {
            console.log("Received requested data ===>", msg.content.toString());
            rabbitChannel.publish(
                "",
                msg.properties.replyTo,
                Buffer.from(`This is reply to correlation id ${msg.properties.correlationId}`)
            );
        },
        { noAck: true }
    );
}

/** Practical 6 : Exchange to Exchange pattern */
async function startE2EServer_cons() {
    rabbitChannel.assertExchange("firstExchange", "direct");
    rabbitChannel.assertExchange("secondExchange", "fanout");
    rabbitChannel.assertQueue("firstQueue", { exclusive: true });
    rabbitChannel.bindQueue("firstQueue", "secondExchange");
    rabbitChannel.consume(
        "firstQueue",
        (msg) => {
            console.log("Received data ===>", msg.content.toString());
        },
        { noAck: true }
    );
}

/** Practical 7 : Header Exchange pattern */
async function startHeaderServer_cons() {
    rabbitChannel.assertExchange("headerExchange", "headers");
    rabbitChannel.assertQueue("headerQueue", { exclusive: true });
    rabbitChannel.bindQueue("headerQueue", "headerExchange", "", { "x-match": "all", "name": "Zaid", "gender": "Male" });
    rabbitChannel.consume(
        "headerQueue",
        (msg) => {
            console.log("Received data for header ===>", msg.content.toString());
        },
        { noAck: true }
    );
}

/** Practical 8 : Alternate Exchange pattern */
async function startAlternateServer_cons() {
    rabbitChannel.assertExchange("altExchange", "fanout");
    rabbitChannel.assertExchange("mainExchange", "direct", { alternateExchange: "altExchange" });
    rabbitChannel.assertQueue("altQueue", { durable: true });
    rabbitChannel.assertQueue("mainQueue", { durable: true });
    rabbitChannel.bindQueue("altQueue", "altExchange");
    rabbitChannel.consume("altQueue", (msg) => {
        console.log(`Alt : Message from id ${msg.content.toString()}`);
    }, { noAck: true });
    rabbitChannel.bindQueue("mainQueue", "mainExchange", "toMainQueue");
    rabbitChannel.consume("mainQueue", (msg) => {
        console.log(`Main : Message from id ${msg.content.toString()}`);
    }, { noAck: true });
}

/** Practical 9 : Dead Letter Exchange pattern */
async function startDeadServer_cons() {
    rabbitChannel.assertExchange("deadExchange", "fanout");
    rabbitChannel.assertExchange("mainExchange", "direct");
    rabbitChannel.assertQueue("deadQueue", { durable: true });
    rabbitChannel.assertQueue("mainQueue", { durable: true, deadLetterExchange: "deadExchange", messageTtl: 1000 });
    rabbitChannel.bindQueue("deadQueue", "deadExchange");
    rabbitChannel.consume("deadQueue", (msg) => {
        console.log(`Dead : Message from id ${msg.content.toString()}`);
    }, { noAck: true });
    rabbitChannel.bindQueue("mainQueue", "mainExchange", "toMainQueue");
    // rabbitChannel.consume("mainQueue", (msg) => {
    //     console.log(`Main : Message from id ${msg.content.toString()}`);
    // }, { noAck: true });
}

startConsumerServer();
