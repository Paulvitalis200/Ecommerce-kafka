import { Kafka } from "kafkajs"

const kafka = new Kafka({
    clientId: "email-service",
    brokers: ["localhost:9094"]
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: "email-service"})

const run = async () => {
    try {
        await producer.connect()
        await consumer.connect()
        await consumer.subscribe({
            topic: "order-successful",
            fromBeginning: true, // If connection is lost and we start from beginning, it will fetch all messages from kafka

        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString()
                const { userId, orderId } = JSON.parse(value)

                // TODO: Send email to the user
                const dummyEmailId = "0283229323";
                console.log(`Email consumer: Email sent to user id ${userId}`)
                await producer.send({
                    topic: "email-successful",
                    messages: [{ value: JSON.stringify({ userId, email: dummyEmailId })}]
                })
            }
        })
    } catch(err) {
        console.log(err)
    }
}

run()