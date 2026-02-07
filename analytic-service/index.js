import { Kafka } from "kafkajs"

const kafka = new Kafka({
    clientId: "analytic-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"]
})


const consumer = kafka.consumer({ groupId: "analytic-service"})

const run = async () => {
    try {
        await consumer.connect()
        await consumer.subscribe({
            topics: ["payment-successful", "order-successful", "email-successful"],
            fromBeginning: true, // If connection is lost and we start from beginning, it will fetch all messages from kafka

        })

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString()
                const {userId} = JSON.parse(value)
                switch(topic) {
                    case "payment-successful":
            
                        const { cart } = JSON.parse(value)
        
                        const total = cart.reduce((acc, item) => acc + item.price, 0).toFixed(2)
                        console.log(`Analytic consumer: User ${userId} paid ${total}`);
                        break
                    case "order-successful":
                        const { orderId } = JSON.parse(value)
        

                        console.log(`Analytic consumer: Order id ${orderId} created for User: ${userId}`);
                        break
                    case "email-successful":
                        const { email } = JSON.parse(value)
        
                        console.log(`Analytic consumer: Email id ${email} sent to User: ${userId}`);
                        break
                    default:
                        break
                }

            }
        })
    } catch(err) {
        console.log(err)
    }
}

run()