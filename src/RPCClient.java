import com.rabbitmq.client.AMQP;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public final class RPCClient implements AutoCloseable {
    private final ConfiguredChannel receivingChannel;
    private final String receivingQueueName;

    private RPCClient() throws IOException, TimeoutException {
        this.receivingChannel = new ConfiguredChannel();
        this.receivingQueueName = this.receivingChannel.channel.queueDeclare(
                "", false, true, false, null
        ).getQueue();
    }

    @Override
    public void close() throws IOException {
        this.receivingChannel.close();
    }

    private String call(String message) throws IOException, InterruptedException, TimeoutException {
        String correlationId = UUID.randomUUID().toString();
        this.receivingChannel.channel.basicPublish(
                "",
                Globals.RPC_QUEUE_NAME,
                new AMQP.BasicProperties.Builder().replyTo(this.receivingQueueName).correlationId(correlationId).build(),
                message.getBytes(StandardCharsets.UTF_8)
        );
        this.receivingChannel.channel.waitForConfirmsOrDie(5000);
        BlockingQueue<String> incomingMessages = new ArrayBlockingQueue<>(1);
        String currentHandlerConsumerTag = this.receivingChannel.channel.basicConsume(this.receivingQueueName, false, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                incomingMessages.offer(new String(delivery.getBody(), StandardCharsets.UTF_8));
                this.receivingChannel.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }, consumerTag -> { });
        String result = incomingMessages.take();
        this.receivingChannel.channel.basicCancel(currentHandlerConsumerTag);
        return result;
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        try (RPCClient RPCClient = new RPCClient();
             Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            while (true) {
                System.out.print(" [*] Enter the number to be processed: ");
                String message = scanner.nextLine();
                System.out.println(" [x] fib(" + message + ") = " + RPCClient.call(message));
            }
        }
    }
}
