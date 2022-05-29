import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RPCServer {
    public static void main(String[] args) throws IOException, TimeoutException {
        //noinspection resource
        ConfiguredChannel configuredChannel = new ConfiguredChannel();
        Channel channel = configuredChannel.channel;
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            String result = fib(new BigInteger(message)).toString();
            channel.basicPublish(
                    "",
                    delivery.getProperties().getReplyTo(),
                    new AMQP.BasicProperties.Builder().correlationId(delivery.getProperties().getCorrelationId()).build(),
                    result.getBytes(StandardCharsets.UTF_8)
            );
            System.out.println(" [x] Sent '" + result + "'");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        channel.basicConsume(
                Globals.RPC_QUEUE_NAME,
                false,  // Assume that messages received by workers are served and thus can be deleted
                deliverCallback,  // New messages would fall here, but only once, because the main thread is locked?
                consumerTag -> { }
        );
    }

    private static BigInteger fib(BigInteger number) {
        if (number.equals(BigInteger.ONE)) { return BigInteger.ZERO; }
        if (number.equals(BigInteger.TWO)) { return BigInteger.ONE; }
        return fib(number.subtract(BigInteger.ONE)).add(fib(number.subtract(BigInteger.TWO)));
    }
}
