import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Receiver {
    public static void main(String[] args) throws IOException, TimeoutException {
        //noinspection resource
        Channel channel = new ConfiguredChannel().channel;
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            doWork(message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
            System.out.println(" [x] Done!");
        };
        channel.basicConsume(
                Globals.QUEUE_NAME,
                false,  // Assume that messages received by workers are served and thus can be deleted
                deliverCallback,
                consumerTag -> { }
        );
    }

    private static void doWork(String message) {
        try {
            for (char character : message.toCharArray()) {
                if (character == '.') {
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
