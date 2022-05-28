import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Receiver {
    private static String lastReceivedMessage = null;
    private static int amountOfEqualMessages = 0;

    public static void main(String[] args) throws IOException, TimeoutException {
        //noinspection resource
        Channel channel = new ConfiguredChannel().channel;
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        RepeatingPrinter repeatingPrinter = new RepeatingPrinter();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (message.equals(lastReceivedMessage) || lastReceivedMessage == null) {
                ++amountOfEqualMessages;
            } else {
                amountOfEqualMessages = 1;
                System.out.println();
            }
            repeatingPrinter.print(" [x] Received '%s' %d times", message, amountOfEqualMessages);
            lastReceivedMessage = message;

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
        };
        channel.basicConsume(
                Globals.QUEUE_NAME,
                false,  // Assume that messages received by workers are served and thus can be deleted
                deliverCallback,
                consumerTag -> { }
        );
    }
}
