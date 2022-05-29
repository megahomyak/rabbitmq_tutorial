import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Receiver {
    public static void main(String[] args) throws IOException, TimeoutException {
        //noinspection resource
        Channel channel = new ConfiguredChannel().channel;
        String anonymousQueueName = channel.queueDeclare().getQueue();
        try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            System.out.print("Enter the names of routing keys of the queue of the current worker, separated by spaces: ");
            String[] routingKeys = scanner.nextLine().split("\\s+");
            for (String routingKey : routingKeys) {
                channel.queueBind(anonymousQueueName, Globals.EXCHANGE_NAME, routingKey);
            }
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };
            channel.basicConsume(
                    anonymousQueueName,
                    false,  // Assume that messages received by workers are served and thus can be deleted
                    deliverCallback,
                    consumerTag -> { }
            );
        }
    }
}
