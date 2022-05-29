import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Sender {
    public static void main(String[] args) throws IOException, TimeoutException {
        try (ConfiguredChannel configuredChannel = new ConfiguredChannel();
             Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            Channel channel = configuredChannel.channel;
            System.out.println(" [*] Waiting for your input to broadcast");
            System.out.println(" [*] Write your messages in the following format: routingKey messageContents");
            while (true) {
                String[] input = scanner.nextLine().split("\\s+", 2);
                String routingKey = input[0];
                String message = input[1];
                channel.basicPublish(
                        Globals.EXCHANGE_NAME,
                        routingKey,  // Routing keys are used to dispatch messages only to certain queues
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8)
                );
                System.out.println(" [x] Sent '" + message + "' to " + routingKey);
            }
        }
    }
}
