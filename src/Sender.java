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
            while (true) {
                String message = scanner.nextLine();
                channel.basicPublish(
                        "",
                        Globals.QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,  // Messages should also be persistent
                        message.getBytes(StandardCharsets.UTF_8)
                );
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }
}
