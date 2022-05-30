import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

public class Sender {
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        ConcurrentNavigableMap<Long, String> sentMessagesNotAcknowledgedByTheServer = new ConcurrentSkipListMap<>();
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel();
             Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            channel.confirmSelect();  // Enable publisher confirms (a guarantee that the message will be delivered to the server)
            channel.addConfirmListener((sequenceNumber, isMultiple) -> {
                if (isMultiple) {
                    sentMessagesNotAcknowledgedByTheServer.headMap(sequenceNumber, true).clear();
                } else {
                    sentMessagesNotAcknowledgedByTheServer.remove(sequenceNumber);
                }
            }, (sequenceNumber, isMultiple) -> {
                if (isMultiple) {
                    ConcurrentNavigableMap<Long, String> negativelyAcknowledgedMessages =
                            sentMessagesNotAcknowledgedByTheServer.headMap(sequenceNumber, true);
                    negativelyAcknowledgedMessages.forEach((messageSequenceNumber, message) -> {
                        System.err.printf("Message with contents %s was negatively acknowledged!%n", message);
                    });
                    negativelyAcknowledgedMessages.clear();
                } else {
                    System.err.printf(
                            "Message with contents %s was negatively acknowledged!%n",
                            sentMessagesNotAcknowledgedByTheServer.remove(sequenceNumber)
                    );
                }
            });
            System.out.println(" [*] Waiting for your input to broadcast");
            channel.queueDeclare(Globals.QUEUE_NAME, false, false, false, null);
            while (true) {
                String message = scanner.nextLine();
                sentMessagesNotAcknowledgedByTheServer.put(channel.getNextPublishSeqNo(), message);
                channel.basicPublish("", Globals.QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }
}
