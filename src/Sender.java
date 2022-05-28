import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Sender {
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) throws IOException, TimeoutException {
        try (ConfiguredChannel configuredChannel = new ConfiguredChannel();
             Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8)) {
            Channel channel = configuredChannel.channel;
            RepeatingPrinter repeatingPrinter = new RepeatingPrinter();
            repeatingPrinter.print("ab");
            repeatingPrinter.print("c");
            System.out.println();
            System.out.println(" [*] Waiting for your input to broadcast");
            System.out.println(" [*] Write your messages in the following format: repetitionsAmount messageContents");
            while (true) {
                String[] messageParts = scanner.nextLine().split("\\s+", 2);
                int repetitionsAmount = Integer.parseInt(messageParts[0]);
                String messageContents = messageParts[1];
                sendRepeatingMessage(channel, messageContents, repetitionsAmount);
            }
        }
    }

    private static void sendRepeatingMessage(Channel channel, String message, final int repetitionsAmount) throws IOException {
        RepeatingPrinter repeatingPrinter = new RepeatingPrinter();
        int remainingRepetitionsAmount = repetitionsAmount;  // Used only for pretty-printing
        int batchNumber = 1;  // Used only for pretty-printing
        while (remainingRepetitionsAmount > 0) {
            repeatingPrinter.print(" [x] Sending a batch number %d (batch size is %d), remaining repetitions amount is %d", batchNumber, BATCH_SIZE, remainingRepetitionsAmount);
            resendBatch:
            while (true) {
                for (int batchMessageIndex = 0; batchMessageIndex < repetitionsAmount && batchMessageIndex < BATCH_SIZE; ++batchMessageIndex) {
                    channel.basicPublish(
                            "",
                            Globals.QUEUE_NAME,
                            MessageProperties.PERSISTENT_TEXT_PLAIN,  // Messages should also be persistent
                            message.getBytes(StandardCharsets.UTF_8)
                    );
                }
                try {
                    channel.waitForConfirmsOrDie(5000);
                    break resendBatch;  // If something is wrong, the entire batch will be re-sent, because we have no idea which message was not received
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (TimeoutException ignored) {}
            }
            remainingRepetitionsAmount -= BATCH_SIZE;
            ++batchNumber;
        }
        System.out.printf("%n [x] Sent '%s' %d times%n", message, repetitionsAmount);
    }
}
