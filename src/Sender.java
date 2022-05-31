import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Sender {
    private static final byte[] DUMMY_MESSAGE_CONTENTS = "test".getBytes(StandardCharsets.UTF_8);
    private static final int BENCHMARK_RUNS_AMOUNT = 50000;

    private static void benchmark(String label, Benchmarkable function) throws Exception {
        AtomicInteger messagesCounter = new AtomicInteger();
        Stream<byte[]> messages = Stream.iterate(DUMMY_MESSAGE_CONTENTS, seed -> DUMMY_MESSAGE_CONTENTS).limit(BENCHMARK_RUNS_AMOUNT).peek(message -> {
            messagesCounter.incrementAndGet();
        });
        long beginningTime = System.nanoTime();
        function.run(messages);
        if (messagesCounter.get() != BENCHMARK_RUNS_AMOUNT) {
            //noinspection ProhibitedExceptionThrown
            throw new RuntimeException(String.format(
                    "Not all messages were sent by benchmark '%s'! " +
                    "(This benchmark needs to send %d messages, but it sent only %d)",
                    label, BENCHMARK_RUNS_AMOUNT, messagesCounter.get()));
        }
        long endTime = System.nanoTime();
        System.out.printf("'%s' completed in %f seconds%n", label, ((double) (endTime - beginningTime)) / 1_000_000_000);
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.confirmSelect();  // Enable publisher confirms (a guarantee that the message will be delivered to the server)
            String queueName = channel.queueDeclare().getQueue();

            benchmarkIndividualMessages(channel, queueName);  // Over the network, this should be the slowest (and it is locally)
            benchmarkMessageBatches(channel, queueName);  // Fastest locally (not every time), but should not be the fastest over the network
            benchmarkAsynchronousMessages(channel, queueName);  // Over the network, this should be the fastest (and sometimes it is the fastest locally)
        }
    }

    private static void benchmarkIndividualMessages(Channel channel, String queueName) throws Exception {
        benchmark("Publishing messages individually", messages -> {
            messages.forEach(msg -> StupidConsumer.consume(msg, message -> {
                channel.basicPublish("", queueName, null, message);
                channel.waitForConfirmsOrDie(5000);
            }));
        });
    }

    private static void benchmarkMessageBatches(Channel channel, String queueName) throws Exception {
        final int BATCH_SIZE = 500;
        benchmark("Publishing messages in batches", (messages) -> {
            AtomicInteger remainingItemsInBatch = new AtomicInteger(BATCH_SIZE);
            messages.forEach(msg -> StupidConsumer.consume(msg, message -> {
                remainingItemsInBatch.decrementAndGet();
                channel.basicPublish("", queueName, null, message);
                if (remainingItemsInBatch.get() <= 0) {
                    channel.waitForConfirmsOrDie(5000);
                    remainingItemsInBatch.set(BATCH_SIZE);
                }
            }));
            if (remainingItemsInBatch.get() != BATCH_SIZE) {
                channel.waitForConfirmsOrDie(5000);
            }
        });
    }

    private static void benchmarkAsynchronousMessages(Channel channel, String queueName) throws Exception {
        Object allMessagesWereSentSynchroniser = new Object();
        ConcurrentNavigableMap<Long, byte[]> sentMessagesNotAcknowledgedByTheServer = new ConcurrentSkipListMap<>();
        channel.addConfirmListener((sequenceNumber, isMultiple) -> {
            if (isMultiple) {
                sentMessagesNotAcknowledgedByTheServer.headMap(sequenceNumber, true).clear();
            } else {
                sentMessagesNotAcknowledgedByTheServer.remove(sequenceNumber);
            }
            if (sentMessagesNotAcknowledgedByTheServer.isEmpty()) {
                synchronized (allMessagesWereSentSynchroniser) {
                    allMessagesWereSentSynchroniser.notifyAll();
                }
            }
        }, (sequenceNumber, isMultiple) -> {
            if (isMultiple) {
                ConcurrentNavigableMap<Long, byte[]> negativelyAcknowledgedMessages =
                        sentMessagesNotAcknowledgedByTheServer.headMap(sequenceNumber, true);
                negativelyAcknowledgedMessages.forEach((messageSequenceNumber, messageBytes) -> {
                    System.err.printf("Message with contents %s was negatively acknowledged!%n", new String(messageBytes, StandardCharsets.UTF_8));
                });
                negativelyAcknowledgedMessages.clear();
            } else {
                System.err.printf(
                        "Message with contents %s was negatively acknowledged!%n",
                        new String(sentMessagesNotAcknowledgedByTheServer.remove(sequenceNumber), StandardCharsets.UTF_8)
                );
            }
            if (sentMessagesNotAcknowledgedByTheServer.isEmpty()) {
                synchronized (allMessagesWereSentSynchroniser) {
                    allMessagesWereSentSynchroniser.notifyAll();
                }
            }
        });
        benchmark("Handling publisher confirms asynchronously", messages -> {
            messages.forEach(msg -> StupidConsumer.consume(msg, message -> {
                sentMessagesNotAcknowledgedByTheServer.put(channel.getNextPublishSeqNo(), message);
                channel.basicPublish("", queueName, null, message);
            }));
            synchronized (allMessagesWereSentSynchroniser) {
                //noinspection UnconditionalWait
                allMessagesWereSentSynchroniser.wait();
            }
        });
    }
}
