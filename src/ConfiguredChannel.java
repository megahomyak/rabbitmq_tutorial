import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

class ConfiguredChannel implements AutoCloseable {
    final Channel channel;
    private final Connection connection;

    ConfiguredChannel() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = this.connection.createChannel();
        this.channel.basicQos(1);  // Works only for Receivers

        this.channel.queueDeclare(
                Globals.QUEUE_NAME,
                true,  // Messages survive the server restart
                false,  // Accessible only for current connection and deletes on disconnection
                false,  // Deletes when there are no consumers
                null  // Additional arguments for plug-ins and the concrete broker
        );
    }

    @Override
    public final void close() throws IOException {
        this.connection.close();
    }
}
