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
        this.channel.confirmSelect();  // Enable publisher confirms (a guarantee that the message will be delivered to the server)
        this.channel.basicQos(1);  // Works only for receivers
        this.channel.queueDeclare(Globals.QUEUE_NAME, false, false, false, null);
        this.channel.queuePurge(Globals.QUEUE_NAME);
    }

    @Override
    public final void close() throws IOException {
        this.connection.close();
    }
}
