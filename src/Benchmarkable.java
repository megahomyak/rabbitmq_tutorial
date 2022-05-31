import java.util.stream.Stream;

@FunctionalInterface
public interface Benchmarkable {
    void run(Stream<byte[]> messages) throws Exception;
}