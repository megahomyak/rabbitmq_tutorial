@FunctionalInterface
public interface DangerousConsumer<T> {
    void consume(T object) throws Exception;
}
