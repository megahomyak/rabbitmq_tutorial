class StupidConsumer {
    static final <T> void consume(T object, DangerousConsumer<? super T> dangerousConsumer) {
        try {
            dangerousConsumer.consume(object);
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}
