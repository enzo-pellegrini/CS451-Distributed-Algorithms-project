package cs451;

import cs451.FIFOBroadcast.FIFOBroadcast;
import cs451.Parser.Host;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class BufferedFIFOBroadcast<T> {
    private static final int MAX_MESSAGES_IN_BUFFER = 8;
    private final FIFOBroadcast<List<T>> fifo;
    private final List<T> buffer = new ArrayList<>();
    private final Consumer<FIFOBroadcast.ReceivedMessage<T>> deliver;

    public BufferedFIFOBroadcast(int myId, int port, List<Host> hosts,
                                 Consumer<FIFOBroadcast.ReceivedMessage<T>> deliver,
                                 BiConsumer<T, ByteBuffer> messageSerializer,
                                 Function<ByteBuffer, T> messageDeserializer,
                                 int messageSize) {
        this.deliver = deliver;
        this.fifo = new FIFOBroadcast<>(myId, port, hosts,
                this::onDeliver,
                (messages, bb) -> {
                    bb.put((byte)(messages.size()));
                    for (T message : messages) {
                        messageSerializer.accept(message, bb);
                    }
                },
                bb -> {
                    int size = bb.get();
                    List<T> messages = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        messages.add(messageDeserializer.apply(bb));
                    }
                    return messages;
                },
                1+8*messageSize);
    }

    public void startThreads() {
        fifo.startThreads();
    }

    public void interruptAll() {
        fifo.interruptAll();
    }

    private void onDeliver(FIFOBroadcast.ReceivedMessage<List<T>> tReceivedMessage) {
        for (T m : tReceivedMessage.message) {
            deliver.accept(new FIFOBroadcast.ReceivedMessage<>(tReceivedMessage.from, m));
        }
    }

    /**
     * Broadcasts a message to all hosts.
     * NOT THREAD SAFE
     * @param message message to broadcast
     */
    public void broadcast(T message) {
        buffer.add(message);

        if (buffer.size() >= MAX_MESSAGES_IN_BUFFER) {
            fifo.broadcast(new ArrayList<>(buffer));
            buffer.clear();
        }
    }

    public void flushBuffers() {
        if (!buffer.isEmpty()) {
            fifo.broadcast(buffer);
            buffer.clear();
        }
    }
}
