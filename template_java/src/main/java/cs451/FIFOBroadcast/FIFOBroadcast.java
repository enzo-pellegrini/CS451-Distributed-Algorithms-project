package cs451.FIFOBroadcast;

import cs451.Parser.Host;
import cs451.UniformReliableBroadcast.ReceivedURBMessage;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class FIFOBroadcast<T> {

    public static final class ReceivedMessage<T> {
        public final int from;
        public final T message;

        public ReceivedMessage(int from, T message) {
            this.from = from;
            this.message = message;
        }
    }

    private final UniformReliableBroadcast<T> urb;
    private final Consumer<ReceivedMessage<T>> deliver;
    private final List<SortedSet<ReceivedURBMessage<T>>> inFlight;
    private final Lock inFlightLock = new ReentrantLock();
    private final int[] lastDelivered;

    public FIFOBroadcast(int myId, int port, List<Host> hosts,
                         Consumer<ReceivedMessage<T>> deliver,
                         BiConsumer<T, ByteBuffer> messageSerializer,
                         Function<ByteBuffer, T> messageDeserializer,
                         int messageSize) {
        this.deliver = deliver;

        List<SortedSet<ReceivedURBMessage<T>>> tmpInFlight = new ArrayList<>();
        for (int i = 0; i < hosts.size(); i++) {
            tmpInFlight.add(new TreeSet<>());
        }
        this.inFlight = Collections.unmodifiableList(tmpInFlight);

        this.lastDelivered = new int[hosts.size()];

        this.urb = new UniformReliableBroadcast<>(myId, port, hosts,
                this::onDeliver,
                messageSerializer,
                messageDeserializer,
                messageSize);
    }

    public void startThreads() {
        urb.startThreads();
    }

    public void broadcast(T message) {
        urb.broadcast(message);
    }

    public void interruptAll() {
        urb.interruptAll();
    }

    private void onDeliver(ReceivedURBMessage<T> packet) {
        inFlightLock.lock();
        try {
            inFlight.get(packet.from - 1).add(packet);

//            System.out.println("Received message " + packet);

            while (packet.n == lastDelivered[packet.from - 1] + 1) {
                deliver.accept(new ReceivedMessage<>(packet.from, packet.message));
                lastDelivered[packet.from - 1] = packet.n;
                inFlight.get(packet.from - 1).remove(packet);

                if (inFlight.get(packet.from - 1).isEmpty()) {
                    break;
                } else {
                    packet = inFlight.get(packet.from - 1).first();
                }
            }
        } finally {
            inFlightLock.unlock();
        }
    }
}
