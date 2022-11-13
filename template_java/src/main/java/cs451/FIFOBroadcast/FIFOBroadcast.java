package cs451.FIFOBroadcast;

import cs451.Parser.Host;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final AtomicInteger sentCounter = new AtomicInteger(0);

    private final UniformReliableBroadcast<FIFOPacket<T>> urb;
    private final Consumer<ReceivedMessage<T>> deliver;
    private final int myId;
    private final List<SortedSet<FIFOPacket<T>>> inFlight;
    private final Lock inFlightLock = new ReentrantLock();
    private final int[] lastDelivered;

    public FIFOBroadcast(int myId, int port, List<Host> hosts,
                         Consumer<ReceivedMessage<T>> deliver,
                         BiConsumer<T, ByteBuffer> messageSerializer,
                         Function<ByteBuffer, T> messageDeserializer,
                         int messageSize) {
        this.myId = myId;
        this.deliver = deliver;

        List<SortedSet<FIFOPacket<T>>> tmpInFlight = new ArrayList<>();
        for (int i = 0; i < hosts.size(); i++) {
            tmpInFlight.add(new TreeSet<>());
        }
        this.inFlight = Collections.unmodifiableList(tmpInFlight);

        this.lastDelivered = new int[hosts.size()];

        this.urb = new UniformReliableBroadcast<>(myId, port, hosts,
                receivedMessage -> onDeliver(receivedMessage.message),
                (fifoPacket, byteBuffer) -> fifoPacket.serialize(byteBuffer, messageSerializer),
                bb -> FIFOPacket.deserialize(bb, messageDeserializer),
                messageSize + Integer.BYTES + 1);
    }

    public void startThreads() {
        urb.startThreads();
    }

    public void broadcast(T message) {
        FIFOPacket<T> packet = new FIFOPacket<>(sentCounter.incrementAndGet(), myId, message);
        urb.broadcast(packet);
    }

    public void flushBuffers() {
        urb.flushBuffers();
    }

    public void interruptAll() {
        urb.interruptAll();
    }

    private void onDeliver(FIFOPacket<T> packet) {
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
