package cs451.UniformReliableBroadcast;

import cs451.Parser.Host;
import cs451.PerfectLinks.PerfectLink;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Uniform Reliable Broadcast
 */
@SuppressWarnings("FieldCanBeLocal")
public class UniformReliableBroadcast<T> {
    private final int MAX_HANDLING_UNDERNEATH = 10_000;
    private final int MAX_HANDLING;
    public static class ReceivedURBMessage<T> {
        public final int from;
        public final T message;

        public ReceivedURBMessage(int from, T message) {
            this.from = from;
            this.message = message;
        }
    }

    private final AtomicInteger sentCounter = new AtomicInteger(0);

    private final PerfectLink<URPacket<T>> pl;
    private final int myId;
    private final List<Host> hosts;
    private final Consumer<ReceivedURBMessage<T>> deliver;
    private final Lock inFlightLock = new ReentrantLock();
    private final Map<URPacket<T>, Integer> inFlight = new HashMap<>();
    private final Set<DeliveredMessage> delivered = new HashSet<>();

    private final Lock handlingNowLock = new ReentrantLock();
    private final Condition canHandleMore = handlingNowLock.newCondition();

    private int handlingNow = 0;

    public UniformReliableBroadcast(int myId, int port, List<Host> hosts,
                                    Consumer<ReceivedURBMessage<T>> deliver,
                                    BiConsumer<T, ByteBuffer> messageSerializer,
                                    Function<ByteBuffer, T> messageDeserializer,
                                    int messageSize) {

        this.pl = new PerfectLink<>(myId, port, hosts,
                receivedMessage -> onDeliver(receivedMessage.data),
                (urPacket, byteBuffer) -> urPacket.serialize(byteBuffer, messageSerializer),
                bb -> URPacket.deserialize(bb, messageDeserializer),
                messageSize + Integer.BYTES + 1);

        this.myId = myId;
        this.hosts = hosts;

        this.MAX_HANDLING = Math.max(1, MAX_HANDLING_UNDERNEATH / (hosts.size() * hosts.size()));

        this.deliver = deliver;
    }

    /**
     * Broadcast message to all senders using Best Effort Broadcast
     *
     * @param message message to broadcast
     */
    public void broadcast(T message) {
        URPacket<T> packet = new URPacket<>(sentCounter.incrementAndGet(), myId, message);

        handlingNowLock.lock();
        try {
            while (handlingNow >= MAX_HANDLING) {
                try {
                    canHandleMore.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            handlingNow++;
            handlingNowLock.unlock();
        }

        inFlightLock.lock();
        try {
            inFlight.put(packet, 1); // If I send it, at least I have it
        } finally {
            inFlightLock.unlock();
        }

        bestEffortBroadcast(packet);
    }

    private void bestEffortBroadcast(URPacket<T> packet) {
        for (Host host : hosts) {
            if (host.getId() != myId) {
                pl.send(packet, host);
            }
        }
    }

    public void flushBuffers() {
        pl.flushMessageBuffers();
    }

    private void onDeliver(URPacket<T> packet) {
        boolean shouldBroadcast = false;
        boolean shouldDeliver = false;

        inFlightLock.lock();
        try {
            Integer soFar = inFlight.getOrDefault(packet, 1);

//            System.out.println("[PerfectLinks] Received message " + packet.message.toString() + " from " + packet.from
//                    + "\n it has " + soFar + " confirmed");

            inFlight.put(packet, soFar + 1); // update number of processes that acked

            if (soFar == 1 && packet.from != myId) {
                shouldBroadcast = true;
            }
            if ((soFar+1 >= ((hosts.size()/2) + 1))
                        && !delivered.contains(new DeliveredMessage(packet.from, packet.n))) {
                delivered.add(new DeliveredMessage(packet.from, packet.n));
                shouldDeliver = true;
            }
        } finally {
            inFlightLock.unlock();
        }

        if (shouldBroadcast) bestEffortBroadcast(packet);
        if (shouldDeliver) {
            deliver.accept(new ReceivedURBMessage<>(packet.from, packet.message));

            handlingNowLock.lock();
            try {
                handlingNow--;
                canHandleMore.signal();
            } finally {
                handlingNowLock.unlock();
            }
        }
    }
}
