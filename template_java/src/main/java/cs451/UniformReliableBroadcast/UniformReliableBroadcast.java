package cs451.UniformReliableBroadcast;

import cs451.Parser.Host;
import cs451.PerfectLinks.PerfectLink;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicInteger sentCounter = new AtomicInteger(0);

    private final PerfectLink<URPacket<T>> pl;
    private final int myId;
    private final List<Host> hosts;
    private final int numHosts;
    private final Consumer<ReceivedURBMessage<T>> deliver;
    private final Lock inFlightLock = new ReentrantLock();
    private final Map<URPacket<T>, Integer> inFlight = new HashMap<>();
    private final Set<DeliveredMessage> delivered = new HashSet<>();


    public UniformReliableBroadcast(int myId, int port, List<Host> hosts,
                                    Consumer<ReceivedURBMessage<T>> deliver,
                                    BiConsumer<T, ByteBuffer> messageSerializer,
                                    Function<ByteBuffer, T> messageDeserializer,
                                    int messageSize) {

        this.myId = myId;
        this.hosts = hosts;
        this.numHosts = hosts.size();

        this.deliver = deliver;

        this.pl = new PerfectLink<>(myId, port, hosts,
                receivedMessage -> onDeliver(receivedMessage.data),
                (m, hostId) -> false,
                (urPacket, byteBuffer) -> urPacket.serialize(byteBuffer, messageSerializer),
                bb -> URPacket.deserialize(bb, messageDeserializer),
                messageSize + Integer.BYTES + 1);
    }

    public void startThreads() {
        pl.startThreads();
    }

    public void interruptAll() {
        pl.interruptAll();
    }

    /**
     * Broadcast message to all senders using Best Effort Broadcast
     *
     * @param message message to broadcast
     */
    public void broadcast(T message) {
        URPacket<T> packet = new URPacket<>(sentCounter.incrementAndGet(), myId, message);

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

    private void onDeliver(URPacket<T> packet) {
        boolean shouldBroadcast = false;
        boolean shouldDeliver = false;

        inFlightLock.lock();
        try {
            Integer soFar = inFlight.getOrDefault(packet, 1);

            inFlight.put(packet, soFar + 1); // update number of processes that acked

            if (soFar == 1 && packet.from != myId) {
                shouldBroadcast = true;
            }
            if ((soFar+1 >= ((numHosts/2) + 1))
                        && !delivered.contains(new DeliveredMessage(packet.from, packet.n))) {
                delivered.add(new DeliveredMessage(packet.from, packet.n));
                shouldDeliver = true;
            }
        } finally {
            inFlightLock.unlock();
        }

        if (shouldBroadcast) bestEffortBroadcast(packet);
        if (shouldDeliver) {
            deliver.accept(new ReceivedURBMessage<>(packet.from, packet.n, packet.message));
        }

//        System.out.println("URB: Delivered: " + packet.from + " " + packet.n);
    }
}
