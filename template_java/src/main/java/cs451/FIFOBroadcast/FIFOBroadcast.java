package cs451.FIFOBroadcast;

import cs451.Parser.Host;
import cs451.UniformReliableBroadcast.ReceivedURBMessage;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
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

    // Limit the number of messages we handle at once
    private final Lock handlingNowLock = new ReentrantLock();
    private final Condition canHandleMore = handlingNowLock.newCondition();
    private final int MAX_HANDLING;
    private final int MAX_HANDLING_UNDERNEATH = 2_500;
    private final int MAX_HERE = 25;
    private int handlingNow = 0;
    private final int myId;
    private final int[] lastDelivered;

    public FIFOBroadcast(int myId, int port, List<Host> hosts,
                         Consumer<ReceivedMessage<T>> deliver,
                         BiConsumer<T, ByteBuffer> messageSerializer,
                         Function<ByteBuffer, T> messageDeserializer,
                         int messageSize) {
        this.deliver = deliver;

        // Mother of all cheats
        this.MAX_HANDLING = Math.min(MAX_HERE, Math.max(1, MAX_HANDLING_UNDERNEATH / (hosts.size() * hosts.size())));
        this.myId = myId;

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
        // wait for handlingNow to be less than MAX_HANDLING
        handlingNowLock.lock();
        try {
            while (handlingNow >= MAX_HANDLING) {
                try {
                    canHandleMore.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            handlingNow++;
            handlingNowLock.unlock();
        }
        urb.broadcast(message);
    }

    public void interruptAll() {
        urb.interruptAll();
    }

    private void onDeliver(ReceivedURBMessage<T> packet) {
        int deliveredFromMe = 0;

        inFlightLock.lock();
        try {
            inFlight.get(packet.from - 1).add(packet);

//            System.out.println("Received message " + packet);
            if (packet.from == myId) {
                deliveredFromMe++;
            }

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

        if (deliveredFromMe > 0) {
            handlingNowLock.lock();
            try {
                handlingNow -= deliveredFromMe;
                canHandleMore.signal();
            } finally {
                handlingNowLock.unlock();
            }
        }
    }
}
