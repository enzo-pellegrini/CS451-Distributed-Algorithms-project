package cs451.PerfectLinks;

import cs451.Parser.Host;
import cs451.PerfectLinks.NetworkTypes.*;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class PerfectLink<T> {
    private static final int SENDER_COUNT = 1;
    /**
     * Multiplying factor of the exponential backoff algorithm
     * NO BULLI!
     */
    private static final double BACKOFF_BASE_UP = 2;
    private static final double BACKOFF_BASE_DOWN = 1.5;
    /**
     * If the average of the sentCount of the values in the resend buffer is lower
     * than this, decrement the resendPause
     */
    private static final double BACKOFF_DECREASE_UPPERBOUND = 1.1;
    /**
     * If the average of the sentCount of the values in the resend buffer is higher
     * than this, increment the resendPause
     */
    private static final double BACKOFF_INCREASE_LOWERBOUND = 1.5;
    /**
     * Minimum value the resend pause can assume
     */
    private static final int MIN_RESENDPAUSE = 20;
    /**
     * Maximum value the resend pause can assume
     */
    private static final int MAX_RESENDPAUSE = 1000;
    /**
     * Maximum number of packages to be sent by <code>resenderRoutine</code> before
     * sleeping
     */
    private static final int MAX_SEND_AT_ONCE = 200;
    private static final int HISTORY_SIZE = 20;

    private final int myId;
    private final int port;
    private final List<Host> hosts;

    private final Lock sendBufferLock = new ReentrantLock();
    private final List<List<T>> sendBuffers;

    /**
     * time to wait before resending the packets waiting to be resent
     * follows exponential backoff
     */
    private int currentResendPause = 50;

    private final AtomicInteger packetCount = new AtomicInteger(0);
    private final BlockingDeque<NetworkTypes.Sendable> senderQueue = new LinkedBlockingDeque<>();
    private final ConcurrentLinkedQueue<Sendable> resendWaitingQueue = new ConcurrentLinkedQueue<>();
    private final Set<Integer> confirmed = ConcurrentHashMap.newKeySet();
    private final Set<ReceivedPacket> deliveredSet = Collections.synchronizedSet(new HashSet<>());
    private final Consumer<ReceivedMessage<T>> deliver;
    private final BiFunction<T, Integer, Boolean> isCanceled;
    private final MSerializer<T> serializer;

    private final Thread[] senderThreads = new Thread[SENDER_COUNT];
    private final Thread receiverThread;
    private final Thread resendTimer;

    public PerfectLink(int myId, int port, List<Host> hosts, Consumer<ReceivedMessage<T>> deliver,
            BiFunction<T, Integer, Boolean> isCanceled,
            BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer, int messageSize) {
        this.myId = myId;
        this.port = port;
        this.hosts = hosts;
        this.serializer = new MSerializer<>(messageSerializer, messageDeserializer, messageSize);
        this.deliver = deliver;
        this.isCanceled = isCanceled;

        List<List<T>> tmp = new ArrayList<>();
        for (int i = 0; i < hosts.size(); i++) {
            tmp.add(new ArrayList<>(8));
        }
        this.sendBuffers = Collections.unmodifiableList(tmp);

        // Start sender workers
        for (int i = 0; i < SENDER_COUNT; i++) {
            senderThreads[i] = new Sender(i);
        }

        // Start receiver worker
        receiverThread = new Receiver();

        resendTimer = new Thread(this::resenderRoutine);
    }

    /**
     * Start all needed threads
     */
    public void startThreads() {
        for (int i = 0; i < SENDER_COUNT; i++) {
            senderThreads[i].start();

            receiverThread.start();

            resendTimer.start();
        }
    }

    /**
     * Add message to queue so that it's <b>eventually</b> sent
     * Adds message to a buffer of MAX_MESSAGES_IN_PACKET packages, call
     * <code>flushMessageBuffers</code>
     * after calling <code>send</code> on all messages
     *
     * @param content message to be sent
     */
    public void send(T content, Host to) {
        sendBufferLock.lock();

        try {
            int toId = to.getId();
            List<T> buffer = sendBuffers.get(toId - 1);
            buffer.add(content);
            if (buffer.size() >= 8) {
                sendBuffer(toId);
            }
        } finally {
            sendBufferLock.unlock();
        }
    }

    public void flushBuffers() {
        sendBufferLock.lock();
        try {
            for (int i = 0; i < sendBuffers.size(); i++) {
                if (!sendBuffers.get(i).isEmpty()) {
                    sendBuffer(i + 1);
                }
            }
        } finally {
            sendBufferLock.unlock();
        }
    }

    private void sendBuffer(int hostId) {
        List<T> buffer = sendBuffers.get(hostId - 1);
        sendSendable(new Sendable(new DataPacket<>(packetCount.incrementAndGet(), myId,
                new ArrayList<>(buffer)), hosts.get(hostId - 1)));
        buffer.clear();
    }

    /**
     * Add sendable to the sender Queue and update all related data structures
     * Lock <code>limitLock</code> first
     *
     * @param se Sendable to be scheduled
     */
    private void sendSendable(Sendable se) {
        resendWaitingQueue.offer(se);
    }

    /**
     * Kill all sender and receiver threads
     * packets waiting to be sent are not sent
     */
    public void interruptAll() {
        for (var senderThread : senderThreads) {
            senderThread.interrupt();
        }
        receiverThread.interrupt();
        resendTimer.interrupt();
    }

    @SuppressWarnings({ "BusyWait", "StatementWithEmptyBody" })
    private void resenderRoutine() {
        Sendable s;
        double[] history = new double[HISTORY_SIZE];
        // init history to 1s
        Arrays.fill(history, 1);
        int historyIndex = 0;
        while (true) {
            try {
                Thread.sleep(currentResendPause);
            } catch (InterruptedException e) {
                System.out.println("Resender routine stopped.");
            }

            flushBuffers();

            int sentCount = 0;
            double average = 0;
            while ((s = resendWaitingQueue.poll()) != null && sentCount < MAX_SEND_AT_ONCE) {

                if (confirmed.contains(s.n)) {
                    confirmed.remove(s.n); // No need to keep track of this package anymore
                } else {
                    // remove all canceled messages from the packet
                    List<T> messages = ((DataPacket<T>) s.message).data;
                    try {
                        final int toId = s.to.getId();
                        messages.removeIf((m) -> isCanceled.apply(m, toId));
                        if (messages.isEmpty()) {
                            continue;
                        }
                    } catch (ConcurrentModificationException ignored) {
                        // ignore, this is not a problem
                    }

                    sentCount++;
                    // gather statistics for incremental backoff
                    average += s.tryCount;

                    senderQueue.offer(s);
                }
            }
            average /= sentCount;

            history[historyIndex] = average > 1 ? average : 1;
            historyIndex = (historyIndex + 1) % HISTORY_SIZE;

            // avgSentCount = sum of sentCount / HISTORY_SIZE, done with for loop
            double avgSentCount = 0;
            for (int i = 0; i < HISTORY_SIZE; i++) {
                avgSentCount += history[i];
            }
            avgSentCount /= HISTORY_SIZE;
            if (sentCount < MAX_SEND_AT_ONCE) {
                continue;
            }
            if (avgSentCount > BACKOFF_INCREASE_LOWERBOUND) {
                currentResendPause = Math.min(MAX_RESENDPAUSE, (int) (currentResendPause * BACKOFF_BASE_UP));
                // System.out.println("[" + myId + "] Incrementing backoff delay to " +
                // currentResendPause + ", sent " + this.packetCount);
            } else if (avgSentCount < BACKOFF_DECREASE_UPPERBOUND) {
                currentResendPause = Math.max(MIN_RESENDPAUSE, (int) (currentResendPause / BACKOFF_BASE_DOWN));
                if (currentResendPause > MIN_RESENDPAUSE) {
                    // System.out.println("[" + myId + "] Decrementing backoff delay to " +
                    // currentResendPause + ", sent " + this.packetCount);
                }
            }
        }
    }

    private class Sender extends Thread {
        private final int ti;

        public Sender(int ti) {
            super();
            this.ti = ti;
        }

        @Override
        public void run() {
            try {
                senderRoutine();
            } catch (InterruptedException e) {
                System.out.println("Sender thread " + ti + " stopped.");
            } catch (IOException e) {
                System.err.println("Sender thread crashed for IOException");
                e.printStackTrace();
            }
        }

        @SuppressWarnings({ "InfiniteLoopStatement", "unchecked", "rawtypes", "BusyWait" })
        private void senderRoutine() throws InterruptedException, IOException {
            try (DatagramSocket s = new DatagramSocket()) {
                while (true) {
                    Sendable se = senderQueue.take();

                    byte[] buf;
                    boolean isAck = true;
                    // prepare network packet
                    if (se.message instanceof DataPacket) {
                        buf = serializer.serialize((DataPacket) se.message);
                        isAck = false;
                    } else {
                        buf = serializer.serialize((AckPacket) se.message);
                    }
                    DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(se.to.getIp()),
                            se.to.getPort());

                    try {
                        // send
                        s.send(p);
                    } catch (IOException e) {
                        System.err.println(e.getMessage());
                        System.out.println("Now sleeping");
                        Thread.sleep(100);
                        senderQueue.putFirst(se);
                    }

                    if (!isAck) {
                        // increment tryCount and put on resendQueue
                        se.tryCount++;
                        resendWaitingQueue.offer(se); // Possible problem: if this thread crashes, se will be lost
                    }
                }
            }
        }
    }

    class Receiver extends Thread {
        public Receiver() {
            super();
        }

        @Override
        public void run() {
            try {
                receiverRoutine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @SuppressWarnings("InfiniteLoopStatement")
        private void receiverRoutine() throws IOException {
            try (DatagramSocket s = new DatagramSocket(port)) {

                System.out.println("Listening on port " + port);

                byte[] buff = new byte[65536];
                while (true) {
                    DatagramPacket p = new DatagramPacket(buff, buff.length);
                    s.receive(p);

                    if (!serializer.isDatapacket(buff)) {
                        AckPacket ap = serializer.deserializeAckPacket(buff);
                        confirmed.add(ap.n);

                        // System.out.println("Received ack for packet " + ap.n);
                    } else {
                        DataPacket<T> dp = serializer.deserializeDataPacket(buff);

                        // System.out.println("Received message " + dp.n + " containing " + dp.data + "
                        // from " + dp.from);

                        sendAckForPacket(dp);

                        if (!deliveredSet.contains(new ReceivedPacket(dp))) {
                            deliveredSet.add(new ReceivedPacket(dp));
                            for (T message : dp.data) {
                                deliver.accept(new ReceivedMessage<>(message, dp.from));
                            }
                        }
                    }
                }
            }
        }

        private void sendAckForPacket(DataPacket<T> dp) {
            final Host dst = hosts.get(dp.from - 1); // TODO: maybe too great of an assumption?

            Sendable se = new Sendable(new AckPacket(dp.n, myId), dst);
            senderQueue.addFirst(se);

            // System.out.println("sending ack to " + se.to.getId());
        }
    }
}
