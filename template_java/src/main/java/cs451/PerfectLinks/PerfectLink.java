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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class PerfectLink<T> {
    private static final int MAX_MESSAGES_IN_PACKET = 8;
    private static final int SENDER_COUNT = 1;
    /**
     * Multiplying factor of the exponential backoff algorithm
     */
    private static final double BACKOFF_BASE_UP = 1.2;
    private static final double BACKOFF_BASE_DOWN = 1.5;
    /**
     * If the average of the sentCount of the values in the resend buffer is lower than this, decrement the resendPause
     */
    private static final double BACKOFF_DECREASE_UPPERBOUND = 1.1;
    /**
     * If the average of the sentCount of the values in the resend buffer is higher than this, increment the resendPause
     */
    private static final double BACKOFF_INCREASE_LOWERBOUND = 1.5;
    /**
     * Minimum value the resend pause can assume
     */
    private static final int MIN_RESENDPAUSE = 10;
    /**
     * Maximum value the resend pause can assume
     */
    private static final int MAX_RESENDPAUSE = 5000;
    /**
     * Maximum number of packages to be sent by <code>resenderRoutine</code> before sleeping
     */
    private static final int MAX_SEND_AT_ONCE = 2500;

    private final int myId;
    private final int port;
    private final List<Host> hosts;



    /**
     * time to wait before resending the packets waiting to be resent
     * follows exponential backoff
     */
    private int currentResendPause = 50;
    /**
     * lock for sendBuffer
     */
    private final Lock sendBufferLock = new ReentrantLock();
    /**
     * For each host_id, list of packets waiting to be sent (up to MAX_MESSAGES_IN_PACKET)
     * take <code>limitLock</code> before modifying the lists or adding new ones
     */
    private final List<List<T>> sendBuffer;


    /**
     * <b>always lock <code>limitLock</code> first</b>
     * Number of packets inserted in the send queue
     * Every new packet to be sent is given as id packetCount++
     */
    private int packetCount = 0;
    private final BlockingDeque<NetworkTypes.Sendable> senderQueue = new LinkedBlockingDeque<>();
    private final ConcurrentLinkedQueue<Sendable> resendWaitingQueue = new ConcurrentLinkedQueue<>();
    private final Set<Integer> confirmed = ConcurrentHashMap.newKeySet();
    private final Set<ReceivedPacket> deliveredSet = Collections.synchronizedSet(new HashSet<>());
    private final Consumer<ReceivedMessage<T>> deliver;
    private final MSerializer<T> serializer;

    private final Thread[] senderThreads = new Thread[SENDER_COUNT];
    private final Thread receiverThread;
    private final Thread resendTimer;

    public PerfectLink(int myId, int port, List<Host> hosts, Consumer<ReceivedMessage<T>> deliver,
                       BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer, int messageSize) {
        this.myId = myId;
        this.port = port;
        this.hosts = hosts;
        this.serializer = new MSerializer<>(messageSerializer, messageDeserializer, messageSize);
        List<List<T>> tmpBuffer = new ArrayList<>(hosts.size());
        for (int i = 0; i < hosts.size(); i++) tmpBuffer.add(new ArrayList<>(MAX_MESSAGES_IN_PACKET));
        this.sendBuffer = Collections.unmodifiableList(tmpBuffer);
        this.deliver = deliver;

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
        for (int i=0; i < SENDER_COUNT; i++) {
            senderThreads[i].start();

            receiverThread.start();

            resendTimer.start();
        }
    }

    /**
     * Add message to queue so that it's <b>eventually</b> sent
     * Adds message to a buffer of MAX_MESSAGES_IN_PACKET packages, call <code>flushMessageBuffers</code>
     * after calling <code>send</code> on all messages
     *
     * @param content message to be sent
     */
    public void send(T content, Host to) {
        sendBufferLock.lock();

        try {
            List<T> buff = sendBuffer.get(to.getId() - 1);
            assert (buff.size() < MAX_MESSAGES_IN_PACKET);   // It can't be MAX_MESSAGES_IN_PACKET if I always send when full

            buff.add(content);

            if (buff.size() == MAX_MESSAGES_IN_PACKET) {
                Sendable se = new Sendable(new DataPacket<>(++packetCount, myId, new ArrayList<>(buff)), to);
                sendSendable(se);
                buff.clear();
            }
        } finally {
            sendBufferLock.unlock();
        }
    }

    public void flushMessageBuffers() {
        sendBufferLock.lock();

        try {
            for (int i = 0; i < hosts.size(); i++) {
                List<T> buff = sendBuffer.get(i);
                if (buff.size() > 0) {
                    Sendable se = new Sendable(new DataPacket<>(++packetCount, myId, new ArrayList<>(buff)), hosts.get(i));
                    sendSendable(se);
                    buff.clear();
                }
            }
        } finally {
            sendBufferLock.unlock();
        }
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

    @SuppressWarnings("BusyWait")
    private void resenderRoutine() {
        Sendable s;
        while (true) {
            try {
                Thread.sleep(currentResendPause);
            } catch (InterruptedException e) {
                System.out.println("Resender routine stopped.");
            }

            int packetCount = 0;
            double sentTrySum = 0;
            while ((s = resendWaitingQueue.poll()) != null && packetCount < MAX_SEND_AT_ONCE) {
                // gather statistics for incremental backoff
                packetCount++;
                sentTrySum += s.tryCount;

                if (confirmed.contains(s.n)) {
                    confirmed.remove(s.n); // No need to keep track of this package anymore

                } else {
                    senderQueue.offer(s);

                }
            }

            // Exponential backoff logic
            double avgSentCount = sentTrySum / packetCount;
            if (packetCount > 2000) { // The algorithm is quite unstable when the queue is empty
                if (avgSentCount > BACKOFF_INCREASE_LOWERBOUND) {
                    currentResendPause = Math.min(MAX_RESENDPAUSE, (int) (currentResendPause * BACKOFF_BASE_UP));
//                    if (currentResendPause > 40) {
                    System.out.println("[" + myId + "] Incrementing backoff delay to " + currentResendPause + ", sent " + this.packetCount);
//                    }
                } else if (avgSentCount < BACKOFF_DECREASE_UPPERBOUND) {
                    currentResendPause = Math.max(MIN_RESENDPAUSE, (int) (currentResendPause / BACKOFF_BASE_DOWN));
                    if (currentResendPause > MIN_RESENDPAUSE) {
                        System.out.println("[" + myId + "] Decrementing backoff delay to " + currentResendPause + ", sent " + this.packetCount);
                    }
                }
            }

            // Flush buffers cause doing it in the upper level is a mess
            // TODO: where can I move this?
            flushMessageBuffers();
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

        @SuppressWarnings({"InfiniteLoopStatement", "unchecked", "rawtypes", "BusyWait"})
        private void senderRoutine() throws InterruptedException, IOException {
            try (DatagramSocket s = new DatagramSocket()) {
                while (true) {
                    Sendable se = senderQueue.take();

                    byte[] buf;
                    boolean isAck = true;
                    // prepare network packet
                    if (se.message instanceof DataPacket) {
                        if (se.serialized == null) {
                            se.serialized = serializer.serialize((DataPacket) se.message);
                        }
                        buf = se.serialized;
                        isAck = false;
                    } else {
                        buf = serializer.serialize((AckPacket)se.message);
                    }
                    DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(se.to.getIp()), se.to.getPort());

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

                while (true) {
                    byte[] buff = new byte[512];
                    DatagramPacket p = new DatagramPacket(buff, buff.length);
                    s.receive(p);

                    if (!serializer.isDatapacket(buff)) {
                        AckPacket ap = serializer.deserializeAckPacket(buff);
                        confirmed.add(ap.n);
//                        System.out.println("Received ack for packet " + ap.n);
                    } else {
                        DataPacket<T> dp = serializer.deserializeDataPacket(buff);

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

//            System.out.println("sending ack to " + se.to.getId());
        }
    }
}
