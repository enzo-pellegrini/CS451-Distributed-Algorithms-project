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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class PerfectLink<T> {
    private static final int MAX_MESSAGES_IN_PACKET = 8;
    private static final int SENDER_COUNT = 1;
//    private static final int MAX_SEND_TRIES = 200;
    /**
     * using send window instead, adding new packages in queue only if the sentCount - min(handlingNow) < MAX_HANDLING
     */
    private static final int MAX_HANDLING = 2_000;

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

    private final int myId;
    private final int port;
    private final List<Host> hosts;


    private final Lock limitLock = new ReentrantLock();
    private final Condition nonFull = limitLock.newCondition();
    /**
     * Alway lock <code>limitLock</code> first
     */
    private int handlingNow = 0;

    /**
     * time to wait before rensending the packets waiting to be resent
     * follows exponential backoff
     */
    private int currentResendPause = 50;
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
    private final LinkedBlockingDeque<NetworkTypes.Sendable<T>> senderQueue = new LinkedBlockingDeque<>();
    private final ConcurrentLinkedQueue<Sendable<T>> resendWaitingQueue = new ConcurrentLinkedQueue<>();
    private final Set<Integer> confirmed = Collections.synchronizedSet(new HashSet<>());
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
        this.sendBuffer = new ArrayList<>(hosts.size());
        for (int i = 0; i < hosts.size(); i++) sendBuffer.add(new ArrayList<>(MAX_MESSAGES_IN_PACKET));
        this.deliver = deliver;

        // Start sender workers
        for (int i = 0; i < SENDER_COUNT; i++) {
            senderThreads[i] = new Sender(i);
            senderThreads[i].start();
        }

        // Start receiver worker
        receiverThread = new Receiver(0);
        receiverThread.start();

        resendTimer = new Thread(this::resenderRoutine);
        resendTimer.start();
    }

    /**
     * Add message to queue so that it's <b>eventually</b> sent
     * Adds message to a buffer of MAX_MESSAGES_IN_PACKET packages, call <code>flushMessageBuffers</code>
     * after calling <code>send</code> on all messages
     *
     * @param content message to be sent
     */
    public void send(T content, Host to) {
        limitLock.lock();

        try {
            while (handlingNow >= MAX_HANDLING) {
                try {
                    nonFull.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            List<T> buff = sendBuffer.get(to.getId() - 1);
            assert (buff.size() < MAX_MESSAGES_IN_PACKET);   // It can't be MAX_MESSAGES_IN_PACKET if I always send when full

            buff.add(content);

            if (buff.size() == MAX_MESSAGES_IN_PACKET) {
                Sendable<T> se = new Sendable<>(new DataPacket<>(++packetCount, myId, new ArrayList<>(buff)), to);
                sendSendable(se);
                buff.clear();
            }
        } finally {
            limitLock.unlock();
        }
    }

    public void flushMessageBuffers() {
        limitLock.lock();

        try {
            for (int i = 0; i < hosts.size(); i++) {
                List<T> buff = sendBuffer.get(i);
                if (buff.size() > 0) {
                    Sendable<T> se = new Sendable<>(new DataPacket<>(++packetCount, myId, new ArrayList<>(buff)), hosts.get(i));
                    sendSendable(se);
                    buff.clear();
                }
            }
        } finally {
            limitLock.unlock();
        }
    }

    /**
     * Add sendable to the sender Queue and update all related data structures
     * Lock <code>limitLock</code> first
     *
     * @param se Sendable to be scheduled
     */
    private void sendSendable(Sendable<T> se) {
        senderQueue.offer(se);
        handlingNow++;
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

    private void resenderRoutine() {
        Sendable<T> s;
        while (true) {
            try {
                Thread.sleep(currentResendPause);
            } catch (InterruptedException e) {
                System.out.println("Resender routine stopped.");
            }

            int packetCount = 0;
            double sentCountSum = 0;
            while ((s = resendWaitingQueue.poll()) != null) {
                // gather statistics for incremental backoff
                packetCount++;
                sentCountSum += s.tryCount;

                if (confirmed.contains(s.n)) {
                    confirmed.remove(s.n); // No need to keep track of this package anymore

                    // The upper level can now add more packages
                    // TODO: why not do this when the ack is received?
                    limitLock.lock();
                    try {
                        handlingNow--;
                        nonFull.signal();
                    } finally {
                        limitLock.unlock();
                    }
                } else {
                    senderQueue.offer(s);

                }
            }

            // Exponential backoff logic
            double avgSentCount = sentCountSum / packetCount;
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
                } else {
//                    System.out.println("Changing nothing");
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

        private void senderRoutine() throws InterruptedException, IOException {
            try (DatagramSocket s = new DatagramSocket()) {
                while (true) {
                    Sendable<T> se = senderQueue.take();

                    byte[] buf;
                    boolean isAck = true;
                    // prepare network packet
                    if (se.message instanceof DataPacket) {
                        buf = serializer.serialize((DataPacket)se.message);
                        isAck = false;
                    } else {
                        buf = serializer.serialize((AckPacket)se.message);
                    }
                    DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(se.to.getIp()), se.to.getPort());

                    // send
                    s.send(p);

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
        private int ti;

        public Receiver(int ti) {
            super();
            this.ti = ti;
        }

        @Override
        public void run() {
            try {
                receiverRoutine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

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

                        sendAckForPacket(s, dp);

                        if (!deliveredSet.contains(new ReceivedPacket(dp))) {
                            deliveredSet.add(new ReceivedPacket(dp));
                            for (Object m : dp.data) {
                                T message = (T) m;
                                deliver.accept(new ReceivedMessage<>(message, dp.from));
                            }
                        }
                    }
                }
            }
        }

        private void sendAckForPacket(DatagramSocket s, DataPacket<T> dp) throws IOException {
            final Host dst = hosts.get(dp.from - 1); // TODO: maybe too great of an assumption?

            Sendable<T> se = new Sendable(new AckPacket(dp.n, myId), dst);
            senderQueue.addFirst(se);

//            System.out.println("sending ack to " + se.to.getId());
        }
    }
}
