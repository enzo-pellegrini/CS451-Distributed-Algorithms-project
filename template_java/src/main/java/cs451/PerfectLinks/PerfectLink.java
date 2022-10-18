package cs451.PerfectLinks;

import cs451.Parser.Host;
import cs451.PerfectLinks.NetworkTypes.*;
import cs451.Printer.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class PerfectLink<T extends Serializable> {
    private static final int SENDER_COUNT = 1;
    private static final int MAX_SEND_TRIES = 200;
    /**
     * using send window instead, adding new packages in queue only if the sentCount - min(handlingNow) < MAX_HANDLING
     */
    private static final int MAX_HANDLING = 1_000;
    private static final int RESEND_PAUSE = 50;

    private final int myId;
    private final int port;
    private final List<Host> hosts;
    private final Logger logger = new Logger("PerfectLink");


    private final Lock limitLock = new ReentrantLock();
    private final Condition nonFull = limitLock.newCondition();
    /**
     * Alway lock limitLock first
     */
    private final SortedSet<Integer> handlingNow = new TreeSet<>();


    /**
     * <b>always lock limitLock first</b>
     * Number of packets inserted in the send queue
     * Every new packet to be sent is given as id packetCount++
     */
    private int packetCount = 0;
    private final BlockingQueue<NetworkTypes.Sendable<T>> senderQueue = new ArrayBlockingQueue<>(2 * MAX_HANDLING);
    private final ConcurrentLinkedQueue<Sendable<T>> resendWaitingQueue = new ConcurrentLinkedQueue<>();
    private final Set<Integer> confirmed = Collections.synchronizedSet(new HashSet<>());
    private final Set<ReceivedPacket> deliveredSet = Collections.synchronizedSet(new HashSet<>());
    private final Consumer<DataPacket<T>> deliver;

    private final Thread[] senderThreads = new Thread[SENDER_COUNT];
    private final Thread receiverThread;
    private final Thread resendTimer;

    public PerfectLink(int myId, int port, List<Host> hosts, Consumer<DataPacket<T>> deliver) {
        this.myId = myId;
        this.port = port;
        this.hosts = hosts;
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
     * <br><b>unless the receiver is deemed dead</b>
     *
     * @param content message to be sent
     */
    public void send(T content, Host to) {
        limitLock.lock();

        try {
            while (packetCount - (handlingNow.isEmpty() ? 0 : handlingNow.first()) >= MAX_HANDLING) {
                try {
                    nonFull.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            packetCount++;
            handlingNow.add(packetCount);
        } finally {
            limitLock.unlock();
        }
        senderQueue.offer(new Sendable<>(new DataPacket<>(packetCount, myId, content), to));
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

        try {
            logger.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void resenderRoutine() {
        Sendable<T> s;
        while (true) {
            try {
                Thread.sleep(RESEND_PAUSE);
            } catch (InterruptedException e) {
                logger.log("Resender routine stopped.");
            }

            while ((s = resendWaitingQueue.poll()) != null) {
                senderQueue.offer(s);
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
                logger.log("Sender thread " + ti + " stopped.");
            } catch (IOException e) {
                System.err.println("Sender thread crashed for IOException");
                e.printStackTrace();
            }
        }

        private void senderRoutine() throws InterruptedException, IOException {
            try (DatagramSocket s = new DatagramSocket()) {
                while (true) {
                    Sendable<T> se = senderQueue.take();

                    // TODO: notify ReliableChannel user that the receiving process has failed
                    if (se.tryCount < MAX_SEND_TRIES && !confirmed.contains(se.n)) {
                        byte[] buf = se.getSerializedMessage();
                        DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(se.to.getIp()), se.to.getPort());

                        s.send(p);

//                    System.out.println("Sent " + se.message.data + " to port " + se.to.getPort());

                        se.tryCount++; // IMPORTANT
                        resendWaitingQueue.offer(se); // Possible problem: if this thread crashes, se will be lost
                    } else if (se.tryCount >= MAX_SEND_TRIES) {
                        logger.log("Dropped packet " + se.n);
                    } else {
                        // Don't need to keep track of confirmed packages anymore
                        confirmed.remove(se.n);

                        limitLock.lock();
                        handlingNow.remove(se.n);
                        nonFull.signal();
                        limitLock.unlock();
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

                logger.log("Listening on port " + port);

                while (true) {
                    byte[] buff = new byte[512];
                    DatagramPacket p = new DatagramPacket(buff, buff.length);
                    s.receive(p);

                    Object received = Serialization.deserialize(buff);

                    if (received instanceof AckPacket) {
                        AckPacket ap = (AckPacket) received;
                        confirmed.add(ap.n);
//                        logger.log("Received ack for packet " + ap.n);
                    } else if (received instanceof NetworkTypes.DataPacket) {
                        DataPacket dp = (DataPacket) received;

                        sendAckForPacket(s, dp);

                        if (!deliveredSet.contains(new ReceivedPacket(dp))) {
                            deliveredSet.add(new ReceivedPacket(dp));
                            deliver.accept(dp);
                        }
                    } else {
                        throw new IOException();
                    }
                }
            }
        }

        private void sendAckForPacket(DatagramSocket s, DataPacket dp) throws IOException {
            byte[] buff = Serialization.serialize(new NetworkTypes.AckPacket(dp.n, myId));
            final Host dst = hosts.get(dp.from - 1); // TODO: maybe too great of an assumption?
            s.send(new DatagramPacket(buff, buff.length, InetAddress.getByName(dst.getIp()), dst.getPort()));
        }
    }
}
