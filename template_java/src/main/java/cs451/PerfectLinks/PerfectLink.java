package cs451.PerfectLinks;

import cs451.Parser.Host;
import cs451.PerfectLinks.NetworkTypes.*;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class PerfectLink<T extends Serializable> {
    private static final int SENDER_COUNT = 1;
    private static final int MAX_TRYCOUNT = 10;
    private static final int MAX_HANDLING = 1_000;
    private static final int RESEND_PAUSE = 100;

    private final int myId;
    private final int port;
    private final List<Host> hosts;

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final Lock limitLock = new ReentrantLock();
    private final Condition nonFull = limitLock.newCondition();
    /**
     * Alway lock limitLock first
     */
    private int handlingNow;

    // private final PoisoningPriorityQueue<Sendable> senderQueue = new
    // PoisoningPriorityQueue<>();
    private final BlockingQueue<NetworkTypes.Sendable<T>> senderQueue = new ArrayBlockingQueue<>(2 * MAX_HANDLING);
    private final ConcurrentLinkedQueue<Sendable<T>> resendWaitingQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger sentCount = new AtomicInteger(); // All good assuming it doesn't overflow
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

    public int getPendingBastards() {
        return senderQueue.size();
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
            while (handlingNow >= MAX_HANDLING) {
                try {
                    nonFull.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            handlingNow++;
        } finally {
            limitLock.unlock();
        }
        boolean success = senderQueue.offer(new Sendable<T>(new DataPacket<>(sentCount.getAndIncrement(), myId, content), to));
        assert success;
    }

    /**
     * Kill all sender and receiver threads
     * packets waiting to be sent are not sent
     */
    public void poison() {
        running.set(false);
        // senderQueue.poison();
    }

    private void resenderRoutine() {
        Sendable<T> s;
        while (running.get()) {
            try {
                Thread.sleep(RESEND_PAUSE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while ((s = resendWaitingQueue.poll()) != null) {
                boolean success = senderQueue.offer(s);
                assert success;
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
            // while (running.get()) {
            try {
                senderRoutine();
            } catch (Exception e) {
                System.err.println("Sender thread " + ti + " crashed!");
                e.printStackTrace();
            }
            // }
        }

        private void senderRoutine() throws InterruptedException, IOException {
            DatagramSocket s = new DatagramSocket();

            while (running.get()) {
                Sendable<T> se = senderQueue.take();
//                Sendable<T> se;
//                do {
//                    se = senderQueue.poll();
//                } while (se == null);

                if (se == null) {
                    break;
                }

                // TODO: notify ReliableChannel user that the receiving process has failed
                if (se.tryCount < MAX_TRYCOUNT && !confirmed.contains(se.n)) {
                    byte[] buf = se.getSerializedMessage();
                    DatagramPacket p = new DatagramPacket(buf, buf.length, InetAddress.getByName(se.to.getIp()), se.to.getPort());

                    s.send(p);

//                    System.out.println("Sent " + se.message.data + " to port " + se.to.getPort());

                    se.tryCount++; // IMPORTANT
                    resendWaitingQueue.offer(se); // Possible problem: if this thread crashes, se will be lost
                } else if (se.tryCount >= MAX_TRYCOUNT) {
                    System.out.println("Dropped packet " + se.n);
                } else {
                    // the packet is thrown away, can reduce memory usage now
                    confirmed.remove(se.n);

                    limitLock.lock();
                    handlingNow--;
                    nonFull.signal();
                    limitLock.unlock();
                }
            }

            s.close();
        }
    }

    class Receiver extends Thread {
        private int ti;
        private DatagramSocket s;

        public Receiver(int ti) {
            super();
            this.ti = ti;
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    receiverRoutine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void receiverRoutine() throws IOException {
            s = new DatagramSocket(port);

            System.out.println("Listening on port " + port);

            try {
                while (running.get()) {
                    byte[] buff = new byte[512];
                    DatagramPacket p = new DatagramPacket(buff, buff.length);
                    s.receive(p);

                    Object received = Serialization.deserialize(buff);

                    if (received instanceof AckPacket) {
                        AckPacket ap = (AckPacket) received;
                        confirmed.add(ap.n);
//                        System.out.println("Received ack for packet " + ap.n);
                    } else if (received instanceof NetworkTypes.DataPacket) {
                        DataPacket dp = (DataPacket) received;

                        sendAckForPacket(dp);

                        if (!deliveredSet.contains(new ReceivedPacket(dp))) {
                            deliveredSet.add(new ReceivedPacket(dp));
                            deliver.accept(dp);
                        }
                    } else {
                        throw new IOException();
                    }
                }
            } finally {
                s.close();
            }
        }

        private void sendAckForPacket(DataPacket dp) throws IOException {
            byte[] buff = Serialization.serialize(new NetworkTypes.AckPacket(dp.n, myId));
            final Host dst = hosts.get(dp.from - 1); // TODO: maybe too great of an assumption?
            s.send(new DatagramPacket(buff, buff.length, InetAddress.getByName(dst.getIp()), dst.getPort()));
        }
    }
}
