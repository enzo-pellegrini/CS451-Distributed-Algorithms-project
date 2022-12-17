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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class PerfectLink<T> {
    /**
     * Multiplying factor of the exponential backoff algorithm
     * NO BULLI!
     */
    private static final double BACKOFF_BASE_UP = 2;
    private static final double BACKOFF_BASE_DOWN = 1.5;
    /**
     * If the directedSender sent less than this number of new packages,
     * increase the delay
     */
    private static final double BACKOFF_INCREASE_UPPERBOUND = 3;
    /**
     * If the directedSender sent more than this number of new packages,
     * decrease the delay
     */
    private static final double BACKOFF_DECREASE_LOWERBOUND = 7;
    /**
     * Minimum value the resend pause can assume
     */
    private static final int MIN_RESENDPAUSE = 20;
    /**
     * Maximum value the resend pause can assume
     */
    private static final int MAX_RESENDPAUSE = 2000;
    /**
     * Maximum number of packages to be sent by <code>resenderRoutine</code> before
     * sleeping
     */

    final int myId;
    private final int port;
    final List<Host> hosts;

    private final Set<ReceivedPacket> deliveredSet = Collections.synchronizedSet(new HashSet<>());
    private final Consumer<ReceivedMessage<T>> deliver;
    final BiFunction<T, Integer, Boolean> isCanceled;
    final MSerializer<T> serializer;

    private final Thread receiverThread;

    private final List<DirectedSender<T>> directedSenders;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final DatagramSocket sendingSocket;

    public PerfectLink(int myId, int port, List<Host> hosts, Consumer<ReceivedMessage<T>> deliver,
            BiFunction<T, Integer, Boolean> isCanceled,
            BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer, int messageSize) {
        this.myId = myId;
        this.port = port;
        this.hosts = hosts;
        this.serializer = new MSerializer<>(messageSerializer, messageDeserializer, messageSize);
        this.deliver = deliver;
        this.isCanceled = isCanceled;

        try {
            sendingSocket = new DatagramSocket();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // Initialize directedSenders
        List<DirectedSender<T>> tmp = new ArrayList<>();
        for (int i = 0; i < hosts.size(); i++) {
            tmp.add(new DirectedSender<>(this, i + 1));
        }
        directedSenders = Collections.unmodifiableList(tmp);

        // Create receiver thread
        receiverThread = new Receiver();
    }

    /**
     * Start all needed threads
     */
    public void startThreads() {
        receiverThread.start();

        for (DirectedSender<T> ds : directedSenders) {
            if (ds.dstId == myId)
                continue;
            executor.submit(() -> sendRoutine(ds.dstId));
        }
    }

    private void sendRoutine(int dstId) {
        DirectedSender<T> ds = directedSenders.get(dstId - 1);
        int sent = ds.send(sendingSocket);
        if (sent > 0) {
            // System.out.println("Sent " + sent + " packets to " + ds.dstId);
        }

        if (sent > BACKOFF_DECREASE_LOWERBOUND) {
            ds.resendPause /= BACKOFF_BASE_DOWN;
            ds.resendPause = Math.max(ds.resendPause, MIN_RESENDPAUSE);
        } else if (sent < BACKOFF_INCREASE_UPPERBOUND) {
            ds.resendPause *= BACKOFF_BASE_UP;
            ds.resendPause = Math.min(ds.resendPause, MAX_RESENDPAUSE);
        }

        // System.out.println("Resend pause for " + dstId + " is " + ds.resendPause);

        // schedule next run in resendPause milliseconds
        executor.schedule(() -> sendRoutine(dstId), ds.resendPause, TimeUnit.MILLISECONDS);
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
        int hostId = to.getId();
        directedSenders.get(hostId - 1).schedule(content);
    }

    /**
     * Kill all sender and receiver threads
     * packets waiting to be sent are not sent
     */
    public void interruptAll() {
        receiverThread.interrupt();
        executor.shutdownNow();
    }

    class Receiver extends Thread {
        // Object cached for performance
        private final AckPacket ackPacket = new AckPacket(myId);

        public Receiver() {
            super("Receiver");
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

                        directedSenders.get(ap.receiver_id - 1).handleAck(ap);

                        // System.out.println("Received ack for packet " + ap.n + " from " + ap.receiver_id);
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
            final Host dst = hosts.get(dp.from - 1);

            ackPacket.assignSeqN(dp.n);
            // submit sending of ack to executor
            executor.submit(() -> {
                try {
                    byte[] buff = serializer.serialize(ackPacket);
                    DatagramPacket p = new DatagramPacket(buff, buff.length, InetAddress.getByName(dst.getIp()),
                            dst.getPort());
                    sendingSocket.send(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            // System.out.println("sending ack to " + dp.from + " for packet " + dp.n);
        }
    }
}
