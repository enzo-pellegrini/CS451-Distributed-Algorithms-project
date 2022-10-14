package cs451;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ReliableChannel<T extends Serializable> {
    private static final int SENDER_COUNT = 4;
    private static final int MAX_TRYCOUNT = 20;

    private final int senderId;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final PoisoningPriorityQueue<Sendable> senderQueue = new PoisoningPriorityQueue<>();
    private final AtomicInteger sentCount = new AtomicInteger(); // All good assuming it doesn't overflow
    private final Set<Integer> confirmed = Collections.synchronizedSet(new HashSet<>());
    private final Set<ReceivedPacket> deliveredSet = Collections.synchronizedSet(new HashSet<>());
    private final Consumer<DataPacket<T>> deliver;

    private final Thread[] senderThreads = new Thread[SENDER_COUNT];
    private final Thread receiverThread;

    public ReliableChannel(int senderId, int port, Consumer<DataPacket<T>> deliver) {
        this.senderId = senderId;
        this.port = port;
        this.deliver = deliver;

        // Start sender workers
        for (int i = 0; i < SENDER_COUNT; i++) {
            senderThreads[i] = new Sender(i);
            senderThreads[i].start();
        }

        // Start receiver worker
        receiverThread = new Receiver(0);
    }

    /**
     * Add message to queue so that it's <b>eventually</t> sent
     * <i>unless the receiver is deemed dead
     * 
     * @param content message to be sent
     */
    public void send(T content, Host to) {
        senderQueue.offer(
                new Sendable(
                        new DataPacket<>(sentCount.getAndIncrement(), senderId, content),
                        to));
    }

    /**
     * Kill all sender and receiver threads
     * packets waiting to be sent are not sent
     */
    public void poison() {
        running.set(false);
        senderQueue.poison();
    }

    public static class DataPacket<T extends Serializable> implements Serializable {
        public final int n;
        public final int from;
        public final T data;

        public DataPacket(int n, int from, T data) {
            this.n = n;
            this.from = from;
            this.data = data;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + n;
            result = prime * result + from;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DataPacket other = (DataPacket<T>) obj;
            if (n != other.n)
                return false;
            if (from != other.from)
                return false;
            return true;
        }
    }

    public static class AckPacket implements Serializable {
        public final int n;
        public final int receiver_id;

        public AckPacket(int n, int receiver_id) {
            this.n = n;
            this.receiver_id = receiver_id;
        }
    }

    public static class ReceivedPacket {
        public final int n;
        public final int sender;

        public ReceivedPacket(int n, int sender) {
            this.n = n;
            this.sender = sender;
        }

        public ReceivedPacket(DataPacket<?> dp) {
            this(dp.n, dp.from);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + n;
            result = prime * result + sender;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ReceivedPacket other = (ReceivedPacket) obj;
            if (n != other.n)
                return false;
            if (sender != other.sender)
                return false;
            return true;
        }
    }

    private class Sendable implements Comparable<Sendable> {
        public final int n;
        public final Host to;
        public int tryCount = 0;
        public DataPacket<T> message;

        private byte[] buff;

        public Sendable(DataPacket<T> message, Host to) {
            this.n = message.n;
            this.message = message;
            this.to = to;
        }

        public byte[] getSerializedMessage() {
            if (buff == null) {
                buff = serialize(message);
            }
            return buff;
        }

        @Override
        public int compareTo(Sendable o) {
            if (this.tryCount > o.tryCount) {
                return -1;
            } else if (this.tryCount < o.tryCount) {
                return 1;
            } else {
                return this.n < o.n ? +1 : -1;
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
            while (running.get()) {
                try {
                    senderRoutine();
                } catch (Exception e) {
                    System.err.println("Sender thread " + ti + " crashed!");
                    e.printStackTrace();
                }
            }
        }

        private void senderRoutine() throws InterruptedException, IOException {
            DatagramSocket s = new DatagramSocket();

            try {
                while (running.get()) {
                    Sendable se = senderQueue.take();
                    if (se == null) {
                        break;
                    }

                    byte[] buf = se.getSerializedMessage();
                    DatagramPacket p = new DatagramPacket(
                            buf,
                            buf.length,
                            InetAddress.getByName(se.to.getIp()),
                            se.to.getPort());

                    s.send(p);

                    // TODO: notify ReliableChannel user that the receiving process has failed
                    if (se.tryCount > MAX_TRYCOUNT || !confirmed.contains(se.n)) {
                        senderQueue.offer(se); // Possible problem: if this thread crashes, se will be lost
                    } else {
                        // the packet is thrown away, can reduce memory usage now
                        confirmed.remove(se.n);
                    }
                }
            } finally {
                s.close();
            }
        }
    }

    private class Receiver extends Thread {
        private int ti;

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
            DatagramSocket s = new DatagramSocket(port);

            try {
                while (running.get()) {
                    DatagramPacket p = new DatagramPacket(null, 0);
                    s.receive(p);
                    byte[] buff = p.getData();

                    Object received = deserialize(buff);

                    if (received instanceof AckPacket) {
                        AckPacket ap = (AckPacket) received;
                        confirmed.add(ap.n);
                    } else if (received instanceof DataPacket) {
                        DataPacket<T> dp = (DataPacket) received;
                        deliveredSet.add(new ReceivedPacket(dp));
                        deliver.accept(dp);
                    } else {
                        throw new IOException();
                    }
                }
            } finally {
                s.close();
            }
        }
    }

    /**
     * Serialize object to byte array
     * 
     * @param data Message to be serialized
     * @return Byte array from message
     * @throws IOException
     */
    static public byte[] serialize(Serializable data) {
        byte[] buf = null;

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(data);
            oos.flush();
            oos.close(); // not sure
            buf = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return buf;
    }

    /**
     * Deserialize object from byte array
     * 
     * @param buff buffer to be serialized
     * @return desiarialized object
     * @throws IOException
     */
    static public Object deserialize(byte[] buff) {
        Object out = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(buff);
            ObjectInputStream ois;
            ois = new ObjectInputStream(bais);
            out = ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return out;
    }
}
