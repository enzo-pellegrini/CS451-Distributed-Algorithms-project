package cs451.PerfectLinks;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import cs451.Parser.Host;
import cs451.PerfectLinks.NetworkTypes.AckPacket;
import cs451.PerfectLinks.NetworkTypes.DataPacket;

public class DirectedSender<T> {
    static private final int MAX_IN_FLIGHT = 10;

    private final PerfectLink<T> parent;
    final int dstId;
    private final Queue<T> queue = new LinkedList<>();

    private int seqNum = 0;
    private Map<Integer, List<T>> inFlight = new HashMap<>();

    int resendPause = 200;

    public DirectedSender(PerfectLink<T> parent, int dstId) {
        this.parent = parent;
        this.dstId = dstId;
    }

    public void schedule(T message) {
        synchronized (this) {
            queue.add(message);
        }
    }

    public int send(DatagramSocket sock) {
        // resent all packages in sent,
        // then send packages from messages in queue, by putting at most 8 messages in
        // each package,
        // and sending at most MAX_IN_FLIGHT packages
        // return the number of packages sent from the queue (not resent), or 10 if the
        // queue is empty
        synchronized (this) {
            int newPackagesSent = 0;
            while (inFlight.size() < MAX_IN_FLIGHT && !queue.isEmpty()) {
                List<T> messages = new ArrayList<>();
                while (!queue.isEmpty() && messages.size() < 8) {
                    var m = queue.poll();
                    // if (parent.isCanceled.apply(m, dstId)) {
                    // System.out.println("Dropping message " + m);
                    // continue;
                    // }
                    messages.add(m);
                }
                int n = this.seqNum++;
                inFlight.put(n, messages);
                newPackagesSent++;
            }

            boolean hadError = false;
            // iterate over entries in sent
            for (Map.Entry<Integer, List<T>> entry : inFlight.entrySet()) {
                // parent.sendPackage(sock, new Package<>(entry.getValue().messages,
                // entry.getValue().seqNum), dstId);
                if (!sendPackage(sock, entry.getKey(), entry.getValue())) {
                    hadError = true;
                    break;
                }
            }
            if (hadError) {
                return 0;
            }

            // System.out.println("[to " + dstId + "] inFlight.size() = " + inFlight.size()
            // + ", queue.size() = " + queue.size());

            // if the queue is empty, we want to go as fast as possible, so we return 10
            return queue.isEmpty() ? MAX_IN_FLIGHT : newPackagesSent;
        }
    }

    public void handleAck(AckPacket ack) {
        synchronized (this) {
            inFlight.remove(ack.n);
            // System.out.println("Received ack for " + ack.n + " from " + ack.receiver_id);
        }
    }

    private boolean sendPackage(DatagramSocket sock, int n, List<T> messages) {
        // TODO: reuse the same datagram packet for all messages
        DataPacket<T> p = new DataPacket<>(n, parent.myId, messages);
        byte[] buf = parent.serializer.serialize(p);
        Host dst = parent.hosts.get(dstId - 1);
        try {
            DatagramPacket dp = new DatagramPacket(buf, buf.length, InetAddress.getByName(dst.getIp()), dst.getPort());
            sock.send(dp);
        } catch (SocketException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}