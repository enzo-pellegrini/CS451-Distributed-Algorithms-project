package cs451.PerfectLinks;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
    private final int dstId;
    private final Queue<T> queue = new LinkedList<>();

    private int seqNum = 0;
    private Map<Integer, List<T>> sent = new HashMap<>();

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
        // then send packages from messages in queue, by putting at most 8 messages in each package, 
        // and sending at most MAX_IN_FLIGHT packages
        // return the number of packages sent from the queue (not resent), or 10 if the queue is empty
        synchronized (this) {
            int newPackagesSent = 0;
            while (sent.size() < MAX_IN_FLIGHT && !queue.isEmpty()) {
                List<T> messages = new ArrayList<>();
                while (!queue.isEmpty() && messages.size() < 8) {
                    messages.add(queue.poll());
                }
                int n = this.seqNum++;
                // parent.sendPackage(sock, new Package<>(messages, seqNum), dstId);
                sent.put(n, messages);
                newPackagesSent++;
            }

            // iterate over entries in sent
            for (Map.Entry<Integer, List<T>> entry : sent.entrySet()) {
                // parent.sendPackage(sock, new Package<>(entry.getValue().messages, entry.getValue().seqNum), dstId);
                sendPackage(sock, entry.getKey(), entry.getValue());
            }

            // if the queue is empty, we want to go as fast as possible, so we return 10
            return queue.isEmpty() ? MAX_IN_FLIGHT : newPackagesSent;
        }
    }

    public void handleAck(AckPacket ack) {
        synchronized (this) {
            sent.remove(ack.n);
        }
    }

    private void sendPackage(DatagramSocket sock, int n, List<T> messages) {
        DataPacket<T> p = new DataPacket<>(n, parent.myId, messages);
        byte[] buf = parent.serializer.serialize(p);
        Host dst = parent.hosts.get(dstId - 1);
        try {
            // TODO: object pool for DatagramPacket
            DatagramPacket dp = new DatagramPacket(buf, buf.length, InetAddress.getByName(dst.getIp()), dst.getPort());
            sock.send(dp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}