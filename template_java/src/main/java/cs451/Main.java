package cs451;

import cs451.Parser.Host;
import cs451.Parser.PerfectLinksConfigParser;
import cs451.Parser.PerfectLinksParser;
import cs451.PerfectLinks.PerfectLink;
import cs451.Printer.OutputWriter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    static PerfectLink<Integer> rc;
    static OutputWriter outputWriter;
    static final AtomicInteger rcvCount = new AtomicInteger(0);
    static final ConcurrentLinkedQueue<Integer> received = new ConcurrentLinkedQueue<>();

    static Instant start;

    private static void handleSignal() {
        var frozen = new ArrayList<>(received);
        int pending = rc.getPendingBastards();
        var stats = frozen.stream().mapToInt(x->x).summaryStatistics();

        var dups = frozen.size() - frozen.stream().distinct().count();
        Duration elapsed = java.time.Duration.between(start, Instant.now());

        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        System.out.println("Received packets" + frozen.size());

        // let's see what I dropped
        System.out.println("Maximum index received " + stats.getMax());
        System.out.println("Minimum index received " + stats.getMin());

        System.out.println("Dups: " + dups);

        System.out.println("Pending packets: " + pending);

        System.out.println("Time since start " + elapsed.getSeconds());
        //write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread(Main::handleSignal));
    }

    public static void main(String[] args) throws InterruptedException {
        start = Instant.now();

        PerfectLinksParser parser = new PerfectLinksParser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");

        try {
            outputWriter = new OutputWriter(parser.output());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

//        rc = new ReliableChannel<>(parser.myId(), parser.hosts().get(parser.myId()-1).getPort(), parser.hosts(), packet -> {
////            System.out.println("Received message " + packet.data + " from host " + packet.from);
//            rcvCount.incrementAndGet();
//            received.add(packet.data);
//        });
        rc = new PerfectLink<>(parser.myId(), parser.hosts().get(parser.myId()-1).getPort(), parser.hosts(), packet -> {
            outputWriter.delivered(packet.from, packet.data);
        });

        System.out.println("Broadcasting and delivering messages...\n");
//        for (int i=0; i<treu; i++) {
////            for (Host h : parser.hosts()) {
//                rc.send(i, me);
////            }
//        }



//        List<Host> hosts = parser.hosts();
//        Thread t = new Thread(() -> {
//            for (int i=0; true; i++) {
////                for (int j=0; j<2; j++) {
//                    rc.send(i, hosts.get(0));
////                }
//            }
//        });
//        t.start();

        int messageNumber = 1;
        for (PerfectLinksConfigParser.ConfigEntry ce : parser.configEntries()) {
            if (parser.myId() != ce.getDstProcess()) {
                for (int i=0; i<ce.getNumMessages(); i++) {
                    rc.send(messageNumber++, parser.hosts().get(ce.getDstProcess()-1));
                    outputWriter.delivered(parser.myId(), messageNumber);
                }
            }
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        // while (true) {
        //     // Sleep for 1 hour
        //     Thread.sleep(60 * 60 * 1000);
        // }

        Thread.sleep(5*1000);
    }
}
