package cs451;

import cs451.Parser.FIFOParser;
import cs451.Parser.Host;
import cs451.Printer.OutputWriter;
import cs451.UniformReliableBroadcast.UniformReliableBroadcast;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

public class Main {
    static OutputWriter outputWriter;

    static Instant start;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
//        rc.interruptAll();

        //write/flush output file if necessary
        System.out.println("Writing output.");
        try {
            outputWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread(Main::handleSignal));
    }

    public static void main(String[] args) throws InterruptedException {
        start = Instant.now();

        FIFOParser parser = new FIFOParser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host : parser.hosts()) {
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

//        rc = new PerfectLink<>(parser.myId(), parser.hosts().get(parser.myId() - 1).getPort(), parser.hosts(),
//                packet -> outputWriter.delivered(packet.from, packet.data),
//                (message, bb) -> bb.putInt(message),
//                ByteBuffer::getInt,
//                4
//        );

        System.out.println("Broadcasting " + parser.numMessages() + " messages and delivering messages...\n");


//        int messageNumber = 0;
//        for (PerfectLinksConfigParser.ConfigEntry ce : parser.configEntries()) {
//            if (parser.myId() != ce.getDstProcess()) {
//                for (int i = 0; i < ce.getNumMessages(); i++) {
//                    outputWriter.broadcasted(++messageNumber);
//                    rc.send(messageNumber, parser.hosts().get(ce.getDstProcess() - 1));
//                }
//            }
//        }
//        System.out.println("I finished sending?");
//        rc.flushMessageBuffers();

        UniformReliableBroadcast<Integer> urb =
                new UniformReliableBroadcast<>(parser.myId(), parser.hosts().get(parser.myId() - 1).getPort(), parser.hosts(),
//                        packet -> System.out.println("Delivered " + packet.message + " from " + packet.from),
                        packet -> outputWriter.delivered(packet.from, packet.message),
                        (message, bb) -> bb.putInt(message),
                        ByteBuffer::getInt,
                        Integer.BYTES);

        for (int i=0; i<parser.numMessages(); i++) {
            urb.broadcast(i+1);
//            System.out.println("Broadcasted " + (i+1));
            outputWriter.broadcasted(i+1);
        }
        urb.flushBuffers();
        System.out.println("Done sending?");

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        //noinspection InfiniteLoopStatement
        while (true) {
            // Sleep for 1 hour
            //noinspection BusyWait
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
