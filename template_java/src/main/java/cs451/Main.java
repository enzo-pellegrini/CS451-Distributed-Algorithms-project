package cs451;

import cs451.LatticeAgreement.ConsensusManager;
import cs451.Parser.Host;
import cs451.Parser.LatticeParser;
import cs451.Printer.LatticeOutputWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class Main {
    static LatticeOutputWriter outputWriter;
    static ConsensusManager<Integer> consensusManager;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        consensusManager.interruptAll();

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
        LatticeParser parser = new LatticeParser(args);
        parser.parse();

        initSignalHandlers();

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
            outputWriter = new LatticeOutputWriter(parser.output());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        System.out.println("Starting " + parser.getP() + " proposals...\n");

        consensusManager = new ConsensusManager<>(parser.myId(), parser.hosts().get(parser.myId() - 1).getPort(),
                parser.hosts(), outputWriter::decided, (message, bb) -> bb.putInt(message), ByteBuffer::getInt, Integer.BYTES,
                parser.getP(), parser.getVs(), parser.getDs());
        consensusManager.startThreads();

        List<Integer> proposal;
        while ((proposal = parser.getNextProposal()) != null) {
            consensusManager.propose(proposal);
        }

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
