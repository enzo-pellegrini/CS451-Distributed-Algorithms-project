package cs451.LatticeAgreement;

import cs451.LatticeAgreement.ConsensusTypes.*;
import cs451.Parser.Host;
import cs451.PerfectLinks.NetworkTypes;
import cs451.PerfectLinks.PerfectLink;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class ConsensusManager<T> {
    final int MAX_HANDLING;

    // for child consensus instances
    final PerfectLink<ConsensusPackage> perfectLink;
    final int myId;
    final List<Host> hosts;
    final Consumer<Set<T>> decide;
    final int vs;
    final int ds;

    private final Lock handlingNowLock = new ReentrantLock();
    private final Condition canHandleMore = handlingNowLock.newCondition();

    private int nextConsensusNumber = 0;
    private final Map<Integer, ConsensusInstance<T>> shots = new HashMap<>();
    private final PriorityQueue<Decision<T>> decisions = new PriorityQueue<>(
            Comparator.comparingInt(d -> d.consensusNumber));
    private int lastDecided = -1;

    public ConsensusManager(int myId, int port, List<Host> hosts, Consumer<Set<T>> decide,
            BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer,
            int messageSize, int p, int vs, int ds) {
        this.hosts = hosts;
        this.decide = decide;
        Serializer<T> serializer = new Serializer<>(messageSerializer, messageDeserializer);
        this.perfectLink = new PerfectLink<>(myId, port, hosts,
                this::onDeliver, this::canCancel,
                serializer::serialize,
                serializer::deserialize,
                messageSize * ds + Integer.BYTES * 3 + 1);
        this.myId = myId;
        this.MAX_HANDLING = Math.min(100, Math.max(8, 1000 / (int) (Math.pow(hosts.size(), 2))));
        this.vs = vs;
        this.ds = ds;
        System.out.println("MAX HANDLING: " + MAX_HANDLING);
    }

    public void startThreads() {
        perfectLink.startThreads();
    }

    public void interruptAll() {
        perfectLink.interruptAll();
    }

    public void propose(Collection<T> proposal) {
        handlingNowLock.lock();

        ConsensusInstance<T> instance;
        try {
            while (nextConsensusNumber - lastDecided > MAX_HANDLING) {
                try {
                    canHandleMore.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            int consensusN = this.nextConsensusNumber++;

            // System.out.println("Proposing on consensus number " + consensusN);
            instance = shots.get(consensusN);
            if (instance == null) {
                instance = new ConsensusInstance<>(consensusN, this);
                shots.put(consensusN, instance);
            }
        } finally {
            handlingNowLock.unlock();
        }

        instance.propose(proposal);
    }

    synchronized void onDecide(Set<T> ts, int consensusN) {
        // System.out.println("Decided on consensus number " + consensusN);

        handlingNowLock.lock();
        try {
            decisions.add(new Decision<>(consensusN, ts));

            boolean printedAny = false;
            while (!decisions.isEmpty() && decisions.peek().consensusNumber == lastDecided + 1) {
                var decision = decisions.poll();
                decide.accept(decision.value);
                // System.out.println("Printing decision: " + decision.consensusNumber);
                lastDecided++;
                printedAny = true;
            }

            if (printedAny)
                canHandleMore.signal();
        } finally {
            handlingNowLock.unlock();
        }
    }

    private void onDeliver(NetworkTypes.ReceivedMessage<ConsensusPackage> receivedMessage) {
        int consensusN = receivedMessage.data.getConsensusNumber();

        ConsensusInstance<T> instance;

        handlingNowLock.lock();

        try {
            instance = shots.get(consensusN);
            if (instance == null) {
                if (consensusN >= this.nextConsensusNumber) {
                    instance = new ConsensusInstance<>(consensusN, this);
                    shots.put(consensusN, instance);
                } else {
                    return;
                }
            }
        } finally {
            handlingNowLock.unlock();
        }

        instance.handlePackage(receivedMessage.data, receivedMessage.from);

        if (instance.canDie()) {
            // System.out.println("gc consensus " + consensusN);

            handlingNowLock.lock();

            try {
                shots.remove(consensusN);
            } finally {
                handlingNowLock.unlock();
            }

            if (consensusN % 200 == 0)
                System.gc();
        }
    }

    private boolean canCancel(ConsensusPackage p, int toId) {
        if (p instanceof ConsensusTypes.Decided)
            return false;

        handlingNowLock.lock();

        ConsensusInstance<T> instance;
        try {
            instance = shots.get(p.getConsensusNumber());
        } finally {
            handlingNowLock.unlock();
        }

        if (instance == null)
            return true;
        else
            return instance.canCancelMessage(p, toId);
    }
}
