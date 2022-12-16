package cs451.LatticeAgreement;

import cs451.LatticeAgreement.ConsensusTypes.*;
import cs451.Parser.Host;
import cs451.PerfectLinks.NetworkTypes;
import cs451.PerfectLinks.PerfectLink;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private int handlingNow = 0;
    private final Lock handlingNowLock = new ReentrantLock();
    private final Condition canHandleMore = handlingNowLock.newCondition();

    private final Lock addShotLock = new ReentrantLock();
    private int consensusNumber = 0;
    private final Map<Integer, ConsensusInstance<T>> shots = new ConcurrentHashMap<>();
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
        this.MAX_HANDLING = Math.max(1, 10000 / (int)(Math.pow(hosts.size(), 2)));
    }

    public void startThreads() {
        perfectLink.startThreads();
    }

    public void interruptAll() {
        perfectLink.interruptAll();
    }

    public void propose(Collection<T> proposal) {
        handlingNowLock.lock();

        try {
            while (handlingNow >= MAX_HANDLING) {
                try {
                    canHandleMore.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            handlingNow++;
        } finally {
            handlingNowLock.unlock();
        }

        int consensusN = this.consensusNumber++;
        var instance = shots.get(consensusN);
        if (instance == null) {
            instance = createInstance(consensusN);
        }

        instance.propose(proposal);
    }

    void onDecide(Set<T> ts, int consensusN) {
        decisions.add(new Decision<>(consensusN, ts));
        while (!decisions.isEmpty() && decisions.peek().consensusNumber == lastDecided + 1) {
            var decision = decisions.poll();
            decide.accept(decision.value);
            lastDecided++;
        }

        handlingNowLock.lock();
        try {
            handlingNow--;
            canHandleMore.signal();
        } finally {
            handlingNowLock.unlock();
        }
    }

    private void onDeliver(NetworkTypes.ReceivedMessage<ConsensusPackage> receivedMessage) {
        int consensusN = receivedMessage.data.getConsensusNumber();

        var instance = shots.get(consensusN);
        if (instance == null) {
            if (consensusN > consensusNumber) {
                instance = createInstance(consensusN);
            } else {
                return;
            }
        }

        instance.handlePackage(receivedMessage.data, receivedMessage.from);
    }

    private ConsensusInstance<T> createInstance(int consensusN) {
        ConsensusInstance<T> shot;

        addShotLock.lock();
        try {
            shot = shots.get(consensusN);
            if (shot == null) {
                shot = new ConsensusInstance<>(consensusN, this);
                shots.put(consensusN, shot);
            }
        } finally {
            addShotLock.unlock();
        }

        assert(shot != null);
        return shot;
    }

    private boolean canCancel(ConsensusPackage p, int toId) {
        if (p instanceof ConsensusTypes.Decided)
            return false;
        // handle each case
        var shot = shots.get(p.getConsensusNumber());
        if (shot == null)
            return true;
        else
            return shot.canCancelMessage(p, toId);
    }
}
