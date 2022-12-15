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

    private int handlingNow = 0;
    private final Lock handlingNowLock = new ReentrantLock();
    private final Condition canHandleMore = handlingNowLock.newCondition();

    private final Lock shotLock = new ReentrantLock();
    private int consensusNumber = 0;
    private final Map<Integer, ConsensusInstance<T>> shots = new HashMap<>();
    private final Map<Integer, Queue<NetworkTypes.ReceivedMessage<ConsensusPackage>>> pendingMessages = new HashMap<>();
    private final PriorityQueue<Decision<T>> decisions = new PriorityQueue<>(Comparator.comparingInt(d -> d.consensusNumber));
    private int lastDecided = -1;

    public ConsensusManager(int myId, int port, List<Host> hosts, Consumer<Set<T>> decide,
                            BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer,
                            int messageSize, int p, int vs, int ds) {
        this.hosts = hosts;
        this.decide = decide;
        Serializer<T> serializer = new Serializer<>(messageSerializer, messageDeserializer);
        this.perfectLink = new PerfectLink<>(myId, port, hosts,
                this::onDeliver, serializer::serialize, serializer::deserialize,
                messageSize * ds + Integer.BYTES * 3 + 1);
        this.myId = myId;
        this.MAX_HANDLING = Math.max(1, 1000 / hosts.size());
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

        shotLock.lock();

        try {
            int consensusN = this.consensusNumber++;
            ConsensusInstance<T> instance = new ConsensusInstance<>(consensusN, this);
            shots.put(consensusN, instance);
            instance.propose(proposal);

            // check if there are pending messages for this consensus instance
            Queue<NetworkTypes.ReceivedMessage<ConsensusPackage>> pending = pendingMessages.get(consensusN);
            if (pending != null) {
                while (!pending.isEmpty()) {
                    NetworkTypes.ReceivedMessage<ConsensusPackage> message = pending.poll();
                    instance.handlePackage(message.data, message.from);
                }
            }
            pendingMessages.remove(consensusN);

            if (instance.canDie()) {
                shots.remove(consensusN);
            }
        } finally {
            shotLock.unlock();
        }
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
        shotLock.lock();

        try {
            if (shots.containsKey(consensusN)) {
                shots.get(consensusN).handlePackage(receivedMessage.data, receivedMessage.from);

                if (shots.get(consensusN).canDie()) {
                    shots.remove(consensusN);
                }
            } else if (consensusN >= consensusNumber) {
                // add to pending messages
                pendingMessages.putIfAbsent(consensusN, new ArrayDeque<>());
                pendingMessages.get(consensusN).add(receivedMessage);
            }
        } finally {
            shotLock.unlock();
        }
    }
}
