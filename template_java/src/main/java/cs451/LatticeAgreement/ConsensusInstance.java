package cs451.LatticeAgreement;

import cs451.Parser.Host;
import cs451.LatticeAgreement.ConsensusTypes.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class ConsensusInstance<T> {
    private final int consensusNumber;
    private final ConsensusManager<T> m;

    private boolean active = false;
    private int ackCount = 0;
    private int nackCount = 0;
    int activeProposalNumber = 0;
    Set<T> proposedValue;
    Set<T> lastBroadcastedProposal;
    Set<T> acceptedValue;

    private final T[] acceptedValueArr;
    private int acceptedValueLength = 0;

    private int decidedCount = 0;
    private int[] latestProposalNumber;

    @SuppressWarnings("unchecked")
    public ConsensusInstance(int consensusNumber, ConsensusManager<T> manager) {
        this.consensusNumber = consensusNumber;
        proposedValue = new HashSet<>();
        acceptedValue = new HashSet<>();

        this.acceptedValueArr = (T[]) new Object[manager.ds];

        // latest proposal number for each host
        latestProposalNumber = new int[manager.hosts.size()];

        this.m = manager;
    }

    public void propose(Collection<T> proposal) {
        synchronized (this) {
            proposedValue.addAll(proposal);
            active = true;
            activeProposalNumber++;

            proposedValue.addAll(acceptedValue);

            broadcastProposal(proposedValue);
        }
    }

    @SuppressWarnings("unchecked")
    public void handlePackage(ConsensusPackage message, int senderId) {
        synchronized (this) {
            if (message instanceof Ack) {
                handleAck((Ack) message);
            } else if (message instanceof Nack) {
                handleNack((Nack<T>) message);
            } else if (message instanceof Proposal) {
                handleProposal((Proposal<T>) message, senderId);
            } else if (message instanceof Decided) {
                handleDecided((Decided) message);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public boolean canCancelMessage(ConsensusPackage message, int toId) {
        synchronized (this) {
            if (message instanceof Proposal) {
                Proposal<T> proposal = (Proposal<T>) message;
                return proposal.proposalNumber < activeProposalNumber;
            } else if (message instanceof Ack) {
                Ack ack = (Ack) message;
                return ack.proposalNumber != latestProposalNumber[toId - 1];
            } else if (message instanceof Nack) {
                Nack<T> nack = (Nack<T>) message;
                return nack.proposalNumber != latestProposalNumber[toId - 1];
            } else {
                return false;
            }
        }
    }

    public boolean canDie() {
        synchronized (this) {
            return !active && decidedCount >= m.hosts.size();
        }
    }

    private void handleProposal(Proposal<T> proposal, int senderId) {
        if (proposal.proposalNumber >= latestProposalNumber[senderId - 1]) {
            latestProposalNumber[senderId - 1] = proposal.proposalNumber;
        } else {
            return;
        }
        // if acceptedValue is a subset of proposal.values, send ack else send nack
        if (proposal.values.containsAll(acceptedValue)) {
            acceptedValue = proposal.values;
            Ack ack = new Ack(consensusNumber, proposal.proposalNumber);
            m.perfectLink.send(ack, m.hosts.get(senderId - 1));
        } else {
            addAllToAccepted(proposal.values);
            OutgoingNack<T> nack = new OutgoingNack<>(consensusNumber, proposal.proposalNumber, acceptedValueArr,
                    acceptedValueLength);
            m.perfectLink.send(nack, m.hosts.get(senderId - 1));
        }
    }

    private void handleAck(Ack ack) {
        if (active && ack.proposalNumber == activeProposalNumber) {
            ackCount++;

            ackLogic();
        }
    }

    private void handleNack(Nack<T> nack) {
        if (active && nack.proposalNumber == activeProposalNumber) {
            nackCount++;
            proposedValue.addAll(nack.values);

            ackLogic();
        }
    }

    private void handleDecided(Decided message) {
        decidedCount++;
    }

    private void ackLogic() {
        if (nackCount > 0 && ackCount + nackCount > m.hosts.size() / 2) {
            activeProposalNumber++;

            ackCount = 0;
            nackCount = 0;

            broadcastProposal(proposedValue);
        } else if (ackCount > m.hosts.size() / 2) {
            // decide.accept(proposedValue);
            m.onDecide(lastBroadcastedProposal, consensusNumber);
            active = false;
            // broadcast decided
            Decided decided = new Decided(consensusNumber);
            for (Host host : m.hosts) {
                if (host.getId() != m.myId) {
                    m.perfectLink.send(decided, host);
                }
            }
            decidedCount++;
        }
    }

    private void broadcastProposal(Set<T> proposal) {
        Set<T> copyOfProposal = Set.copyOf(proposal);
        lastBroadcastedProposal = copyOfProposal;
        Proposal<T> proposalPackage = new Proposal<>(consensusNumber, activeProposalNumber, copyOfProposal);
        // OutgoingProposal<T> proposalPackage = new OutgoingProposal<>(consensusNumber,
        // activeProposalNumber, acceptedValueArr, acceptedValueLength);

        addAllToAccepted(proposal);

        // if acceptedValue is a subset of proposal, count as ack otherwise count as
        // nack and add difference to proposedValue
        if (proposal.containsAll(acceptedValue)) {
            ackCount++;
        } else {
            nackCount++;
            proposedValue.addAll(acceptedValue);
        }

        for (Host host : m.hosts) {
            if (host.getId() != m.myId) {
                m.perfectLink.send(proposalPackage, host);
            }
        }
    }

    private void addAllToAccepted(Set<T> values) {
        for (T value : values) {
            if (!acceptedValue.contains(value)) {
                acceptedValue.add(value);
                acceptedValueArr[acceptedValueLength] = value;
                acceptedValueLength++;
            }
        }
    }
}
