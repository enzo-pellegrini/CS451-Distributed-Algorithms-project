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
    private int decidedCount = 0;
    private int[] latestProposalNumber;

    public ConsensusInstance(int consensusNumber, ConsensusManager<T> manager) {
        this.consensusNumber = consensusNumber;
        proposedValue = new HashSet<>();
        acceptedValue = new HashSet<>();

        // latest proposal number for each host
        latestProposalNumber = new int[manager.hosts.size()];

        this.m = manager;
    }

    public void propose(Collection<T> proposal) {
        synchronized (this) {
            proposedValue.addAll(proposal);
            active = true;
            activeProposalNumber++;

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
            if (!active && decidedCount >= m.hosts.size()) {
                // System.out.println("gc consensus " + consensusNumber);
                return true;
            }
            return false;
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
            acceptedValue.addAll(proposal.values);
            Set<T> toSend = new HashSet<>(acceptedValue);
            toSend.removeAll(proposal.values);
            Nack<T> nack = new Nack<>(consensusNumber, proposal.proposalNumber, toSend);
            m.perfectLink.send(nack, m.hosts.get(senderId - 1));
        }
    }

    private void handleAck(Ack ack) {
        if (ack.proposalNumber == activeProposalNumber) {
            ackCount++;

            ackLogic(true);
        }
    }

    private void handleNack(Nack<T> nack) {
        if (nack.proposalNumber == activeProposalNumber) {
            nackCount++;
            proposedValue.addAll(nack.values);

            ackLogic(false);
        }
    }

    private void handleDecided(Decided message) {
        decidedCount++;
    }

    private void ackLogic(boolean isAck) {
        if (active) {
            if (isAck && ackCount > m.hosts.size() / 2) {
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
            } else if (ackCount + nackCount > m.hosts.size() / 2) {
                activeProposalNumber++;

                ackCount = 0;
                nackCount = 0;

                broadcastProposal(proposedValue);
            }
        }
    }

    private void broadcastProposal(Set<T> proposal) {
        Set<T> copyOfProposal = Set.copyOf(proposal);
        lastBroadcastedProposal = copyOfProposal;
        Proposal<T> proposalPackage = new Proposal<>(consensusNumber, activeProposalNumber, copyOfProposal);

        // if acceptedValue is a subset of proposal, count as ack otherwise count as
        // nack and add difference to proposedValue
        if (proposal.containsAll(acceptedValue)) {
            acceptedValue.addAll(proposal);
            ackCount++;
        } else {
            acceptedValue.addAll(proposal);
            nackCount++;
            proposedValue.addAll(acceptedValue);
        }

        for (Host host : m.hosts) {
            if (host.getId() != m.myId) {
                m.perfectLink.send(proposalPackage, host);
            }
        }
    }
}
