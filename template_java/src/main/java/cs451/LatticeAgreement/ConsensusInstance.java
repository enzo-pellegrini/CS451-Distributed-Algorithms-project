package cs451.LatticeAgreement;

import cs451.Parser.Host;
import cs451.PerfectLinks.PerfectLink;
import cs451.LatticeAgreement.ConsensusTypes.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

class ConsensusInstance<T> {
    private final int consensusNumber;
    private final Consumer<Set<T>> decide;
    private final PerfectLink<ConsensusPackage> perfectLink;
    private final List<Host> hosts;
    private final int myId;

    private boolean active = false;
    private int ackCount = 0;
    private int nackCount = 0;
    int activeProposalNumber = 0;
    Set<T> proposedValue;
    Set<T> acceptedValue;
    private int decidedCount = 0;

    public ConsensusInstance(int consensusNumber, Consumer<Set<T>> decide, PerfectLink<ConsensusPackage> perfectLink, List<Host> hosts, int myId) {
        this.consensusNumber = consensusNumber;
        this.decide = decide;
        proposedValue = new HashSet<>();
        acceptedValue = new HashSet<>();
        this.perfectLink = perfectLink;
        this.hosts = hosts;
        this.myId = myId;
    }

    public void propose(Collection<T> proposal) {
        proposedValue.addAll(proposal);
        active = true;
        activeProposalNumber++;

        broadcastProposal(proposedValue);
    }

    @SuppressWarnings("unchecked")
    public void handlePackage(ConsensusPackage message, int senderId) {
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

    public boolean canDie() {
        return !active && decidedCount >= hosts.size();
    }


    private void handleProposal(Proposal<T> proposal, int senderId) {
        // if acceptedValue is a subset of proposal.values, send ack else send nack
        if (proposal.values.containsAll(acceptedValue)) {
            acceptedValue = proposal.values;
            Ack ack = new Ack(consensusNumber, proposal.proposalNumber);
            perfectLink.send(ack, hosts.get(senderId-1));
        } else {
            acceptedValue.addAll(proposal.values);
            Set<T> difference = new HashSet<>(acceptedValue);
            difference.removeAll(proposal.values);
            Nack<T> nack = new Nack<>(consensusNumber, proposal.proposalNumber, difference);
            perfectLink.send(nack, hosts.get(senderId-1));
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
            if (isAck && ackCount > hosts.size() / 2) {
                decide.accept(proposedValue);
                active = false;
                // broadcast decided
                Decided decided = new Decided(consensusNumber);
                for (Host host : hosts) {
                    if (host.getId() != myId) {
                        perfectLink.send(decided, host);
                    }
                }
                decidedCount++;
            } else if (ackCount + nackCount > hosts.size() / 2) {
                activeProposalNumber++;

                ackCount = 0;
                nackCount = 0;

                broadcastProposal(proposedValue);
            }
        }
    }

    private void broadcastProposal(Set<T> proposal) {
        Set<T> copyOfProposal = new HashSet<>(proposal);
        Proposal<T> proposalPackage = new Proposal<>(consensusNumber, activeProposalNumber, copyOfProposal);

        // if acceptedValue is a subset of proposal, count as ack otherwise count as nack and add difference to proposedValue
        if (acceptedValue.containsAll(proposal)) {
            ackCount++;
        } else {
            nackCount++;
            proposedValue.addAll(acceptedValue);
        }

        for (Host host : hosts) {
            if (host.getId() != myId) {
                perfectLink.send(proposalPackage, host);
            }
        }
    }
}
