package cs451.LatticeAgreement;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ConsensusTypes {
    interface ConsensusPackage {
        int getConsensusNumber();
    }

    static class Ack implements ConsensusPackage {
        final int consensusNumber;
        final int proposalNumber;

        @Override
        public String toString() {
            return "Ack{" +
                    "consensusNumber=" + consensusNumber +
                    ", proposalNumber=" + proposalNumber +
                    '}';
        }

        public Ack(int consensusNumber, int proposalNumber) {
            this.consensusNumber = consensusNumber;
            this.proposalNumber = proposalNumber;
        }

        @Override
        public int getConsensusNumber() {
            return consensusNumber;
        }
    }

    static class Nack<T> implements ConsensusPackage {
        final int consensusNumber;
        final int proposalNumber;
        final Set<T> values;

        @Override
        public String toString() {
            return "Nack{" +
                    "consensusNumber=" + consensusNumber +
                    ", proposalNumber=" + proposalNumber +
                    ", values=" + values +
                    '}';
        }

        public Nack(int consensusNumber, int proposalNumber, Set<T> values) {
            this.consensusNumber = consensusNumber;
            this.proposalNumber = proposalNumber;
            this.values = values;
        }

        @Override
        public int getConsensusNumber() {
            return consensusNumber;
        }
    }

    static class Proposal<T> implements ConsensusPackage {
        final int consensusNumber;
        final int proposalNumber;
        final Set<T> values;

        @Override
        public String toString() {
            return "Proposal{" +
                    "consensusNumber=" + consensusNumber +
                    ", proposalNumber=" + proposalNumber +
                    ", values=" + values +
                    '}';
        }

        public Proposal(int consensusNumber, int proposalNumber, Set<T> values) {
            this.consensusNumber = consensusNumber;
            this.proposalNumber = proposalNumber;
            this.values = values;
        }

        @Override
        public int getConsensusNumber() {
            return consensusNumber;
        }
    }

    static class Decided implements ConsensusPackage {
        final int consensusNumber;

        public Decided(int consensusNumber) {
            this.consensusNumber = consensusNumber;
        }

        @Override
        public String toString() {
            return "Decided{" +
                    "consensusNumber=" + consensusNumber +
                    '}';
        }

        @Override
        public int getConsensusNumber() {
            return consensusNumber;
        }
    }

    static class Serializer<T> {
        private final BiConsumer<T, ByteBuffer> messageSerializer;
        private final Function<ByteBuffer, T> messageDeserializer;

        public Serializer(BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer) {
            this.messageSerializer = messageSerializer;
            this.messageDeserializer = messageDeserializer;
        }

        private void serializeSet(Set<T> set, ByteBuffer buffer) {
            buffer.putInt(set.size());
            for (T value : set) {
                messageSerializer.accept(value, buffer);
            }
        }

        private Set<T> deserializeSet(ByteBuffer buffer) {
            int size = buffer.getInt();
            Set<T> set = new HashSet<>();
            for (int i = 0; i < size; i++) {
                set.add(messageDeserializer.apply(buffer));
            }
            return set;
        }

        private void serializeAck(Ack ack, ByteBuffer buffer) {
            buffer.put((byte) 0);
            buffer.putInt(ack.consensusNumber);
            buffer.putInt(ack.proposalNumber);
        }

        private Ack deserializeAck(ByteBuffer buffer) {
            int consensusNumber = buffer.getInt();
            int proposalNumber = buffer.getInt();
            return new Ack(consensusNumber, proposalNumber);
        }

        private void serializeNack(Nack<T> nack, ByteBuffer buffer) {
            buffer.put((byte) 1);
            buffer.putInt(nack.consensusNumber);
            buffer.putInt(nack.proposalNumber);
            serializeSet(nack.values, buffer);
        }

        private Nack<T> deserializeNack(ByteBuffer buffer) {
            int consensusNumber = buffer.getInt();
            int proposalNumber = buffer.getInt();
            Set<T> values = deserializeSet(buffer);
            return new Nack<>(consensusNumber, proposalNumber, values);
        }

        private void serializeProposal(Proposal<T> proposal, ByteBuffer buffer) {
            buffer.put((byte) 2);
            buffer.putInt(proposal.consensusNumber);
            buffer.putInt(proposal.proposalNumber);
            serializeSet(proposal.values, buffer);
        }

        private Proposal<T> deserializeProposal(ByteBuffer buffer) {
            int consensusNumber = buffer.getInt();
            int proposalNumber = buffer.getInt();
            Set<T> values = deserializeSet(buffer);
            return new Proposal<>(consensusNumber, proposalNumber, values);
        }

        private void serializeDecided(Decided decided, ByteBuffer buffer) {
            buffer.put((byte) 3);
            buffer.putInt(decided.consensusNumber);
        }

        private Decided deserializeDecided(ByteBuffer buffer) {
            int consensusNumber = buffer.getInt();
            return new Decided(consensusNumber);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public void serialize(ConsensusPackage message, ByteBuffer buffer) {
            if (message instanceof Ack) {
                serializeAck((Ack) message, buffer);
            } else if (message instanceof Nack) {
                serializeNack((Nack) message, buffer);
            } else if (message instanceof Proposal) {
                serializeProposal((Proposal) message, buffer);
            } else if (message instanceof Decided) {
                serializeDecided((Decided) message, buffer);
            } else {
                throw new IllegalArgumentException("Unknown message type");
            }
        }

        public ConsensusPackage deserialize(ByteBuffer buffer) {
            byte type = buffer.get();
            if (type == 0) {
                return deserializeAck(buffer);
            } else if (type == 1) {
                return deserializeNack(buffer);
            } else if (type == 2) {
                return deserializeProposal(buffer);
            } else if (type == 3) {
                return deserializeDecided(buffer);
            } else {
                throw new IllegalArgumentException("Unknown type: " + type);
            }
        }
    }
}
