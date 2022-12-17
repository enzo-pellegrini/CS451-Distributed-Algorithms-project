package cs451.PerfectLinks;

import cs451.Parser.Host;

import java.util.List;

/**
 * Types that get sent through UDP (serializable)
 */
public class NetworkTypes {
    interface NetworkPacket {}

    static class DataPacket<T> implements NetworkPacket {
        public int n;
        public int from;
        public List<T> data;

        public void assignFields(int n, int from, List<T> data) {
            this.n = n;
            this.from = from;
            this.data = data;
        }

        @Override
        public String toString() {
            return "DataPacket [n=" + n + ", from=" + from + ", data=" + data + "]";
        }
    }

    static class AckPacket implements NetworkPacket {
        public int n;
        public int receiver_id;

        public AckPacket(int receiverId) {
            this.receiver_id = receiverId;
        }

        public AckPacket() {}

        public void assignSeqN(int n) {
            this.n = n;
        }
        
        public void assignFields(int n, int receiverId) {
            this.n = n;
            this.receiver_id = receiverId;
        }

        @Override
        public String toString() {
            return "AckPacket [n=" + n + ", receiver_id=" + receiver_id + "]";
        }
    }

    // Entry in set to guarantee non-duplication
    static class ReceivedPacket {
        public final int n;
        public final int sender;

        public ReceivedPacket(int n, int sender) {
            this.n = n;
            this.sender = sender;
        }

        public ReceivedPacket(DataPacket<?> dp) {
            this(dp.n, dp.from);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + n;
            result = prime * result + sender;
            return result;
        }

        @SuppressWarnings("ALL")
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ReceivedPacket other = (ReceivedPacket) obj;
            if (n != other.n)
                return false;
            if (sender != other.sender)
                return false;
            return true;
        }
    }

    // Tuple for delivery to upper layers
    public static class ReceivedMessage<T> {
        public final T data;
        public final int from;

        public ReceivedMessage(T data, int from) {
            this.data = data;
            this.from = from;
        }
    }
}
