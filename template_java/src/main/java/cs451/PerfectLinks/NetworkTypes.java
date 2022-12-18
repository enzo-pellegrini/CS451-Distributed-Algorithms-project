package cs451.PerfectLinks;

import java.util.List;

/**
 * Types that get sent through UDP (serializable)
 */
public class NetworkTypes {
    static abstract class NetworkPacket {
        public abstract int getN();
    }
    static class DataPacket<T> extends NetworkPacket {
        public int n;
        public int from;
        public List<T> data;

        @Override
        public int getN() {
            return n;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + n;
            result = prime * result + from;
            return result;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DataPacket other = (DataPacket) obj;
            if (n != other.n)
                return false;
            if (from != other.from)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "DataPacket{" +
                    "n=" + n +
                    ", from=" + from +
                    ", data=" + data +
                    '}';
        }
    }

    static class AckPacket extends NetworkPacket {
        public int n;
        public int receiver_id;

        public AckPacket() {
        }

        public AckPacket(int n, int receiver_id) {
            this.n = n;
            this.receiver_id = receiver_id;
        }

        @Override
        public int getN() {
            return n;
        }
    }

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

    public static class ReceivedMessage<T> {
        public final T data;
        public final int from;

        public ReceivedMessage(T data, int from) {
            this.data = data;
            this.from = from;
        }
    }
}
