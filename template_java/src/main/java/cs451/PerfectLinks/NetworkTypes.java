package cs451.PerfectLinks;

import cs451.Parser.Host;

import java.util.List;

/**
 * Types that get sent through UDP (serializable)
 */
public class NetworkTypes {
    static abstract class NetworkPacket {}
    static class DataPacket<T> extends NetworkPacket {
        public final int n;
        public final int from;
        public final List<T> data;

        public DataPacket(int n, int from, List<T> data) {
            this.n = n;
            this.from = from;
            this.data = data;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + n;
            result = prime * result + from;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DataPacket other = (DataPacket<T>) obj;
            if (n != other.n)
                return false;
            if (from != other.from)
                return false;
            return true;
        }
    }

    static class AckPacket extends NetworkPacket {
        public final int n;
        public final int receiver_id;

        public AckPacket(int n, int receiver_id) {
            this.n = n;
            this.receiver_id = receiver_id;
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

    static class Sendable<T> {
        public final int n;
        public final Host to;
        public int tryCount = 0;
        public DataPacket<T> message;

        private byte[] buff;

        public Sendable(DataPacket<T> message, Host to) {
            this.n = message.n;
            this.message = message;
            this.to = to;
        }
    }

    public static class ReceivedMessage<T> { // TODO: do messages have to be numbered (per project specification)?
        public final T data;
        public final int from;

        public ReceivedMessage(T data, int from) {
            this.data = data;
            this.from = from;
        }
    }
}
