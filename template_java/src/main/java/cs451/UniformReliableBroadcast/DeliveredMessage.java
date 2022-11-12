package cs451.UniformReliableBroadcast;

import java.util.Objects;

class DeliveredMessage {
    public int from;
    public int n;

    public DeliveredMessage(int from, int n) {
        this.from = from;
        this.n = n;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeliveredMessage that = (DeliveredMessage) o;
        return from == that.from && n == that.n;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, n);
    }
}
