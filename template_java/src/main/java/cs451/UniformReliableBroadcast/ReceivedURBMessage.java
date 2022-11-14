package cs451.UniformReliableBroadcast;

import java.util.Objects;

public class ReceivedURBMessage<T> implements Comparable<ReceivedURBMessage<T>> {
    public final int from;
    public final int n;
    public final T message;

    public ReceivedURBMessage(int from, int n, T message) {
        this.from = from;
        this.n = n;
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReceivedURBMessage<?> that = (ReceivedURBMessage<?>) o;
        return from == that.from && n == that.n;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, n);
    }

    @Override
    public int compareTo(ReceivedURBMessage<T> o) {
        if (this.from == o.from) {
            return Integer.compare(this.n, o.n);
        } else {
            return Integer.compare(this.from, o.from);
        }
    }
}
