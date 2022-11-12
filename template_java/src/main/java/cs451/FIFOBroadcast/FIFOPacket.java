package cs451.FIFOBroadcast;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

class FIFOPacket<T> implements Comparable<T> {
    public final int n;
    public final int from;
    public final T message;

    public FIFOPacket(int n, int from, T message) {
        this.n = n;
        this.from = from;
        this.message = message;
    }

    public static <T> FIFOPacket<T> deserialize(ByteBuffer bb, Function<ByteBuffer, T> deserializer) {
        int n = bb.getInt();
        int from = (bb.get() & 0xff) + 1;
        T message = deserializer.apply(bb);
        return new FIFOPacket<>(n, from, message);
    }

    public void serialize(ByteBuffer bb, BiConsumer<T, ByteBuffer> serializer) {
        bb.putInt(n);
        bb.put((byte) (from - 1));
        serializer.accept(message, bb);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FIFOPacket<?> that = (FIFOPacket<?>) o;
        return n == that.n && from == that.from;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, from);
    }

    @Override
    public int compareTo(T o) {
        FIFOPacket<?> that = (FIFOPacket<?>) o;
        if (this.from == that.from) {
            return this.n - that.n;
        }
        return this.from - that.from;
    }
}
