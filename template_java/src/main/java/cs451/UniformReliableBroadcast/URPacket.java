package cs451.UniformReliableBroadcast;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

class URPacket<T> {
    public final int n;
    public final int from;
    public final T message;

    public URPacket(int n, int from, T message) {
        this.n = n;
        this.from = from;
        this.message = message;
    }

    public static <T> URPacket<T> deserialize(ByteBuffer bb, Function<ByteBuffer, T> deserializer) {
        int n = bb.getInt();
        int from = (bb.get() & 0xff) + 1;
        T message = deserializer.apply(bb);
        return new URPacket<>(n, from, message);
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
        URPacket<?> urPacket = (URPacket<?>) o;
        return n == urPacket.n && from == urPacket.from;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, from);
    }
}
