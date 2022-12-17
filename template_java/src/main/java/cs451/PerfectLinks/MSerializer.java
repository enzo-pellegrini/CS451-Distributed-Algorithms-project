package cs451.PerfectLinks;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class MSerializer<T> {
    static final int BUFFER_SIZE = 65536;
    private final BiConsumer<T, ByteBuffer> messageSerializer;
    private final Function<ByteBuffer, T> messageDeserializer;
    @SuppressWarnings("unused")
    private final int messageSize; // Upper bound on size of single message
    private final byte[] buffer;

    public MSerializer(BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer, int messageSize) {
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
        this.messageSize = messageSize;
        this.buffer = new byte[BUFFER_SIZE];
    }

    public byte[] serialize(NetworkTypes.DataPacket<T> dp) {
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.put((byte) (1));
        bb.putInt(dp.n);
        bb.put((byte) (dp.from - 1));
        bb.put((byte) (dp.data.size()));
        for (T message : dp.data) {
            messageSerializer.accept(message, bb);
        }
        return Arrays.copyOfRange(bb.array(), 0, bb.position());
    }

    public byte[] serialize(NetworkTypes.AckPacket ap) {
        int size = 1 + 4 + 1;
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.put((byte) (0));
        bb.putInt(ap.n);
        bb.put((byte) (ap.receiver_id - 1));
        return bb.array();
    }

    public boolean isDatapacket(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        return bb.get() > 0;
    }

    public NetworkTypes.DataPacket<T> deserializeDataPacket(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        int type = bb.get();
        assert type > 0;

        int n = bb.getInt();
        int from = (bb.get() & 0xFF) + 1;

        int size = bb.get() & 0xFF;
        List<T> messages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            messages.add(messageDeserializer.apply(bb));
        }

        return new NetworkTypes.DataPacket<>(n, from, messages);
    }

    public NetworkTypes.AckPacket deserializeAckPacket(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        int type = bb.get();
        assert type == 0;

        int n = bb.getInt();
        int receiver_id = (bb.get() & 0xFF) + 1;

        return new NetworkTypes.AckPacket(n, receiver_id);
    }
}
