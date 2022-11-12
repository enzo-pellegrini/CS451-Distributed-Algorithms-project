package cs451.PerfectLinks;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class MSerializer<T> {
    private final BiConsumer<T, ByteBuffer> messageSerializer;
    private final Function<ByteBuffer, T> messageDeserializer;
    private final int messageSize; // Upper bound on size of single message

    public MSerializer(BiConsumer<T, ByteBuffer> messageSerializer, Function<ByteBuffer, T> messageDeserializer, int messageSize) {
        this.messageSerializer = messageSerializer;
        this.messageDeserializer = messageDeserializer;
        this.messageSize = messageSize;
    }

    public byte[] serialize(NetworkTypes.DataPacket<T> dp) {
        int size = 1 + 4 + 1 + 1 + messageSize * dp.data.size();
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.put((byte)(1));
        bb.putInt(dp.n);
        bb.put((byte)(dp.from));
        bb.put((byte)(dp.data.size()));
        for (T m : dp.data)
            messageSerializer.accept(m, bb);
        return bb.array();
    }

    public byte[] serialize(NetworkTypes.AckPacket ap) {
        int size = 1 + 4 + 1;
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.put((byte)(0));
        bb.putInt(ap.n);
        bb.put((byte)(ap.receiver_id));
        return bb.array();
    }

    public boolean isDatapacket(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte out = bb.get();
        return (int)(out) > 0;
    }

    public NetworkTypes.DataPacket<T> deserializeDataPacket(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        int type = bb.get();
        assert type > 0;

        int n = bb.getInt();
        int from = bb.get();

        int nMessages = bb.get();
        List<T> messages = new ArrayList<>(nMessages);
        for (int i=0; i<nMessages; i++) {
            messages.add(messageDeserializer.apply(bb));
        }

        return new NetworkTypes.DataPacket<>(n, from, messages);
    }

    public NetworkTypes.AckPacket deserializeAckPacket(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        int type = bb.get();
        assert type == 0;

        int n = bb.getInt();
        int receiver_id = bb.get();

        return new NetworkTypes.AckPacket(n, receiver_id);
    }
}
