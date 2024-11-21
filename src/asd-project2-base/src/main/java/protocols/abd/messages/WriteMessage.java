package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class WriteMessage extends ProtoMessage {
    public final static short MSG_ID = 103;
    private final Pair<Integer, UUID> tag;
    private final byte[] data;

    public WriteMessage(Pair<Integer, UUID> tag, byte[] data) {
        super(MSG_ID);
        this.data = data;
        this.tag = tag;
    }

    public Pair<Integer, UUID> getTag() { return tag; }

    public byte[] getData() { return data; }

    @Override
    public String toString() {
        return "ReadTag";
    }

    public static ISerializer<WriteMessage> serializer = new ISerializer<WriteMessage>() {
        @Override
        public void serialize(WriteMessage msg, ByteBuf out) {
            out.writeInt(msg.getTag().getLeft());
            serializeUUID(out, msg.getTag().getRight());
            serializeByteArray(out, msg.getData());
        }

        private void serializeUUID(ByteBuf out, UUID uuid) {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }

        private void serializeByteArray(ByteBuf out, byte[] array) {
            out.writeInt(array.length);
            out.writeBytes(array);
        }

        @Override
        public WriteMessage deserialize(ByteBuf in) {
            int tagLeft = in.readInt();
            UUID uuid = deserializeUUID(in);
            Pair<Integer, UUID> p = Pair.of(tagLeft, uuid);
            byte[] data = deserializeByteArray(in);
            return new WriteMessage(p, data);
        }

        private byte[] deserializeByteArray(ByteBuf in) {
            int length = in.readInt();
            byte[] tmp = new byte[length];
            in.readBytes(tmp);
            return tmp;
        }

        private UUID deserializeUUID(ByteBuf in) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            return new UUID(mostSigBits, leastSigBits);
        }
    };
}
