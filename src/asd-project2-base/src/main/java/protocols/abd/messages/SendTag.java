package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class SendTag extends ProtoMessage {
    public final static short MSG_ID = 102;
    private final Pair<Integer, UUID> tag;
    public SendTag(Pair<Integer, UUID> tag) {
        super(MSG_ID);
        this.tag = tag;
    }

    public Pair<Integer, UUID> getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "ReadTag";
    }

    public static ISerializer<SendTag> serializer = new ISerializer<SendTag>() {
        @Override
        public void serialize(SendTag msg, ByteBuf out) {
            out.writeInt(msg.getTag().getLeft());
            serializeUUID(out, msg.getTag().getRight());
        }

        private void serializeUUID(ByteBuf out, UUID uuid) {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }

        @Override
        public SendTag deserialize(ByteBuf in) {
            int tagLeft = in.readInt();
            UUID uuid = deserializeUUID(in);
            Pair<Integer, UUID> p = Pair.of(tagLeft, uuid);
            return new SendTag(p);
        }

        private UUID deserializeUUID(ByteBuf in) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            return new UUID(mostSigBits, leastSigBits);
        }
    };
}
