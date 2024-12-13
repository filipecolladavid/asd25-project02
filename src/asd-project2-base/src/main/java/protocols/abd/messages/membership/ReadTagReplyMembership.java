package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class ReadTagReplyMembership extends ProtoMessage {
    public final static short MSG_ID = 108;
    private final Pair<Integer, Host> tag;
    private final UUID peerOpID;
    public ReadTagReplyMembership(Pair<Integer, Host> tag, UUID peerOpID) {
        super(MSG_ID);
        this.peerOpID = peerOpID;
        this.tag = tag;
    }

    public UUID getPeerOpID() {
        return peerOpID;
    }

    public Pair<Integer, Host> getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "ReadTagReplyMembership: {" +
                "tag='" + tag + '\''+
                ", id=" + peerOpID +
                "}";
    }

    public static ISerializer<ReadTagReplyMembership> serializer = new ISerializer<ReadTagReplyMembership>() {
        @Override
        public void serialize(ReadTagReplyMembership msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getTag().getLeft());
            Host.serializer.serialize(msg.getTag().getRight(), out);
            serializeCharArray(out, msg.getPeerOpID().toString().toCharArray());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public ReadTagReplyMembership deserialize(ByteBuf in) throws IOException {
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            UUID id = UUID.fromString(new String(deserializeCharArray(in)));
            return new ReadTagReplyMembership(p, id);
        }

        private char[] deserializeCharArray(ByteBuf in) {
            int length = in.readInt();
            char[] array = new char[length];
            for(int i = 0; i < length; i++) {
                array[i] = in.readChar();
            }
            return array;
        }
    };
}
