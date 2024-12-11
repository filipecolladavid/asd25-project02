package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ReadTagReplyMembership extends ProtoMessage {
    public final static short MSG_ID = 106;
    private final Pair<Integer, Host> tag;
    private final int peerOpID;
    public ReadTagReplyMembership(Pair<Integer, Host> tag, int peerOpID) {
        super(MSG_ID);
        this.peerOpID = peerOpID;
        this.tag = tag;
    }

    public int getPeerOpID() {
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
            out.writeInt(msg.getPeerOpID());
        }

        @Override
        public ReadTagReplyMembership deserialize(ByteBuf in) throws IOException {
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            int id = in.readInt();
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
