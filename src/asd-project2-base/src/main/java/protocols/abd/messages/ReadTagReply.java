package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Arrays;

public class ReadTagReply extends ProtoMessage {
    public final static short MSG_ID = 102;
    private final Pair<Integer, Host> tag;
    private final int peerOpID;
    private final char[] key;
    public ReadTagReply(Pair<Integer, Host> tag, int peerOpID, char[] key) {
        super(MSG_ID);
        this.peerOpID = peerOpID;
        this.tag = tag;
        this.key = key;
    }

    public char[] getKey() {
        return key;
    }

    public int getPeerOpID() {
        return peerOpID;
    }

    public Pair<Integer, Host> getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "ReadTagReply: {" +
                "key='"+ new String(key) +'\''+
                ", tag='" + tag + '\''+
                ", id=" + peerOpID +
                "}";
    }

    public static ISerializer<ReadTagReply> serializer = new ISerializer<ReadTagReply>() {
        @Override
        public void serialize(ReadTagReply msg, ByteBuf out) throws IOException {
            serializeCharArray(out, msg.getKey());
            out.writeInt(msg.getTag().getLeft());
            Host.serializer.serialize(msg.getTag().getRight(), out);
            out.writeInt(msg.getPeerOpID());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public ReadTagReply deserialize(ByteBuf in) throws IOException {
            char[] key = deserializeCharArray(in);
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            int id = in.readInt();
            return new ReadTagReply(p, id, key);
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
