package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ReadReply extends ProtoMessage {
    public final static short MSG_ID = 107;
    private final int peerOpID;
    private final char[] key;
    private final Pair<Integer, Host> tag;
    private final byte[] value;

    public ReadReply(int peerOpID, char[] key, Pair<Integer, Host> tag, byte[] value) {
        super(MSG_ID);
        this.peerOpID = peerOpID;
        this.key = key;
        this.tag = tag;
        this.value = value;
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

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ReadReply: {" +
                "key='"+ new String(key) +'\''+
                ", tag='" + tag + '\''+
                ", id=" + peerOpID +
                "}";
    }

    public static ISerializer<ReadReply> serializer = new ISerializer<ReadReply>() {
        @Override
        public void serialize(ReadReply msg, ByteBuf out) throws IOException {
            serializeCharArray(out, msg.getKey());
            out.writeInt(msg.getTag().getLeft());
            Host.serializer.serialize(msg.getTag().getRight(), out);
            out.writeInt(msg.getPeerOpID());
            serializeByteArray(out, msg.getValue());
        }

        private void serializeByteArray(ByteBuf out, byte[] array) {
            if (array == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeInt(array.length);
                out.writeBytes(array);
            }
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public ReadReply deserialize(ByteBuf in) throws IOException {
            char[] key = deserializeCharArray(in);
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            int id = in.readInt();
            byte[] value = deserializeByteArray(in);
            return new ReadReply(id, key, p, value);
        }

        private byte[] deserializeByteArray(ByteBuf in) {
            boolean isNotNull = in.readBoolean();
            if (!isNotNull) {
                return null;
            }
            int length = in.readInt();
            byte[] tmp = new byte[length];
            in.readBytes(tmp);
            return tmp;
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
