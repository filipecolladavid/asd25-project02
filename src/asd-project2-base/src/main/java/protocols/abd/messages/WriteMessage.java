package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class WriteMessage extends ProtoMessage {
    public final static short MSG_ID = 103;
    private final int opSeq;
    private final char[] key;
    private final Pair<Integer, Host> tag;
    private final byte[] data;

    public WriteMessage(int opSeq, char[] key, Pair<Integer, Host> tag, byte[] data) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.tag = tag;
        this.data = data;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public char[] getKey() {
        return key;
    }

    public Pair<Integer, Host> getTag() { return tag; }


    public byte[] getData() { return data; }

    @Override
    public String toString() {
        return "WriteMessage: {" +
                "opSeq=" + opSeq +
                ", key='"+ new String(key) +'\''+
                ", tag='" + tag + '\''+
                "}";
    }

    public static ISerializer<WriteMessage> serializer = new ISerializer<WriteMessage>() {
        @Override
        public void serialize(WriteMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.opSeq);
            serializeCharArray(out, msg.getKey());
            out.writeInt(msg.getTag().getLeft());
            Host.serializer.serialize(msg.getTag().getRight(), out);
            serializeByteArray(out, msg.getData());
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
        public WriteMessage deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            char[] key = deserializeCharArray(in);
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            byte[] data = deserializeByteArray(in);
            return new WriteMessage(opSeq, key, p, data);
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
