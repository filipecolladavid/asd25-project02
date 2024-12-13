package protocols.abd.messages.writeread;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ReadMessage extends ProtoMessage {
    public final static short MSG_ID = 111;
    private final int opSeq;
    private final char[] key;

    public ReadMessage(int opSeq, char[] key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public char[] getKey() {
        return key;
    }


    @Override
    public String toString() {
        return "ReadMessage: {" +
                "opSeq=" + opSeq +
                ", key='"+ new String(key) +'\''+
                "}";
    }

    public static ISerializer<ReadMessage> serializer = new ISerializer<ReadMessage>() {
        @Override
        public void serialize(ReadMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getOpSeq());
            serializeCharArray(out, msg.getKey());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public ReadMessage deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            char[] key = deserializeCharArray(in);
            return new ReadMessage(opSeq, key);
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
