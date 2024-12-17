package protocols.abd.messages.writeread;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class Ack extends ProtoMessage {
    public final static short MSG_ID = 110;
    private final int opSeq;
    private final char[] key;

    public Ack(int opSeq, char[] key) {
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

    public static ISerializer<Ack> serializer = new ISerializer<Ack>() {
        @Override
        public void serialize(Ack msg, ByteBuf out) {
            out.writeInt(msg.opSeq);
            serializeCharArray(out, msg.getKey());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public Ack deserialize(ByteBuf in) {
            int opSeq = in.readInt();
            char[] key = deserializeCharArray(in);
            return new Ack(opSeq, key);
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
