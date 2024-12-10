package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTagMembership extends ProtoMessage {

    public final static short MSG_ID = 101;
    private int opSec;
    private char[] key;

    public ReadTagMembership(int opSec, char[] key) {
        super(MSG_ID);
        this.opSec = opSec;
        this.key = key;
    }

    public int getOpSec() {
        return opSec;
    }

    public char[] getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "ReadTagMembership={" +
                "opSec="+opSec+
                ", key="+ new String(key) +
                "}";
    }

    public static ISerializer<ReadTagMembership> serializer = new ISerializer<ReadTagMembership>() {
        @Override
        public void serialize(ReadTagMembership msg, ByteBuf out) {
            out.writeInt(msg.opSec);
            serializeCharArray(out, msg.getKey());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public ReadTagMembership deserialize(ByteBuf in) {
            int opSec = in.readInt();
            char[] key = deserializeCharArray(in);
            return new ReadTagMembership(opSec, key);
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
