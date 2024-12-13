package protocols.abd.messages.writeread;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTag extends ProtoMessage {

    public final static short MSG_ID = 113;
    private int opSec;
    private char[] key;

    public ReadTag(int opSec, char[] key) {
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
        return "ReadTag={" +
                "opSec="+opSec+
                ", key="+ new String(key) +
                "}";
    }

    public static ISerializer<ReadTag> serializer = new ISerializer<ReadTag>() {
        @Override
        public void serialize(ReadTag msg, ByteBuf out) {
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
        public ReadTag deserialize(ByteBuf in) {
            int opSec = in.readInt();
            char[] key = deserializeCharArray(in);
            return new ReadTag(opSec, key);
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
