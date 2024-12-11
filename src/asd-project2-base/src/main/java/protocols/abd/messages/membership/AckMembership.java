package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AckMembership extends ProtoMessage {
    public final static short MSG_ID = 101;
    private final int opSeq;
    private final char[] key;
    private final Action action;

    public AckMembership(int opSeq, char[] key, Action action) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.action = action;
    }

    @Override
    public String toString() {
        return "Ack for "+this.action+"\n";
    }

    public int getOpSeq() {
        return opSeq;
    }

    public char[] getKey() {
        return key;
    }

    public static ISerializer<AckMembership> serializer = new ISerializer<AckMembership>() {
        @Override
        public void serialize(AckMembership msg, ByteBuf out) {
            out.writeInt(msg.getAction().ordinal());
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
        public AckMembership deserialize(ByteBuf in) {
            Action action = Action.values()[in.readInt()];
            int opSeq = in.readInt();
            char[] key = deserializeCharArray(in);
            return new AckMembership(opSeq, key, action);
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

    public Action getAction() {
        return action;
    }
}
