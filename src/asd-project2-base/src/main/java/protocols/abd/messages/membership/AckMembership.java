package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AckMembership extends ProtoMessage {
    public final static short MSG_ID = 101;
    private final int opSeq;

    private final UUID opID;
    private final Action action;

    public AckMembership(int opSeq, UUID opID, Action action) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.opID = opID;
        this.action = action;
    }

    @Override
    public String toString() {
        return "Ack for "+this.action+"\n";
    }

    public int getOpSeq() {
        return opSeq;
    }
    public UUID getOpID() {
        return opID;
    }

    public static ISerializer<AckMembership> serializer = new ISerializer<AckMembership>() {
        @Override
        public void serialize(AckMembership msg, ByteBuf out) {
            out.writeInt(msg.getAction().ordinal());
            out.writeInt(msg.opSeq);
            serializeCharArray(out, msg.getOpID().toString().toCharArray());
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
            UUID opID = UUID.fromString(new String(deserializeCharArray(in)));
            return new AckMembership(opSeq, opID, action);
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
