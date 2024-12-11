package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class ReadTagMembership extends ProtoMessage {

    public final static short MSG_ID = 105;
    private final int opSec;
    private final UUID opID;

    public ReadTagMembership(int opSec, UUID opID) {
        super(MSG_ID);
        this.opSec = opSec;
        this.opID = opID;
    }

    public UUID getOpID() {
        return opID;
    }

    public int getOpSec() {
        return opSec;
    }

    @Override
    public String toString() {
        return "ReadTagMembership={" +
                "opSec="+opSec+
                "}";
    }

    public static ISerializer<ReadTagMembership> serializer = new ISerializer<ReadTagMembership>() {
        @Override
        public void serialize(ReadTagMembership msg, ByteBuf out) {
            out.writeInt(msg.opSec);
            serializeCharArray(out, msg.getOpID().toString().toCharArray());
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
            String id = new String(deserializeCharArray(in));
            UUID opID = UUID.fromString(id);
            return new ReadTagMembership(opSec, opID);
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
