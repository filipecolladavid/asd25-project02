package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class WriteMessageMembership extends ProtoMessage {
    public final static short MSG_ID = 109;
    private final int opSeq;
    private final Pair<Integer, Host> tag;
    private final Host replica;
    private final Action action;
    private final UUID opID;



    public WriteMessageMembership(int opSeq, Pair<Integer, Host> tag, Host replica, UUID opID, Action action) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.opID = opID;
        this.tag = tag;
        this.replica = replica;
        this.action = action;
    }

    public Host getReplica() {
        return replica;
    }

    public Action getAction() {
        return action;
    }

    public UUID getOpID() {
        return opID;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public Pair<Integer, Host> getTag() { return tag; }

    @Override
    public String toString() {
        return "WriteMessage: {" +
                "opSeq=" + opSeq +
                ", tag='" + tag + '\''+
                "}";
    }

    public static ISerializer<WriteMessageMembership> serializer = new ISerializer<WriteMessageMembership>() {
        @Override
        public void serialize(WriteMessageMembership msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getAction().ordinal());
            out.writeInt(msg.opSeq);
            out.writeInt(msg.getTag().getLeft());
            Host.serializer.serialize(msg.getTag().getRight(), out);
            Host.serializer.serialize(msg.getReplica(), out);
            serializeCharArray(out, msg.getOpID().toString().toCharArray());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public WriteMessageMembership deserialize(ByteBuf in) throws IOException {
            Action action = Action.values()[in.readInt()];
            int opSeq = in.readInt();
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            Host replica = Host.serializer.deserialize(in);
            UUID id = UUID.fromString(new String(deserializeCharArray(in)));
            return new WriteMessageMembership(opSeq, p, replica, id, action);
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
