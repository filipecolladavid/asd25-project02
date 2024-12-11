package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class WriteMessageMembership extends ProtoMessage {
    public final static short MSG_ID = 107;
    private final int opSeq;
    private final Pair<Integer, Host> tag;
    private final Host replica;
    private final Action action;



    public WriteMessageMembership(int opSeq, Pair<Integer, Host> tag, Host replica, Action action) {
        super(MSG_ID);
        this.opSeq = opSeq;
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
        }
//
//        private void serializeHashSet(ByteBuf out, Host replica) throws IOException {
//            out.writeInt(membership.size());
//            for (Host member : membership) {
//                Host.serializer.serialize(member, out);
//            }
//        }

        @Override
        public WriteMessageMembership deserialize(ByteBuf in) throws IOException {
            Action action = Action.values()[in.readInt()];
            int opSeq = in.readInt();
            int tagLeft = in.readInt();
            Host h = Host.serializer.deserialize(in);
            Pair<Integer, Host> p = Pair.of(tagLeft, h);
            Host replica = Host.serializer.deserialize(in);
            return new WriteMessageMembership(opSeq, p, replica, action);
        }

//        private Set<Host> deserializeHashSet(ByteBuf in) throws IOException {
//            Host replica = new HashSet<>();
//            int size = in.readInt();
//            for (int i = 0; i < size; i++) {
//                Host h = Host.serializer.deserialize(in);
//                membership.add(h);
//            }
//            return membership;
//        }

    };
}
