package protocols.statemachine.messages;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class StateTransferMessage extends ProtoMessage {
    public static final short MSG_ID = 203;

    private final byte[] state;
    private final Host leader;
    private final Host replica;
    private List<Host> membership;

    public StateTransferMessage(byte[] state, Host leader, Host replica, List<Host> membership) {
        super(MSG_ID);
        this.state = state;
        this.leader = leader;
        this.replica = replica;
        this.membership = membership;
    }

    public byte[] getState() {
        return state;
    }

    public Host getLeader() {
        return leader;
    }

    public Host getReplica() {
        return replica;
    }

    public List<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "StateTransferMessage{" +
                "state=" + state +
                ", leader=" + leader +
                ", replica=" + replica +
                ", membership=" + membership +
                '}';
    }

    public static ISerializer<StateTransferMessage> serializer = new ISerializer<StateTransferMessage>() {
        @Override
        public void serialize(StateTransferMessage msg, ByteBuf out) throws IOException {
            serializeByteArray(out, msg.getState());
            Host.serializer.serialize(msg.getLeader(), out);
            Host.serializer.serialize(msg.getReplica(), out);
            if (msg.getMembership() != null) {
                out.writeInt(msg.getMembership().size());
                for (Host host : msg.getMembership()) {
                    Host.serializer.serialize(host, out);
                }
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public StateTransferMessage deserialize(ByteBuf in) throws IOException {
            byte[] state = deserializeByteArray(in);
            Host leader = Host.serializer.deserialize(in);
            Host replica = Host.serializer.deserialize(in);
            int membershipSize = in.readInt();
            LinkedList<Host> membership = new LinkedList<>();
            for (int i = 0; i < membershipSize; i++) {
                membership.add(Host.serializer.deserialize(in));
            }
            return new StateTransferMessage(state, leader, replica, membership);
        }

        private void serializeByteArray(ByteBuf out, byte[] array) {
            out.writeInt(array.length);
            out.writeBytes(array);
        }

        private byte[] deserializeByteArray(ByteBuf in) {
            int length = in.readInt();
            byte[] tmp = new byte[length];
            in.readBytes(tmp);
            return tmp;
        }
    };
}
