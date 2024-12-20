package protocols.agreement.messages;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class NewNodeMessage extends ProtoMessage {
    public static final short MSG_ID = 106;
    private final UUID operationId;
    private final Host joiningNode;
    private final List<Host> updatedMembership;

    public NewNodeMessage(UUID operationId, Host joiningNode, List<Host> membership) {
        super(MSG_ID);
        this.operationId = operationId;
        this.joiningNode = joiningNode;
        this.updatedMembership = new LinkedList<>(membership);
    }

    public UUID getOperationId() {
        return operationId;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public List<Host> getUpdatedMembership() {
        return updatedMembership;
    }

    @Override
    public String toString() {
        return "NewNodeMessage{" +
                "operationId=" + operationId +
                ", joiningNode=" + joiningNode +
                ", updatedMembership=" + updatedMembership +
                '}';
    }

    public static final ISerializer<NewNodeMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NewNodeMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.operationId.getMostSignificantBits());
            out.writeLong(msg.operationId.getLeastSignificantBits());
            Host.serializer.serialize(msg.joiningNode, out);
            if (msg.updatedMembership != null) {
                out.writeInt(msg.updatedMembership.size());
                for (Host host : msg.updatedMembership) {
                    Host.serializer.serialize(host, out);
                }
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public NewNodeMessage deserialize(ByteBuf in) throws IOException {
            if (in.readableBytes() < 24) {
                throw new IOException("Buffer too small - not enough data");
            }

            // Read operation ID
            UUID operationId = new UUID(in.readLong(), in.readLong());

            // Read joining node
            Host joiningNode = Host.serializer.deserialize(in);

            // Read updated membership
            int membershipSize = in.readInt();
            LinkedList<Host> membership = new LinkedList<>();
            for (int i = 0; i < membershipSize; i++) {
                membership.add(Host.serializer.deserialize(in));
            }

            return new NewNodeMessage(operationId, joiningNode, membership);
        }
    };
}
