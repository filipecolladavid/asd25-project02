package protocols.statemachine.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.ISerializer;

// Message that is sent to the leader to join the system
public class JoinMessage extends ProtoMessage {
    public static final short MSG_ID = 202;

    public enum JoinType {
        REQUEST,
        RESPONSE
    }

    private final UUID opID;
    private final JoinType joinType;
    private final Host joiningNode;
    private final int currentInstance;
    private final byte[] operation;

    public JoinMessage(UUID opID, JoinType joinType, Host joiningNode, int currentInstance) {
        super(MSG_ID);
        this.opID = opID;
        this.joinType = joinType;
        this.joiningNode = joiningNode;
        this.currentInstance = currentInstance;

        this.operation = new byte[JoinType.REQUEST.ordinal()];
    }

    public UUID getOpID() {
        return opID;
    }

    public byte[] getOperation() {
        return operation;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public int getCurrentInstance() {
        return currentInstance;
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage joinMessage, ByteBuf out) throws IOException {
            out.writeLong(joinMessage.opID.getMostSignificantBits());
            out.writeLong(joinMessage.opID.getLeastSignificantBits());

            out.writeInt(joinMessage.joinType.ordinal());

            byte[] addressBytes = joinMessage.joiningNode.getAddress().getAddress();
            out.writeInt(addressBytes.length);
            out.writeBytes(addressBytes);
            out.writeInt(joinMessage.joiningNode.getPort());

            out.writeInt(joinMessage.currentInstance);

            out.writeInt(joinMessage.operation.length);
            out.writeBytes(joinMessage.operation);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            UUID opID = new UUID(mostSigBits, leastSigBits);

            int joinTypeOrdinal = in.readInt();
            JoinType joinType = JoinType.values()[joinTypeOrdinal];

            int addressLength = in.readInt();
            byte[] addressBytes = new byte[addressLength];
            in.readBytes(addressBytes);
            java.net.InetAddress address = java.net.InetAddress.getByAddress(addressBytes);
            int port = in.readInt();
            Host joiningNode = new Host(address, port);

            int currentInstance = in.readInt();

            int operationLength = in.readInt();
            byte[] operation = new byte[operationLength];
            in.readBytes(operation);

            return new JoinMessage(opID, joinType, joiningNode, currentInstance);
        }
    };

    @Override
    public String toString() {
        return "JoinMessage{" +
                "opID=" + opID +
                "joinType=" + joinType +
                ", joiningNode=" + joiningNode +
                ", currentInstance=" + currentInstance +
                '}';
    }
}
