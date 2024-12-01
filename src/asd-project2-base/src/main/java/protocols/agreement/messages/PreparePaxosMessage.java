package protocols.agreement.messages;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PreparePaxosMessage extends ProtoMessage {
    public enum OperationType {
        REGULAR,
        JOIN
    }

    public static final short MESSAGE_ID = 101;

    private final UUID paxosInstanceID;
    private int ballot;
    private final UUID operationID;
    private OperationType operationType;

    public PreparePaxosMessage(UUID paxosInstanceID, int ballot, UUID operationID, OperationType operationType) {
        super(MESSAGE_ID);
        this.paxosInstanceID = paxosInstanceID;
        this.ballot = ballot;
        this.operationID = operationID;
        this.operationType = operationType;
    };

    public UUID getPaxosInstanceID() {
        return paxosInstanceID;
    }

    public int getBallot() {
        return ballot;
    }

    public UUID getOperationID() {
        return operationID;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    @Override
    public String toString() {
        return "PreparePaxosMessage{" +
                "paxosInstanceID=" + paxosInstanceID +
                ", ballot=" + ballot +
                ", operationID=" + operationID +
                ", operationType=" + operationType +
                '}';
    }

    public static ISerializer<PreparePaxosMessage> serializer = new ISerializer<PreparePaxosMessage>() {
        @Override
        public void serialize(PreparePaxosMessage msg, ByteBuf out) {
            out.writeLong(msg.paxosInstanceID.getMostSignificantBits());
            out.writeLong(msg.paxosInstanceID.getLeastSignificantBits());
            out.writeInt(msg.ballot);
            out.writeLong(msg.operationID.getMostSignificantBits());
            out.writeLong(msg.operationID.getLeastSignificantBits());
            out.writeInt(msg.operationType.ordinal());
        }

        @Override
        public PreparePaxosMessage deserialize(ByteBuf in) {
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID paxosInstanceID = new UUID(highBytes, lowBytes);
            int ballot = in.readInt();
            long highBytes2 = in.readLong();
            long lowBytes2 = in.readLong();
            UUID operationID = new UUID(highBytes2, lowBytes2);
            OperationType operationType = OperationType.values()[in.readInt()];
            return new PreparePaxosMessage(paxosInstanceID, ballot, operationID, operationType);
        }
    };
}
