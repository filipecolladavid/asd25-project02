package protocols.agreement.messages;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PromisePaxosMessage extends ProtoMessage {
    public enum OperationType {
        REGULAR,
        JOIN
    }

    public static final short MESSAGE_ID = 102;
    private final UUID paxosInstanceID;
    private int ballot;
    private final UUID operationID;
    private OperationType operationType;

    public PromisePaxosMessage(UUID paxosInstanceID, int ballot, UUID operationID, OperationType operationType) {
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
        return "PromisePaxosMessage{" +
                "paxosInstanceID=" + paxosInstanceID +
                ", ballot=" + ballot +
                ", operationID=" + operationID +
                ", operationType=" + operationType +
                '}';
    }

    public static ISerializer<PromisePaxosMessage> serializer = new ISerializer<PromisePaxosMessage>() {
        @Override
        public void serialize(PromisePaxosMessage msg, ByteBuf out) {
            out.writeLong(msg.paxosInstanceID.getMostSignificantBits());
            out.writeLong(msg.paxosInstanceID.getLeastSignificantBits());
            out.writeInt(msg.ballot);
            out.writeLong(msg.operationID.getMostSignificantBits());
            out.writeLong(msg.operationID.getLeastSignificantBits());
            out.writeInt(msg.operationType.ordinal());
        }

        @Override
        public PromisePaxosMessage deserialize(ByteBuf in) {
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID paxosInstanceID = new UUID(highBytes, lowBytes);
            int ballot = in.readInt();
            long highBytes2 = in.readLong();
            long lowBytes2 = in.readLong();
            UUID operationID = new UUID(highBytes2, lowBytes2);
            OperationType operationType = OperationType.values()[in.readInt()];
            return new PromisePaxosMessage(paxosInstanceID, ballot, operationID, operationType);
        }
    };
}
