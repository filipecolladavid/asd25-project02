package protocols.agreement.messages;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptPaxosMessage extends ProtoMessage {
    public static final short MESSAGE_ID = 103;
    private final UUID paxosInstanceID;
    private int ballot;
    private final UUID operationID;

    public AcceptPaxosMessage(UUID paxosInstanceID, int ballot, UUID operationID) {
        super(MESSAGE_ID);
        this.paxosInstanceID = paxosInstanceID;
        this.ballot = ballot;
        this.operationID = operationID;
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

    @Override
    public String toString() {
        return "AcceptPaxosMessage{" +
                "paxosInstanceID=" + paxosInstanceID +
                ", ballot=" + ballot +
                ", operationID=" + operationID +
                '}';
    }

    public static ISerializer<AcceptPaxosMessage> serializer = new ISerializer<AcceptPaxosMessage>() {
        @Override
        public void serialize(AcceptPaxosMessage msg, ByteBuf out) {
            out.writeLong(msg.paxosInstanceID.getMostSignificantBits());
            out.writeLong(msg.paxosInstanceID.getLeastSignificantBits());
            out.writeInt(msg.ballot);
            out.writeLong(msg.operationID.getMostSignificantBits());
            out.writeLong(msg.operationID.getLeastSignificantBits());
        }

        @Override
        public AcceptPaxosMessage deserialize(ByteBuf in) {
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID paxosInstanceID = new UUID(highBytes, lowBytes);
            int ballot = in.readInt();
            long highBytes2 = in.readLong();
            long lowBytes2 = in.readLong();
            UUID operationID = new UUID(highBytes2, lowBytes2);
            return new AcceptPaxosMessage(paxosInstanceID, ballot, operationID);
        }
    };
}
