package protocols.statemachine.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

// Base Message that encapsulates an operation
public class OperationMessage extends ProtoMessage {
    public static final short MSG_ID = 201;

    private final UUID operationId;
    private final byte[] operationPayload;
    private final int instanceNumber;

    public OperationMessage(UUID operationId, byte[] operationPayload, int instanceNumber) {
        super(MSG_ID);
        this.operationId = operationId;
        this.operationPayload = operationPayload;
        this.instanceNumber = instanceNumber;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public byte[] getOperationPayload() {
        return operationPayload;
    }

    public int getInstanceNumber() {
        return instanceNumber;
    }

    public static ISerializer<OperationMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(OperationMessage operationMessage, ByteBuf out) throws IOException {
            out.writeLong(operationMessage.operationId.getMostSignificantBits());
            out.writeLong(operationMessage.operationId.getLeastSignificantBits());
            out.writeInt(operationMessage.operationPayload.length);
            out.writeBytes(operationMessage.operationPayload);
            out.writeInt(operationMessage.instanceNumber);
        }

        @Override
        public OperationMessage deserialize(ByteBuf in) throws IOException {
            long mostSignificantBits = in.readLong();
            long leastSignificantBits = in.readLong();
            UUID operationId = new UUID(mostSignificantBits, leastSignificantBits);

            int operationPayloadLength = in.readInt();
            byte[] operationPayload = new byte[operationPayloadLength];
            in.readBytes(operationPayload);

            int instanceNumber = in.readInt();
            return new OperationMessage(operationId, operationPayload, instanceNumber);
        }
    };

    @Override
    public String toString() {
        return "OperationMessage{" +
                "operationId=" + operationId +
                ", operationPayload=" + operationPayload +
                ", instanceNumber=" + instanceNumber +
                '}';
    }
}
