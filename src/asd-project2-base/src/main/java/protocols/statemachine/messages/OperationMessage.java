package protocols.statemachine.messages;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

// Base Message that encapsulates an operation
public class OperationMessage extends ProtoMessage {
    public static final short MSG_ID = 201;

    public enum OperationType {
        NEW_STATE,
        JOIN_REQUEST,
        ADD_REPLICA,
        REMOVE_REPLICA,
    }

    private final UUID operationId;
    private final OperationType operationType;
    private final Host operationRequester;
    private final int instanceNumber;

    public OperationMessage(UUID operationId, OperationType operationType,
            Host operationRequester, int instanceNumber) {
        super(MSG_ID);
        this.operationId = operationId;
        this.operationType = operationType;
        this.operationRequester = operationRequester;
        this.instanceNumber = instanceNumber;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public Host getOperationRequester() {
        return operationRequester;
    }

    public int getInstanceNumber() {
        return instanceNumber;
    }

    public byte[] getOperationPayload() {
        ByteBuf buf = Unpooled.buffer();
        try {
            serializer.serialize(this, buf);
            byte[] payload = new byte[buf.readableBytes()];
            buf.readBytes(payload);

            return payload;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize operation", e);
        } finally {
            buf.release();
        }
    }

    public static ISerializer<OperationMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(OperationMessage operationMessage, ByteBuf out) throws IOException {
            out.writeLong(operationMessage.operationId.getMostSignificantBits());
            out.writeLong(operationMessage.operationId.getLeastSignificantBits());
            out.writeInt(operationMessage.operationType.ordinal());
            byte[] addressBytes = operationMessage.operationRequester.getAddress().getAddress();
            out.writeInt(addressBytes.length);
            out.writeBytes(addressBytes);
            out.writeInt(operationMessage.operationRequester.getPort());
            out.writeInt(operationMessage.instanceNumber);
        }

        @Override
        public OperationMessage deserialize(ByteBuf in) throws IOException {
            // Add validation for minimum required bytes
            if (in.readableBytes() < 24) { // 8 + 8 + 4 + 4 minimum
                throw new IOException("Buffer too small - not enough data");
            }

            long mostSignificantBits = in.readLong();
            long leastSignificantBits = in.readLong();
            UUID operationId = new UUID(mostSignificantBits, leastSignificantBits);

            int typeOrdinal = in.readInt();
            if (typeOrdinal < 0 || typeOrdinal >= OperationType.values().length) {
                throw new IOException("Invalid operation type ordinal: " + typeOrdinal);
            }
            OperationType operationType = OperationType.values()[typeOrdinal];

            int addressLength = in.readInt();
            if (addressLength <= 0 || addressLength > 16) { // IPv4 or IPv6 address
                throw new IOException("Invalid address length: " + addressLength);
            }

            byte[] addressBytes = new byte[addressLength];
            in.readBytes(addressBytes);
            java.net.InetAddress address = java.net.InetAddress.getByAddress(addressBytes);

            int port = in.readInt();
            if (port < 0 || port > 65535) {
                throw new IOException("Invalid port number: " + port);
            }

            Host operationRequester = new Host(address, port);
            int instanceNumber = in.readInt();

            return new OperationMessage(operationId, operationType, operationRequester, instanceNumber);
        }
    };

    @Override
    public String toString() {
        return "OperationMessage{" +
                "operationId=" + operationId +
                ", operationType=" + operationType +
                ", operationRequester=" + operationRequester +
                ", instanceNumber=" + instanceNumber +
                '}';
    }
}
