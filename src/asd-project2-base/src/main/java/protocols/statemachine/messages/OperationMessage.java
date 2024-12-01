package protocols.statemachine.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class OperationMessage extends ProtoMessage {
    public static final short MSG_ID = 201;

    private final UUID operationId;
    private final Host requester;
    private final int instanceNumber;
    private final byte[] payload;

    public OperationMessage(UUID operationId, Host requester, int instanceNumber, byte[] payload) {
        super(MSG_ID);
        this.operationId = operationId;
        this.requester = requester;
        this.instanceNumber = instanceNumber;
        this.payload = payload;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public Host getRequester() {
        return requester;
    }

    public int getInstanceNumber() {
        return instanceNumber;
    }

    public byte[] getPayload() {
        return payload;
    }

    public byte[] getOperationPayload() {
        ByteBuf buf = Unpooled.buffer();
        try {
            serializer.serialize(this, buf);
            byte[] operationPayload = new byte[buf.readableBytes()];
            buf.readBytes(operationPayload);
            return operationPayload;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize operation", e);
        } finally {
            buf.release();
        }
    }

    public static final ISerializer<OperationMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(OperationMessage msg, ByteBuf out) throws IOException {
            // Write operation ID
            out.writeLong(msg.operationId.getMostSignificantBits());
            out.writeLong(msg.operationId.getLeastSignificantBits());

            // Write requester info
            byte[] addressBytes = msg.requester.getAddress().getAddress();
            out.writeInt(addressBytes.length);
            out.writeBytes(addressBytes);
            out.writeInt(msg.requester.getPort());

            // Write instance number
            out.writeInt(msg.instanceNumber);

            // Write payload
            if (msg.payload != null) {
                out.writeInt(msg.payload.length);
                out.writeBytes(msg.payload);
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public OperationMessage deserialize(ByteBuf in) throws IOException {
            if (in.readableBytes() < 24) {
                throw new IOException("Buffer too small - not enough data");
            }

            // Read operation ID
            UUID operationId = new UUID(in.readLong(), in.readLong());

            // Read requester info
            int addressLength = in.readInt();
            if (addressLength <= 0 || addressLength > 16) {
                throw new IOException("Invalid address length: " + addressLength);
            }
            byte[] addressBytes = new byte[addressLength];
            in.readBytes(addressBytes);
            java.net.InetAddress address = java.net.InetAddress.getByAddress(addressBytes);
            int port = in.readInt();
            if (port < 0 || port > 65535) {
                throw new IOException("Invalid port number: " + port);
            }
            Host requester = new Host(address, port);

            // Read instance number
            int instanceNumber = in.readInt();

            // Read payload
            byte[] payload = null;
            int payloadLength = in.readInt();
            if (payloadLength > 0) {
                payload = new byte[payloadLength];
                in.readBytes(payload);
            }

            return new OperationMessage(operationId, requester, instanceNumber, payload);
        }
    };

    @Override
    public String toString() {
        return "OperationMessage{" +
                "operationId=" + operationId +
                ", requester=" + requester +
                ", instanceNumber=" + instanceNumber +
                ", payloadSize=" + (payload != null ? payload.length : 0) +
                '}';
    }
}
