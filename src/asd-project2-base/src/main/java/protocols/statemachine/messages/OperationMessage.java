package protocols.statemachine.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class OperationMessage extends ProtoMessage {
    public final static short MSG_ID = 201;

    private final UUID operationId;
    private final Host requester;
    private final int instanceNumber;
    private final byte[] operation;

    public OperationMessage(UUID operationId, Host requester, int instanceNumber, byte[] operation) {
        super(MSG_ID);
        this.operationId = operationId;
        this.requester = requester;
        this.instanceNumber = instanceNumber;
        this.operation = operation;
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

    public byte[] getOperation() {
        return operation;
    }

    public static ISerializer<OperationMessage> serializer = new ISerializer<OperationMessage>() {
        @Override
        public void serialize(OperationMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.operationId.getMostSignificantBits());
            out.writeLong(msg.operationId.getLeastSignificantBits());

            byte[] addressBytes = msg.requester.getAddress().getAddress();
            out.writeInt(addressBytes.length); // Write length first
            out.writeBytes(addressBytes);
            out.writeInt(msg.requester.getPort());

            out.writeInt(msg.instanceNumber);

            if (msg.operation != null) {
                out.writeInt(msg.operation.length);
                out.writeBytes(msg.operation);
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public OperationMessage deserialize(ByteBuf in) throws IOException {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            UUID operationId = new UUID(mostSigBits, leastSigBits);

            int addrLength = in.readInt();
            if (addrLength <= 0 || addrLength > 16) { // IPv4 or IPv6
                throw new IOException("Invalid address length: " + addrLength);
            }
            byte[] addressBytes = new byte[addrLength];
            in.readBytes(addressBytes);
            int port = in.readInt();
            Host requester = new Host(InetAddress.getByAddress(addressBytes), port);

            int instanceNumber = in.readInt();

            int opLength = in.readInt();
            byte[] operation;
            if (opLength > 0) {
                operation = new byte[opLength];
                in.readBytes(operation);
            } else {
                operation = null;
            }

            return new OperationMessage(operationId, requester, instanceNumber, operation);
        }
    };

    @Override
    public String toString() {
        return "OperationMessage{" +
                "operationId=" + operationId +
                ", requester=" + requester +
                ", instanceNumber=" + instanceNumber +
                ", payloadSize=" + (operation != null ? operation.length : 0) +
                '}';
    }
}
