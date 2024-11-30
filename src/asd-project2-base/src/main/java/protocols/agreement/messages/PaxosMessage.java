package protocols.agreement.messages;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.UUID;

import org.apache.commons.codec.binary.Hex;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class PaxosMessage extends ProtoMessage {
    public static final short MESSAGE_ID = 101;

    public enum OperationType {
        PROPOSE,
        ACCEPT,
        REJECT,
        QUERY,
        COMMIT,
        ABORT
    }

    private final OperationType operation;
    private final byte[] operationPayload;
    private final HashSet<Host> membership;

    public PaxosMessage(OperationType operation, byte[] operationPayload, HashSet<Host> membership) {
        super(MESSAGE_ID);
        this.operation = operation;
        this.operationPayload = operationPayload;
        this.membership = membership;
    }

    public OperationType getOperation() {
        return operation;
    }

    public byte[] getOperationPayload() {
        return operationPayload;
    }

    public HashSet<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "PaxosMessage{" +
                ", operation=" + operation +
                ", operationPayload=" + Hex.encodeHexString(operationPayload) +
                ", membership=" + membership +
                '}';
    }

    public static ISerializer<PaxosMessage> serializer = new ISerializer<PaxosMessage>() {
        @Override
        public void serialize(PaxosMessage msg, ByteBuf out) {
            out.writeInt(msg.operation.ordinal());
            serializeByteArray(out, msg.getOperationPayload());
            out.writeInt(msg.membership.size());
            for (Host host : msg.membership) {
                byte[] address = host.getAddress().getAddress();
                out.writeInt(address.length);
                out.writeBytes(address);
                out.writeInt(host.getPort());
            }
        }

        @Override
        public PaxosMessage deserialize(ByteBuf in) {
            OperationType operation = OperationType.values()[in.readInt()];
            byte[] operationPayload = deserializeByteArray(in);
            int membershipSize = in.readInt();
            HashSet<Host> membership = new HashSet<>();
            for (int i = 0; i < membershipSize; i++) {
                int addressLength = in.readInt();
                byte[] address = new byte[addressLength];
                in.readBytes(address);
                int port = in.readInt();

                try {
                    Host host = new Host(InetAddress.getByAddress(address), port);
                    membership.add(host);
                } catch (UnknownHostException e) {
                    throw new RuntimeException("Failed to deserialize host address", e);
                }
            }

            return new PaxosMessage(operation, operationPayload, membership);
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
