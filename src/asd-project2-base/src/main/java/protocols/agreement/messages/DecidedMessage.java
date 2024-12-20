package protocols.agreement.messages;

import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class DecidedMessage extends ProtoMessage {
    public static final short MESSAGE_ID = 107;

    private final UUID operationID;
    private LinkedList<Host> membership;
    private byte[] operationPayload;

    public DecidedMessage(UUID operationID, LinkedList<Host> membership, byte[] operationPayload) {
        super(MESSAGE_ID);
        this.operationID = operationID;
        this.membership = membership;
        this.operationPayload = operationPayload;
    }

    public UUID getOperationID() {
        return operationID;
    }

    public LinkedList<Host> getMembership() {
        return membership;
    }

    public byte[] getOperationPayload() {
        return operationPayload;
    }

    @Override
    public String toString() {
        return "DecidedMessage{" +
                "operationID=" + operationID +
                ", membership=" + membership +
                ", operationPayload=" + operationPayload +
                '}';
    }

    public static final ISerializer<DecidedMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DecidedMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.operationID.getMostSignificantBits());
            out.writeLong(msg.operationID.getLeastSignificantBits());
            if (msg.membership != null) {
                out.writeInt(msg.membership.size());
                for (Host host : msg.membership) {
                    Host.serializer.serialize(host, out);
                }
            } else {
                out.writeInt(0);
            }

            if (msg.operationPayload != null) {
                out.writeInt(msg.operationPayload.length);
                out.writeBytes(msg.operationPayload);
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public DecidedMessage deserialize(ByteBuf in) throws IOException {
            if (in.readableBytes() < 24) {
                throw new IOException("Buffer too small");
            }
            UUID operationId = new UUID(in.readLong(), in.readLong());
            int membershipSize = in.readInt();
            LinkedList<Host> membership = new LinkedList<>();
            for (int i = 0; i < membershipSize; i++) {
                membership.add(Host.serializer.deserialize(in));
            }
            int operationPayloadSize = in.readInt();
            byte[] operationPayload = new byte[operationPayloadSize];
            in.readBytes(operationPayload);

            return new DecidedMessage(operationId, membership, operationPayload);
        }
    };
}
