package protocols.statemachine.messages;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinMessage extends ProtoMessage {
    public static final short MSG_ID = 202;

    private final UUID operationId;
    private final Host requester;
    private final List<Host> membershipList;
    private final byte[] currentState;

    public JoinMessage(UUID operationId, Host requester, List<Host> membershipList, byte[] currentState) {
        super(MSG_ID);
        this.operationId = operationId;
        this.requester = requester;
        this.membershipList = membershipList;
        this.currentState = currentState;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public Host getRequester() {
        return requester;
    }

    public List<Host> getMembershipList() {
        return membershipList;
    }

    public byte[] getCurrentState() {
        return currentState;
    }

    public byte[] getOperationPayload() {
        ByteBuf buf = Unpooled.buffer();
        try {
            serializer.serialize(this, buf);
            byte[] operationPayload = new byte[buf.readableBytes()];
            buf.readBytes(operationPayload);
            return operationPayload;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize join request", e);
        } finally {
            buf.release();
        }
    }

    public static final ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) throws IOException {
            // Write operation ID
            out.writeLong(msg.operationId.getMostSignificantBits());
            out.writeLong(msg.operationId.getLeastSignificantBits());

            // Write requester info
            byte[] addressBytes = msg.requester.getAddress().getAddress();
            out.writeInt(addressBytes.length);
            out.writeBytes(addressBytes);
            out.writeInt(msg.requester.getPort());

            // Write membership list
            if (msg.membershipList != null) {
                out.writeInt(msg.membershipList.size());
                for (Host host : msg.membershipList) {
                    byte[] hostAddressBytes = host.getAddress().getAddress();
                    out.writeInt(hostAddressBytes.length);
                    out.writeBytes(hostAddressBytes);
                    out.writeInt(host.getPort());
                }
            } else {
                out.writeInt(0);
            }

            // Write current state
            if (msg.currentState != null) {
                out.writeInt(msg.currentState.length);
                out.writeBytes(msg.currentState);
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
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

            // Read membership list
            List<Host> membershipList = new java.util.ArrayList<>();
            int membershipSize = in.readInt();
            for (int i = 0; i < membershipSize; i++) {
                int hostAddressLength = in.readInt();
                if (hostAddressLength <= 0 || hostAddressLength > 16) {
                    throw new IOException("Invalid host address length: " + hostAddressLength);
                }
                byte[] hostAddressBytes = new byte[hostAddressLength];
                in.readBytes(hostAddressBytes);
                java.net.InetAddress hostAddress = java.net.InetAddress.getByAddress(hostAddressBytes);
                int hostPort = in.readInt();
                if (hostPort < 0 || hostPort > 65535) {
                    throw new IOException("Invalid host port number: " + hostPort);
                }
                membershipList.add(new Host(hostAddress, hostPort));
            }

            // Read current state
            byte[] currentState = null;
            int stateLength = in.readInt();
            if (stateLength > 0) {
                currentState = new byte[stateLength];
                in.readBytes(currentState);
            }

            return new JoinMessage(operationId, requester, membershipList, currentState);
        }
    };

    @Override
    public String toString() {
        return "JoinRequestMessage{" +
                "operationId=" + operationId +
                ", requester=" + requester +
                ", membershipSize=" + (membershipList != null ? membershipList.size() : 0) +
                ", stateSize=" + (currentState != null ? currentState.length : 0) +
                '}';
    }
}
