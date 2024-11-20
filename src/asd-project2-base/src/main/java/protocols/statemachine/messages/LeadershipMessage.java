package protocols.statemachine.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class LeadershipMessage extends ProtoMessage {
    public static final short MSG_ID = 204;

    public enum Type {
        PREPARE,
        PREPARE_OK,
        HEARTBEAT,
    }

    private final Type type;
    private final int proposalNumber;
    private final Host sender;
    private final int lastInstanceSeen;

    public LeadershipMessage(Type type, int proposalNumber, Host sender, int lastInstanceSeen) {
        super(MSG_ID);
        this.type = type;
        this.proposalNumber = proposalNumber;
        this.sender = sender;
        this.lastInstanceSeen = lastInstanceSeen;
    }

    public Type getType() {
        return type;
    }

    public int getProposalNumber() {
        return proposalNumber;
    }

    public Host getSender() {
        return sender;
    }

    public int getLastInstanceSeen() {
        return lastInstanceSeen;
    }

    public static ISerializer<LeadershipMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(LeadershipMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.type.ordinal());
            out.writeInt(msg.proposalNumber);
            Host.serializer.serialize(msg.sender, out);
            out.writeInt(msg.lastInstanceSeen);
        };

        @Override
        public LeadershipMessage deserialize(ByteBuf in) throws IOException {
            Type type = Type.values()[in.readInt()];
            int proposalNumber = in.readInt();
            Host sender = Host.serializer.deserialize(in);
            int lastInstanceSeen = in.readInt();

            return new LeadershipMessage(type, proposalNumber, sender, lastInstanceSeen);
        }
    };

    @Override
    public String toString() {
        return "LeadershipMessage{" +
                "type=" + type +
                ", proposalNumber=" + proposalNumber +
                ", sender=" + sender +
                ", lastInstanceSeen=" + lastInstanceSeen +
                '}';
    }
}
