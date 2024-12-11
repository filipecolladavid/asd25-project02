package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class JoinMessage extends ProtoMessage {
    public final static short MSG_ID = 102;
    private final Host myself;

    public JoinMessage(Host myself) {
        super(MSG_ID);
        this.myself = myself;
    }

    public Host getMyself() {
        return myself;
    }


    @Override
    public String toString() {
        return "JoinMessage: {" +
                "Host=" + myself +
                "}";
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<JoinMessage>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.getMyself(), out);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            Host myself = Host.serializer.deserialize(in);
            return new JoinMessage(myself);
        }
    };
}
