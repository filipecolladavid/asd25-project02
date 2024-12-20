package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class HeartbeatMessage extends ProtoMessage {
    public static final short MESSAGE_ID = 105;

    public HeartbeatMessage() {
        super(MESSAGE_ID);
    }

    @Override
    public String toString() {
        return "HeartbeatMessage{}";
    }

    public static ISerializer<HeartbeatMessage> serializer = new ISerializer<HeartbeatMessage>() {
        @Override
        public void serialize(HeartbeatMessage msg, ByteBuf out) {
        }

        @Override
        public HeartbeatMessage deserialize(ByteBuf in) {
            return new HeartbeatMessage();
        }
    };
}
