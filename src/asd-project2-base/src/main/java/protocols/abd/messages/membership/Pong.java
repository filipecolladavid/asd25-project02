package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Used to verify that Node is alive
 */
public class Pong extends ProtoMessage {
    public static final short MSG_ID = 106;

    public Pong() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "Pong";
    }

    public static ISerializer<Pong> serializer = new ISerializer<>() {

        @Override
        public void serialize(Pong ping, ByteBuf out) {
            out.writeBytes(new byte[0]);
        }

        @Override
        public Pong deserialize(ByteBuf in) {
            return new Pong();

        }
    };
}