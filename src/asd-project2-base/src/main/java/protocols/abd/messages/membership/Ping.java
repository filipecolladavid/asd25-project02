package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Used to verify that Node is alive
 */
public class Ping extends ProtoMessage {
    public static final short MSG_ID = 105;

    public Ping() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "Ping Message";
    }

    public static ISerializer<Ping> serializer = new ISerializer<>() {

        @Override
        public void serialize(Ping ping, ByteBuf out) {
        }

        @Override
        public Ping deserialize(ByteBuf in) {
            return new Ping();

        }
    };
}