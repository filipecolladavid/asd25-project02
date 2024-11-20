package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTag extends ProtoMessage {

    public final static short MSG_ID = 101;

    public ReadTag() {
        super(MSG_ID);
    }

    @Override
    public String toString() {
        return "ReadTag";
    }

    public static ISerializer<ReadTag> serializer = new ISerializer<ReadTag>() {
        @Override
        public void serialize(ReadTag msg, ByteBuf out) {
        }

        @Override
        public ReadTag deserialize(ByteBuf in) {
            return new ReadTag();
        }
    };
}
