package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class JoinnedMessage extends ProtoMessage {
    public final static short MSG_ID = 103;
    private final int instance;

    public JoinnedMessage(int instance) {
        super(MSG_ID);
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    public static ISerializer<JoinnedMessage> serializer = new ISerializer<JoinnedMessage>() {
        @Override
        public void serialize(JoinnedMessage msg, ByteBuf out) {
            out.writeInt(msg.getInstance());
        }

        @Override
        public JoinnedMessage deserialize(ByteBuf in) {
            int opSeq = in.readInt();
            return new JoinnedMessage(opSeq);
        }
    };
}
