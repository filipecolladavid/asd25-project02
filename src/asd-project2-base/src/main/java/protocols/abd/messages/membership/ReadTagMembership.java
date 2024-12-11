package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTagMembership extends ProtoMessage {

    public final static short MSG_ID = 105;
    private int opSec;

    public ReadTagMembership(int opSec) {
        super(MSG_ID);
        this.opSec = opSec;
    }

    public int getOpSec() {
        return opSec;
    }

    @Override
    public String toString() {
        return "ReadTagMembership={" +
                "opSec="+opSec+
                "}";
    }

    public static ISerializer<ReadTagMembership> serializer = new ISerializer<ReadTagMembership>() {
        @Override
        public void serialize(ReadTagMembership msg, ByteBuf out) {
            out.writeInt(msg.opSec);
        }

        @Override
        public ReadTagMembership deserialize(ByteBuf in) {
            int opSec = in.readInt();
            return new ReadTagMembership(opSec);
        }
    };
}
