package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AddReplicaMessage extends ProtoMessage {

    public final static short MSG_ID = 104;
    private final int instance;
    private final Host replica;

    public AddReplicaMessage(int instance, Host replica) {
        super(MSG_ID);
        this.instance = instance;
        this.replica = replica;
    }

    public Host getReplica() { return replica; }
    public int getInstance() { return instance; }

    @Override
    public String toString() {
        return "ReadTag";
    }

    public static ISerializer<AddReplicaMessage> serializer = new ISerializer<AddReplicaMessage>() {
        @Override
        public void serialize(AddReplicaMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getInstance());
            Host.serializer.serialize(msg.getReplica(), out);
        }

        @Override
        public AddReplicaMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            Host replica = Host.serializer.deserialize(in);
            return new AddReplicaMessage(instance, replica);
        }
    };
}
