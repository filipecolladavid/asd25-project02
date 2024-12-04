package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class JoinReply extends ProtoMessage {
    public final static short MSG_ID = 109;
    private final HashSet<Host> members;
    private Map<String, byte[]> val;
    private Map<String, Pair<Integer, Host>> tag;

    public JoinReply(HashSet<Host> members, Map<String, byte[]> val, Map<String, Pair<Integer, Host>> tag) {
        super(MSG_ID);
        this.members = members;
        this.val = val;
        this.tag = tag;
    }

    public HashSet<Host> getMembers() {
        return members;
    }

    public Map<String, byte[]> getVal() {
        return val;
    }

    public Map<String, Pair<Integer, Host>> getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "Transferring State";
    }

    public static ISerializer<JoinReply> serializer = new ISerializer<JoinReply>() {
        @Override
        public void serialize(JoinReply msg, ByteBuf out) throws IOException {
            HashSet<Host> m = msg.getMembers();
            out.writeInt(m.size());
            for(Host h : m) {
                Host.serializer.serialize(h, out);
            }
            out.writeInt(msg.getVal().size());
            for(Map.Entry<String, byte[]> e : msg.getVal().entrySet()) {
                Pair<Integer, Host> p = msg.getTag().get(e.getKey());
                out.writeInt(p.getLeft());
                serializeByteArray(out, e.getValue());
            }

            out.writeInt(msg.getTag().size());
            for(Pair<Integer, Host> p : msg.getTag().values()) {

            }
        }

        private void serializeByteArray(ByteBuf out, byte[] array) {
            if (array == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeInt(array.length);
                out.writeBytes(array);
            }
        }

        @Override
        public JoinReply deserialize(ByteBuf in) throws IOException {
            HashSet<Host> members = new HashSet<>();
            int size = in.readInt();
            for(int i = 0; i < size; i++) {
                Host h = Host.serializer.deserialize(in);
                members.add(h);
            }
            return new JoinReply(members);
        }

        private byte[] deserializeByteArray(ByteBuf in) {
            boolean isNotNull = in.readBoolean();
            if (!isNotNull) {
                return null;
            }
            int length = in.readInt();
            byte[] tmp = new byte[length];
            in.readBytes(tmp);
            return tmp;
        }
    };
}
