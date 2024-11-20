package protocols.statemachine.messages;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.ISerializer;

// Message that is sent to the leader to join the system
public class JoinMessage extends ProtoMessage {
    public static final short MSG_ID = 202;

    public enum JoinType {
        REQUEST,
        RESPONSE
    }

    private final JoinType joinType;
    private final Host joiningNode;
    private final List<Host> currentMembers;
    private final int currentInstance;
    private final Map<String, byte[]> currentState;

    public JoinMessage(JoinType joinType, Host joiningNode) {
        this(joinType, joiningNode, null, 0, null);
    }

    public JoinMessage(JoinType joinType, Host joiningNode, List<Host> currentMembers, int currentInstance,
            Map<String, byte[]> currentState) {
        super(MSG_ID);
        this.joinType = joinType;
        this.joiningNode = joiningNode;
        this.currentMembers = currentMembers;
        this.currentInstance = currentInstance;
        this.currentState = currentState;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public List<Host> getCurrentMembers() {
        return currentMembers;
    }

    public int getCurrentInstance() {
        return currentInstance;
    }

    public Map<String, byte[]> getCurrentState() {
        return currentState;
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage joinMessage, ByteBuf out) throws IOException {
            out.writeInt(joinMessage.joinType.ordinal());
            Host.serializer.serialize(joinMessage.joiningNode, out);

            if (joinMessage.joinType == JoinType.RESPONSE) {
                out.writeInt(joinMessage.currentMembers.size());

                for (Host host : joinMessage.currentMembers)
                    Host.serializer.serialize(host, out);

                out.writeInt(joinMessage.currentInstance);
                out.writeInt(joinMessage.currentState.size());

                for (Map.Entry<String, byte[]> entry : joinMessage.currentState.entrySet()) {
                    byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(keyBytes.length);
                    out.writeBytes(keyBytes);

                    out.writeInt(entry.getValue().length);
                    out.writeBytes(entry.getValue());
                }
            }
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            JoinType joinType = JoinType.values()[in.readInt()];
            Host joiningNode = Host.serializer.deserialize(in);

            if (joinType == JoinType.REQUEST) {
                return new JoinMessage(joinType, joiningNode);
            }

            List<Host> members = new LinkedList<>();
            int membersSize = in.readInt();

            for (int i = 0; i < membersSize; i++)
                members.add(Host.serializer.deserialize(in));

            int currentInstance = in.readInt();
            Map<String, byte[]> currentState = new HashMap<>();
            int currentStateSize = in.readInt();
            for (int i = 0; i < currentStateSize; i++) {
                byte[] keyBytes = new byte[in.readInt()];
                in.readBytes(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valueSize = in.readInt();
                byte[] value = new byte[valueSize];
                in.readBytes(value);
                currentState.put(key, value);
            }

            return new JoinMessage(joinType, joiningNode, members, currentInstance, currentState);
        }
    };

    @Override
    public String toString() {
        return "JoinMessage{" +
                "joinType=" + joinType +
                ", joiningNode=" + joiningNode +
                ", currentMembers=" + currentMembers +
                ", currentInstance=" + currentInstance +
                ", currentState=" + currentState +
                '}';
    }
}
