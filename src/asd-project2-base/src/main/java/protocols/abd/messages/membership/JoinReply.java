package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * No need to send state, just membership, because writes and reads are always propagated
 */
public class JoinReply extends ProtoMessage {
    public final static short MSG_ID = 104;
    private final Set<Host> membership;
    private final Pair<Integer, Host> membershipTag;
    private final UUID opID;

    public JoinReply(Set<Host> membership, Pair<Integer, Host> membershipTag, UUID opID) {
        super(MSG_ID);
        this.membership = membership;
        this.membershipTag = membershipTag;
        this.opID = opID;
    }

    public Set<Host> getMembership() {
        return membership;
    }

    public Pair<Integer, Host> getMembershipTag() {
        return membershipTag;
    }

    public UUID getOpID() {
        return opID;
    }

    public static ISerializer<JoinReply> serializer = new ISerializer<JoinReply>() {
        @Override
        public void serialize(JoinReply msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getMembershipTag().getLeft());
            Host.serializer.serialize(msg.getMembershipTag().getRight(), out);
            serializeHashSet(out, msg.getMembership());
            serializeCharArray(out, msg.getOpID().toString().toCharArray());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        private void serializeHashSet(ByteBuf out, Set<Host> membership) throws IOException {
            out.writeInt(membership.size());
            for (Host member : membership) {
                Host.serializer.serialize(member, out);
            }
        }

        @Override
        public JoinReply deserialize(ByteBuf in) throws IOException {
            int membershipTagCounter = in.readInt();
            Host membershipTagHost = Host.serializer.deserialize(in);
            HashSet<Host> membership = deserializeHashSet(in);
            Pair<Integer, Host> membershipTag = Pair.of(membershipTagCounter, membershipTagHost);
            UUID id = UUID.fromString(new String(deserializeCharArray(in)));
            return new JoinReply(membership, membershipTag, id);
        }

        private char[] deserializeCharArray(ByteBuf in) {
            int length = in.readInt();
            char[] array = new char[length];
            for(int i = 0; i < length; i++) {
                array[i] = in.readChar();
            }
            return array;
        }

        private HashSet<Host> deserializeHashSet(ByteBuf in) throws IOException {
            HashSet<Host> membership = new HashSet<>();
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                Host h = Host.serializer.deserialize(in);
                membership.add(h);
            }
            return membership;
        }
    };
}

