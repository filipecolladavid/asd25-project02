package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 * No need to send state, just membership, because writes and reads are always propagated
 */
public class JoinReply extends ProtoMessage {
    public final static short MSG_ID = 104;
    private final HashSet<Host> membership;
    private final Pair<Integer, Host> membershipTag;

    public JoinReply(HashSet<Host> membership, Pair<Integer, Host> membershipTag) {
        super(MSG_ID);
        this.membership = membership;
        this.membershipTag = membershipTag;
    }

    public HashSet<Host> getMembership() {
        return membership;
    }

    public Pair<Integer, Host> getMembershipTag() {
        return membershipTag;
    }

    public static ISerializer<JoinReply> serializer = new ISerializer<JoinReply>() {
        @Override
        public void serialize(JoinReply msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getMembershipTag().getLeft());
            Host.serializer.serialize(msg.getMembershipTag().getRight(), out);
            serializeHashSet(out, msg.getMembership());
        }

        private void serializeHashSet(ByteBuf out, HashSet<Host> membership) throws IOException {
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
            return new JoinReply(membership, membershipTag);
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
