package protocols.abd.operation;

import org.apache.commons.lang3.tuple.Pair;
import protocols.abd.messages.membership.Action;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class MembershipOperation extends Operation {

    Pair<UUID, Host> pending;
    private final Action action;

    public MembershipOperation(ProtoRequest request, int opSeq, UUID id, Host replica, Action action) {
        super(request, opSeq);
        this.action = action;
        this.answersAck = new HashSet<>();
        this.answersReadTag = new HashSet<>();
        this.request = request;
        this.pending = Pair.of(id, replica);
    }

    public Pair<UUID, Host> getPending() {
        return pending;
    }

    public void setPending(Pair<UUID, Host> pending) {
        this.pending = pending;
    }

    public Action getAction() {
        return this.action;
    }
}
