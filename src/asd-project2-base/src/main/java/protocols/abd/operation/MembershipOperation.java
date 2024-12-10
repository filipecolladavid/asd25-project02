package protocols.abd.operation;

import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Set;
import java.util.UUID;

public class MembershipOperation extends Operation {

    Pair<UUID, Set<Host>> pending;

    public MembershipOperation(ProtoRequest request, int opSeq) {
        super(request, opSeq);
    }
}
