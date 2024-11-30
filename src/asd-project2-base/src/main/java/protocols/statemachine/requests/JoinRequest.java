package protocols.statemachine.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class JoinRequest extends ProtoRequest {

    public static final short REQUEST_ID = 202;

    private final UUID opId;
    private final Host replicaHost;

    public JoinRequest(UUID opId, Host replicaHost) {
        super(REQUEST_ID);
        this.opId = opId;
        this.replicaHost = replicaHost;
    }

    public Host getReplicaHost() {
        return replicaHost;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "OrderRequest{" +
                "opId=" + opId +
                ", replicaHost=" + replicaHost +
                '}';
    }
}
