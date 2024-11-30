package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class PaxosRequest extends ProtoRequest {
    public static final short REQUEST_ID = 104;

    enum operationType {
        PROPOSE,
        ACCEPT,
        REJECT,
        QUERY,
        COMMIT,
        ABORT
    }

    private final UUID opID;
    private final operationType operation;
    private final int instance;
    private final Host requestedReplica;

    public PaxosRequest(UUID opID, operationType operation, int instance, Host requestedReplica) {
        super(REQUEST_ID);
        this.opID = opID;
        this.operation = operation;
        this.instance = instance;
        this.requestedReplica = requestedReplica;
    }

    public UUID getOpID() {
        return opID;
    }

    public operationType getOperation() {
        return operation;
    }

    public int getInstance() {
        return instance;
    }

    public Host getRequestedReplica() {
        return requestedReplica;
    }

    @Override
    public String toString() {
        return "PaxosRequest{" +
                "opID=" + opID +
                ", operation=" + operation +
                ", instance=" + instance +
                ", requestedReplica=" + requestedReplica +
                '}';
    }
}
