package protocols.agreement.requests;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class AddReplicaRequest extends ProtoRequest {

    public static final short REQUEST_ID = 103;

    private final int instance;
    private final UUID opID;
    private final Host replica;

    public AddReplicaRequest(int instance, UUID opID, Host replica) {
        super(REQUEST_ID);
        this.opID = opID;
        this.instance = instance;
        this.replica = replica;
    }

    public UUID getOpID() {
        return opID;
    }

    public int getInstance() {
        return instance;
    }

    public Host getReplica() {
        return replica;
    }

    @Override
    public String toString() {
        return "ProposeRequest{" +
                "instance=" + instance +
                ", opID=" + opID +
                ", replica=" + replica +
                '}';
    }
}
