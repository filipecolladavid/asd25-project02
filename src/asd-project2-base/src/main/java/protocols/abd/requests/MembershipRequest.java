package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class MembershipRequest extends ProtoRequest {

    public static final short REQUEST_ID = 103;

    private final Host replica;

    public MembershipRequest(Host replica) {
        super(REQUEST_ID);
        this.replica = replica;
    }

    public Host getReplica() {
    	return replica;
    }
   

    @Override
    public String toString() {
        return "AddReplicaRequest{" +
                ", replica=" + replica +
                '}';
    }
}
