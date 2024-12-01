package protocols.agreement.requests;

import java.util.UUID;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinRequest extends ProtoRequest {
    public static final short REQUEST_ID = 102;

    private final int instance;
    private final UUID operationId;
    private final Host joiningNode;

    public JoinRequest(int instance, UUID operationId, Host joiningNode) {
        super(REQUEST_ID);
        this.instance = instance;
        this.operationId = operationId;
        this.joiningNode = joiningNode;
    }

    public int getInstance() {
        return instance;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public UUID getOperationId() {
        return operationId;
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
                "instance=" + instance +
                ", operationId=" + operationId +
                ", joiningNode=" + joiningNode +
                '}';
    }

}
