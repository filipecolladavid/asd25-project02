package protocols.agreement.notifications;

import java.util.LinkedList;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class DecidedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    public enum DecisionType {
        COMMIT,
        ABORT
    }

    private final UUID operationId;
    private final DecisionType decisionType;
    private final byte[] operationPayload;
    private final LinkedList<Host> membership;

    public DecidedNotification(UUID operationId, DecisionType decisionType, LinkedList<Host> membership,
            byte[] operationPayload) {
        super(NOTIFICATION_ID);
        this.operationId = operationId;
        this.decisionType = decisionType;
        this.operationPayload = operationPayload;
        this.membership = membership;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public DecisionType getDecisionType() {
        return decisionType;
    }

    public byte[] getOperationPayload() {
        return this.operationPayload;
    }

    public LinkedList<Host> getMembership() {
        return this.membership;
    }

    @Override
    public String toString() {
        return "DecidedNotification{" +
                "operationId=" + this.operationId +
                ", operationPayload=" + this.operationPayload +
                ", decisionType=" + this.decisionType +
                ", membership=" + this.membership +
                '}';
    }
}
