package protocols.agreement.notifications;

import java.util.HashSet;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class DecidedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    public enum DecisionType {
        COMMIT,
        ABORT
    }

    private final DecisionType decisionType;
    private final byte[] operationPayload;
    private final HashSet<Host> membership;

    public DecidedNotification(DecisionType decisionType, byte[] operationPayload, HashSet<Host> membership) {
        super(NOTIFICATION_ID);
        this.decisionType = decisionType;
        this.operationPayload = operationPayload;
        this.membership = membership;
    }

    public DecisionType getDecisionType() {
        return decisionType;
    }

    public byte[] getOperationPayload() {
        return this.operationPayload;
    }

    public HashSet<Host> getMembership() {
        return this.membership;
    }

    @Override
    public String toString() {
        return "DecidedNotification{" +
                ", operationPayload=" + this.operationPayload +
                ", decisionType=" + this.decisionType +
                ", membership=" + this.membership +
                '}';
    }
}
