package protocols.agreement.notifications;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NodeDecidedNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 102;

    public enum DecisionType {
        COMMIT,
        ABORT
    }

    public enum OperationType {
        JOIN,
        LEAVE
    }

    private final UUID operationID;
    private final OperationType OperationType;
    private final DecisionType decisionType;
    private final Host joiningNode;
    private final int instance;

    public NodeDecidedNotification(UUID operationID, OperationType OperationType, DecisionType decisionType,
            Host joiningNode,
            int instance, byte[] operationPayload) {
        super(NOTIFICATION_ID);
        this.operationID = operationID;
        this.OperationType = OperationType;
        this.decisionType = decisionType;
        this.joiningNode = joiningNode;
        this.instance = instance;
    }

    public UUID getOperationID() {
        return operationID;
    }

    public OperationType getOperationType() {
        return OperationType;
    }

    public DecisionType getDecisionType() {
        return decisionType;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public int getInstance() {
        return instance;
    }

    @Override
    public String toString() {
        return "NodeDecidedNotification{" +
                "operationID=" + operationID +
                "operationType=" + OperationType +
                "decisionType=" + decisionType +
                ", joiningNode=" + joiningNode +
                ", instance=" + instance +
                '}';
    }
}
