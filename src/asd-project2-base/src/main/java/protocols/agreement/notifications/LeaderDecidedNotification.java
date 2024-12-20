package protocols.agreement.notifications;

import java.util.LinkedList;
import java.util.UUID;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class LeaderDecidedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 103;

    public enum DecisionType {
        COMMIT,
        ABORT
    }

    private final UUID operationId;
    private final DecisionType decisionType;
    private final Host newLeaderHost;
    private final LinkedList<Host> membership;

    public LeaderDecidedNotification(UUID operationId, DecisionType decisionType, LinkedList<Host> membership,
            Host newLeaderHost) {
        super(NOTIFICATION_ID);
        this.operationId = operationId;
        this.decisionType = decisionType;
        this.newLeaderHost = newLeaderHost;
        this.membership = membership;
    }

    public UUID getOperationId() {
        return operationId;
    }

    public DecisionType getDecisionType() {
        return decisionType;
    }

    public Host getNewLeaderHost() {
        return this.newLeaderHost;
    }

    public LinkedList<Host> getMembership() {
        return this.membership;
    }

    @Override
    public String toString() {
        return "DecidedNotification{" +
                "operationId=" + this.operationId +
                ", newLeaderHost=" + this.newLeaderHost +
                ", decisionType=" + this.decisionType +
                ", membership=" + this.membership +
                '}';
    }
}
