package protocols.agreement.notificationsbp;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;

public class JoinedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 102;

    private final HashSet<Host> membership;
    private final Host joinInstance;

    public JoinedNotification(HashSet<Host> membership, Host joinInstance) {
        super(NOTIFICATION_ID);
        this.membership = membership;
        this.joinInstance = joinInstance;
    }

    public Host getJoinInstance() {
        return joinInstance;
    }

    public HashSet<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "JoinedNotification{" +
                "membership=" + membership +
                ", joinInstance=" + joinInstance +
                '}';
    }
}
