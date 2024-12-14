package protocols.statemachine.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ExecutionPendingOperationTimer extends ProtoTimer {
    public static final short TIMER_ID = 102;

    public ExecutionPendingOperationTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
