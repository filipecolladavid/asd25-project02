package protocols.abd.timer;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class StartOperationTimer extends ProtoTimer {
    public static final short TIMER_ID = 108;
    public StartOperationTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
