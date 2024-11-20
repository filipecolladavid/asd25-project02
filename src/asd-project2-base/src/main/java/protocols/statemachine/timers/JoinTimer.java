package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class JoinTimer extends ProtoTimer {
    public static final short TIMER_ID = 101;

    public JoinTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
