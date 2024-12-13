package protocols.abd.timer;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class StatusTimer extends ProtoTimer {
    public static final short TIMER_ID = 103;
    public StatusTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
