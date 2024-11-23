package protocols.abd.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.UUID;

public class WriteCompleteNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 207;
    private final UUID opId;
    private final char[] key;
    private final byte[] value;


    public WriteCompleteNotification(UUID opId, char[] key, byte[] value) {
        super(NOTIFICATION_ID);
        this.opId = opId;
        this.key = key;
        this.value = value;
    }

    public char[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "WriteCompleteNotification{" +
                "opId=" + opId +
                ", key='" + new String(key) + '\'' +
                ", value='" + new String(value) + '\'' +
                '}';
    }
}
