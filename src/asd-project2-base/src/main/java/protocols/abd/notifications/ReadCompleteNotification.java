package protocols.abd.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.Arrays;
import java.util.UUID;

public class ReadCompleteNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 205;
    private final UUID opId;
    private final byte[] value;
    private final char[] key;
    public ReadCompleteNotification(char[] key, byte[] value, UUID opID) {
        super(NOTIFICATION_ID);
        this.opId = opID;
        this.value = value;
        this.key = key;
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
        return "ReadCompleteNotification{" +
                "OpId=" + opId +
                ", key='" + new String(key) + '\'' +
                ", value='" + new String(value) + '\'' +
                '}';
    }
}
