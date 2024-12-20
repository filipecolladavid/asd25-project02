package protocols.abd.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class UpdateValueNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 206;
    private final char[] key;
    private final byte[] value;
    public UpdateValueNotification(char[] key, byte[] value) {
        super(NOTIFICATION_ID);
        this.key = key;
        this.value = value;
    }

    public char[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ReadCompleteNotification{" +
                ", key='" + new String(key) + '\'' +
                ", value='" + new String(value) + '\'' +
                '}';
    }
}
