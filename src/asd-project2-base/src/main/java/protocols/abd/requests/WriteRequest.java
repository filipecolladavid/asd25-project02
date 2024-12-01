package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class WriteRequest extends ProtoRequest {
    public static final short REQUEST_ID = 105;

    private final UUID opId;
    private final char[] key;
    private final byte[] data;

    public WriteRequest(UUID opId, char[] key, byte[] data) {
        super(REQUEST_ID);
        this.opId = opId;
        this.key = key;
        this.data = data;
    }

    public byte[] getData() { return data; }

    public UUID getOpId() { return opId; }

    public char[] getKey() { return key; }

    @Override
    public String toString() {
        return "WriteRequest{" +
                "opId=" + opId +
                ", key=" + new String(key) +
                '}';
    }
}
