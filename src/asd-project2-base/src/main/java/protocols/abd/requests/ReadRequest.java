package protocols.abd.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class ReadRequest extends ProtoRequest {
    public static final short REQUEST_ID = 103;

    private final UUID opId;
    private final char[] key;
    public ReadRequest(UUID opId, char[] key) {
        super(REQUEST_ID);
        this.opId = opId;
        this.key = key;
    }

    public char[] getKey() {
        return key;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "ReadRequest{" +
                "opId=" + opId +
                ", key=" + new String(key) +
                '}';
    }
}
