package protocols.abd.operation;

import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.HashSet;
import java.util.UUID;

public class ReadWriteOperation extends Operation {

    Pair<UUID, byte[]> pending;

    public ReadWriteOperation(ProtoRequest request, int opSeq) {
        super(request, opSeq);
        this.answersReadReply = new HashSet<>();
        this.answersAck = new HashSet<>();
        this.answersReadTag = new HashSet<>();
        this.request = request;
        this.pending = null;
        this.opSeq = opSeq;
    }

    public Pair<UUID, byte[]> getPending() {
        return pending;
    }

    public void setPending(Pair<UUID, byte[]> pending) {
        this.pending = pending;
    }
}
