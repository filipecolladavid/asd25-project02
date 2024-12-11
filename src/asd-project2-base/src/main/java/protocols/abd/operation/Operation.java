package protocols.abd.operation;

import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Set;

public abstract class Operation {
    Set<ProtoMessage> answersReadReply;
    Set<Host> answersAck;
    HashSet<Pair<Integer, Host>> answersReadTag;
    ProtoRequest request;
    int opSeq;

    public Operation(ProtoRequest request, int opSeq){
        this.answersReadReply = new HashSet<>();
        this.answersAck = new HashSet<>();
        this.answersReadTag = new HashSet<>();
        this.request = request;
        this.opSeq = opSeq;
    }

    public void incrementOpSeq() {
        this.opSeq++;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public void setOpSeq(int opSeq) {
        this.opSeq = opSeq;
    }

    public ProtoRequest getRequest() {
        return request;
    }

    public void setRequest(ProtoRequest request) {
        this.request = request;
    }

    public HashSet<Pair<Integer, Host>> getAnswersReadTag() {
        return answersReadTag;
    }

    public void setAnswersReadTag(HashSet<Pair<Integer, Host>> answersReadTag) {
        this.answersReadTag = answersReadTag;
    }

    public Set<Host> getAnswersAck() {
        return answersAck;
    }

    public void setAnswersAck(Set<Host> answersAck) {
        this.answersAck = answersAck;
    }
}