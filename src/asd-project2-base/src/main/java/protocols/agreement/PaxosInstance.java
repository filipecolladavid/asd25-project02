package protocols.agreement;

import java.util.HashMap;
import java.util.UUID;
import java.util.List;

import pt.unl.fct.di.novasys.network.data.Host;

class PaxosInstance {
    public enum InstanceType {
        REGULAR,
        JOIN
    }

    UUID id;
    UUID operationId;
    int ballot;
    HashMap<Host, Boolean> promises;
    HashMap<Host, Boolean> accepts;
    int promisesFailureCounter;
    int acceptsFailureCounter;
    boolean isDecided;
    InstanceType instanceType;
    List<Host> members;
    Host joiningNode;
    ProposedValue proposedValue;

    public PaxosInstance(UUID operationId, int ballot, List<Host> agreementMembers, InstanceType instanceType) {
        this.id = UUID.randomUUID();
        this.ballot = ballot;
        this.operationId = operationId;
        this.promises = new HashMap<Host, Boolean>();
        this.accepts = new HashMap<Host, Boolean>();
        this.promisesFailureCounter = 0;
        this.acceptsFailureCounter = 0;
        this.isDecided = false;
        this.instanceType = instanceType;
        this.members = agreementMembers;
        this.joiningNode = null;
    }

    public UUID getId() {
        return this.id;
    }

    public UUID getOperationId() {
        return this.operationId;
    }

    public int getBallot() {
        return this.ballot;
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public HashMap<Host, Boolean> getPromises() {
        return this.promises;
    }

    public void setPromise(Host host, boolean promise) {
        this.promises.put(host, promise);
    }

    public void setAccept(Host host, boolean accept) {
        this.accepts.put(host, accept);
    }

    public HashMap<Host, Boolean> getAccepts() {
        return this.accepts;
    }

    public InstanceType getInstanceType() {
        return this.instanceType;
    }

    public int getPromisesFailureCounter() {
        return this.promisesFailureCounter;
    }

    public void setPromisesFailureCounter() {
        this.promisesFailureCounter++;
    }

    public int getAcceptsFailureCounter() {
        return this.acceptsFailureCounter;
    }

    public void setAcceptsFailureCounter() {
        this.acceptsFailureCounter++;
    }

    public Host getJoiningNode() {
        return this.joiningNode;
    }

    public void setJoiningNode(Host joiningNode) {
        this.joiningNode = joiningNode;
    }

    public void setProposedValue(ProposedValue value) {
        this.proposedValue = value;
    }

    public ProposedValue getProposedValue() {
        return this.proposedValue;
    }

}
