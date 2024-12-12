package protocols.agreement;

import pt.unl.fct.di.novasys.network.data.Host;

class ProposedValue {
    public enum OperationType {
        REGULAR,
        JOIN
    }

    private final OperationType type;
    private Host joiningNode;
    private byte[] value;

    public ProposedValue(OperationType type, Host joiningNode, byte[] value) {
        this.type = type;
        this.joiningNode = joiningNode;
        this.value = value;
    }

    public OperationType getType() {
        return type;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setJoiningNode(Host joiningNode) {
        this.joiningNode = joiningNode;
    }
}
