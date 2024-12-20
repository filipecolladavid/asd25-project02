package protocols.agreement;

import pt.unl.fct.di.novasys.network.data.Host;

class ProposedValue {
    public enum OperationType {
        REGULAR,
        JOIN,
        LEADER_ELECTION
    }

    private final OperationType type;
    private Host host;
    private byte[] value;

    public ProposedValue(OperationType type, Host host, byte[] value) {
        this.type = type;
        this.host = host;
        this.value = value;
    }

    public OperationType getType() {
        return type;
    }

    public Host getHost() {
        return host;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setHost(Host host) {
        this.host = host;
    }
}
