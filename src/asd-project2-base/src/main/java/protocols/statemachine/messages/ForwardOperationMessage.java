package protocols.statemachine.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ForwardOperationMessage extends ProtoMessage {
    public static final short MSG_ID = 203;

    private final OperationMessage operationMessage;
    private final Host sourceNode;

    public ForwardOperationMessage(OperationMessage operationMessage, Host sourceNode) {
        super(MSG_ID);
        this.operationMessage = operationMessage;
        this.sourceNode = sourceNode;
    }

    public OperationMessage getOperationMessage() {
        return operationMessage;
    }

    public Host getSourceNode() {
        return sourceNode;
    }

    public static ISerializer<ForwardOperationMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardOperationMessage msg, ByteBuf out) throws IOException {
            OperationMessage.serializer.serialize(msg.operationMessage, out);
            Host.serializer.serialize(msg.sourceNode, out);
        }

        @Override
        public ForwardOperationMessage deserialize(ByteBuf in) throws IOException {
            OperationMessage message = OperationMessage.serializer.deserialize(in);
            Host source = Host.serializer.deserialize(in);
            return new ForwardOperationMessage(message, source);
        }
    };

    @Override
    public String toString() {
        return "ForwardOperationMessage{" +
                "operationMessage=" + operationMessage +
                ", sourceNode=" + sourceNode +
                '}';
    }
}
