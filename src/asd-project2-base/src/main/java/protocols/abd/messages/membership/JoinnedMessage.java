package protocols.abd.messages.membership;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class JoinnedMessage extends ProtoMessage {
    public final static short MSG_ID = 103;
    private final int instance;


    private final UUID opID;

    public JoinnedMessage(int instance, UUID opID) {
        super(MSG_ID);
        this.instance = instance;
        this.opID = opID;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpID() {
        return opID;
    }

    public static ISerializer<JoinnedMessage> serializer = new ISerializer<JoinnedMessage>() {
        @Override
        public void serialize(JoinnedMessage msg, ByteBuf out) {
            out.writeInt(msg.getInstance());
            serializeCharArray(out, msg.getOpID().toString().toCharArray());
        }

        private void serializeCharArray(ByteBuf out, char[] array) {
            out.writeInt(array.length);
            for(char c : array) {
                out.writeChar(c);
            }
        }

        @Override
        public JoinnedMessage deserialize(ByteBuf in) {
            int opSeq = in.readInt();
            UUID id = UUID.fromString(new String(deserializeCharArray(in)));
            return new JoinnedMessage(opSeq, id);
        }

        private char[] deserializeCharArray(ByteBuf in) {
            int length = in.readInt();
            char[] array = new char[length];
            for(int i = 0; i < length; i++) {
                array[i] = in.readChar();
            }
            return array;
        }
    };
}
