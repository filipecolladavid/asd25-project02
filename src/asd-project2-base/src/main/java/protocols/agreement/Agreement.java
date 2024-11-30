package protocols.agreement;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import protocols.agreement.messages.Ack;
import protocols.agreement.messages.PaxosMessage;
import protocols.agreement.messages.PaxosMessage.OperationType;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.PaxosRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.messages.OperationMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class Agreement extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(Agreement.class);

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Agreement";

    private HashSet<Host> membership;
    private int channelID;
    private HashMap<UUID, byte[]> operations;

    private final Host self;

    public Agreement(Properties props)
            throws HandlerRegistrationException, NumberFormatException, UnknownHostException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);

        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));
        this.membership = new HashSet<Host>();
        this.operations = new HashMap<UUID, byte[]>();

        this.membership.add(this.self);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposedRequest);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplicaRequest);

        /*--------------------- Register Paxos Handlers ----------------------------- */
        registerRequestHandler(PaxosRequest.REQUEST_ID, this::uponPaxosRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
    }

    public void uponProposedRequest(ProposeRequest request, short sourceProto) {
        try {
            byte[] operation = request.getOperation();
            ByteBuf operationBuf = Unpooled.wrappedBuffer(operation);
            OperationMessage operationMessage = OperationMessage.serializer.deserialize(operationBuf);
            this.operations.put(operationMessage.getOperationId(), operationMessage.getOperationPayload());

            if (this.membership.size() < 3) {
                if (operationMessage.getOperationType() == OperationMessage.OperationType.JOIN_REQUEST) {
                    this.addReplicaHandler(operationMessage);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to deserialize operation message", e);
            e.printStackTrace();
        }
    }

    public void addReplicaHandler(OperationMessage operationMessage) {
        this.membership.add(operationMessage.getOperationRequester());
        DecidedNotification decidedNotification = new DecidedNotification(DecidedNotification.DecisionType.COMMIT,
                operationMessage.getOperationPayload(), this.membership);

        triggerNotification(decidedNotification);
        membership.forEach(replica -> {
            if (replica.equals(this.self)) {
                return;
            }
            logger.info("Self[{}] Sending Membership List {} ", this.self, this.membership);
            PaxosMessage decidedPaxosMessage = new PaxosMessage(
                    OperationType.COMMIT, operationMessage.getOperationPayload(), this.membership);
            this.openConnection(replica);
            sendMessage(decidedPaxosMessage, replica);
        });
    }

    public void uponRemoveReplicaRequest(RemoveReplicaRequest request, short sourceProto) {
        // logger.info("Received Remove Replica Request");
    }

    public void uponPaxosRequest(PaxosRequest request, short sourceProto) {
        // logger.info("Received Paxos Request");
    }

    private void uponPaxosMessage(PaxosMessage msg, Host host, short souceProto, int channelId) {
        if (msg.getOperation() == PaxosMessage.OperationType.COMMIT) {
            byte[] messagedCommited = msg.getOperationPayload();

            logger.info("Self:[{}] Paxos Message Membership Received -> {}", self, msg.getMembership());

            msg.getMembership().forEach(member -> {
                if (!member.equals(self) && !this.membership.contains(member)) {
                    this.membership.add(member);
                }
            });
            DecidedNotification decidedNotification = new DecidedNotification(DecidedNotification.DecisionType.COMMIT,
                    messagedCommited, msg.getMembership());

            triggerNotification(decidedNotification);

            logger.info("Self:[{}] Paxos Message Membership -> {}", self, this.membership);
        }
    }

    private void uponMsgFail(PaxosMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        this.channelID = notification.getChannelId();

        registerSharedChannel(this.channelID);
        try {
            /*--------------------- Register Message Handlers ----------------------------- */
            registerMessageHandler(this.channelID, PaxosMessage.MESSAGE_ID, this::uponPaxosMessage, this::uponMsgFail);
            registerMessageHandler(this.channelID, Ack.MSG_ID, this::uponAck, this::uponAckFail);
            registerMessageSerializer(this.channelID, PaxosMessage.MESSAGE_ID, PaxosMessage.serializer);
            registerMessageSerializer(this.channelID, Ack.MSG_ID, Ack.serializer);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    private void uponAck(Ack msg, Host host, short souceProto, int channelId) {
        // logger.info("Received Ack");
    }

    private void uponAckFail(Ack msg, Host host, short destProto, Throwable throwable, int channelId) {
        // logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
}
