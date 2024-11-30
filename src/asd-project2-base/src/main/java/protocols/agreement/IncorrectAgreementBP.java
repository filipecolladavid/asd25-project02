package protocols.agreement;

import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.requestsbp.JoinRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.messagesbp.BroadcastMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notificationsbp.JoinedNotification;
import protocols.agreement.requests.ProposeRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * This is NOT a correct agreement protocol (it is actually a VERY wrong one)
 * This is simply an example of things you can do, and can be used as a starting
 * point.
 *
 * You are free to change/delete ANYTHING in this class, including its fields.
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class IncorrectAgreementBP extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(IncorrectAgreementBP.class);

    // Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "EmptyAgreement";

    private Host myself;
    private int joinedInstance;
    private HashSet<Host> membership;

    public IncorrectAgreementBP(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; // -1 means we have not yet joined the system
        membership = new HashSet<>();

        /*--------------------- Register Timer Handlers ----------------------------- */

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(JoinRequest.REQUEST_ID, this::uponJoinRequest);
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
    }

    @Override
    public void init(Properties props) {
        // logger.info("[Agreement] - Init");
        // Nothing to do here, we just wait for events from the application or agreement
    }

    // Upon receiving the channelId from the membership, register our own callbacks
    // and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        // logger.info("[Agreement] - Upon Channel Created");
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        // logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short souceProto, int channelId) {
        logger.info("[Agreement] - Upon BroadcastMessage");
        // logger.info("[Agreement] - Upon BroadcastMessage");
        if (joinedInstance >= 0) {
            // logger.info("[Agreement] - Joined Instance > 0");
            // Obviously your agreement protocols will not decide things as soon as you
            // receive the first message
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        } else {
            // logger.info("[Agreement] - Joined Instance < 0");
            // We have not yet received a JoinedNotification, but we are already receiving
            // messages from the other
            // agreement instances, maybe we should do something with them...?
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        logger.info("3. Decided - Upon JoinedNotification");
        logger.info("3.0 - Joined Instance: {}", notification.getJoinInstance());

        if (this.membership.contains(notification.getJoinInstance())) {
            logger.info(notification.getMembership());
            logger.info("3.1 - Already in the membership");
            return;
        }
        logger.info("3.2 - Not in the membership");
        this.membership.add(notification.getJoinInstance());
        logger.info("3.3 - Membership: {}", this.membership);

        // logger.info("[Agreement] - Upon JoinedNotification")
        // We joined the system and can now start doing things
        // joinedInstance = notification.getJoinInstance();
        // membership = new LinkedList<>(notification.getMembership());
        // logger.info("Agreement starting at instance {}, membership: {}",
        // joinedInstance, membership);
    }

    private void uponJoinRequest(JoinRequest request, short sourceProto) {
        logger.info("2.1 - Received Join Propose Request");
        ByteBuf buf = Unpooled.wrappedBuffer(request.getOperation());
        try {
            JoinMessage joinMessage = JoinMessage.serializer.deserialize(buf);

            if (this.membership.size() < 3) {
                logger.info("Only Leader added, consensus automatically");
                logger.info("Request from: " + request);
                logger.info("Source Proto: " + sourceProto);
                this.membership.add(joinMessage.getJoiningNode());

                Host joinedInstance = joinMessage.getJoiningNode();
                this.openConnection(joinedInstance);
                // triggerNotification(
                // new DecidedNotification(joinMessage.getCurrentInstance(),
                // joinMessage.getOpID(),
                // joinMessage.getOperation()));
            }

        } catch (IOException e) {
            logger.error("Error deserializing message", e);
        } finally {
            buf.release();
        }

    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.info("2.1 - Received Propose Request");

        // ByteBuf buf = Unpooled.buffer();
        // try {
        //
        // } catch (IOException e) {
        // logger.error("Error serializing message", e);
        // } finally {
        // buf.release();
        // }
        //
        // if (this.membership.size() <= 3) {
        // logger.info("Only Leader added, consensus automatically");
        // logger.info("Request from: " + request);
        // logger.info("Source Proto: " + sourceProto);
        //
        // }

        // // logger.info("[Agreement] - Upon ProposeRequest");
        // BroadcastMessage msg = new BroadcastMessage(request.getInstance(),
        // request.getOpId(), request.getOperation());
        // // logger.info("Sending to: " + membership);
        //
        // membership.forEach(h -> {
        // this.openConnection(h);
        // sendMessage(msg, h);
        // });
    }

    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.info("Received decided notification from {}", notification);
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.info("2.1 - Received Add Replica Request");
        logger.info("Request from: " + request);

        if (this.membership.contains(request.getReplica())) {
            logger.info("Already a member of the cluster");
            return;
        }

        if (this.membership.size() < 3) {
            logger.info("Only Leader added, consensus automatically");
            this.membership.add(request.getReplica());

            JoinedNotification joinedNotification = new JoinedNotification(this.membership, request.getReplica());
            triggerNotification(joinedNotification);
            return;
        }

        // The AddReplicaRequest contains an "instance" field, which we ignore in this
        // incorrect protocol.
        // You should probably take it into account while doing whatever you do here.
        // membership.forEach(h -> {
        // if (h.equals(request.getReplica())) {
        // return;
        // }
        // this.openConnection(h);
        // IncorrectAgreement msg = new IncorrectAgreement(request.getInstance(),
        // request.getId());
        // sendMessage(msg, h);
        // });
        // membership.add(request.getReplica());
        // membership.forEach({
        //
        // });
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        // logger.info("Received " + request);
        // The RemoveReplicaRequest contains an "instance" field, which we ignore in
        // this incorrect protocol.
        // You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
        this.joinedInstance--;
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        // If a message fails to be sent, for whatever reason, log the message and the
        // reason
        //
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
