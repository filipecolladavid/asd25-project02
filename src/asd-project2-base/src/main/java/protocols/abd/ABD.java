package protocols.abd;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.abd.messages.AddReplicaMessage;
import protocols.abd.messages.BroadcastMessage;
import protocols.abd.messages.ReadTag;
import protocols.abd.messages.SendTag;
import protocols.abd.notifications.ChannelReadyNotification;
import protocols.abd.notifications.DecidedNotification;
import protocols.abd.notifications.JoinedNotification;
import protocols.abd.requests.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

/**
 * ABD agreement protocol.
 * Based on the Sharing Memory Robustly in Message-Passing Systems (aka ABD) paper.
 * @see <a href="https://www.cs.huji.ac.il/course/2004/dist/p124-attiya.pdf">Paper</a>
 */
public class ABD extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(ABD.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "ABD";

    private final int channelID;

    private Host myself;
    // Current instance of the protocol (should we increment everytime we read/write)?
    private int joinedInstance;
    // Current members in the membership
    private List<Host> membership;
    private Pair<Integer, UUID> tag;
    private boolean pending;
    // Write Request Queue
    private List<WriteRequest> writeRequestsQ;
    // Read Request Queue
    private List<ReadRequest> readRequestsQ;
    // Local copy of the KV map
    private HashMap<String, byte[]> state;

    public ABD(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;
        tag = Pair.of(0, null); // We haven't yet seen a message

        Properties channelProps = new Properties();
        // Creating TCP channel
        int port = Integer.parseInt(props.getProperty("port"));
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, String.valueOf(port));
        this.channelID = createChannel(TCPChannel.NAME, channelProps);

        /*------------------------------ Register Timer Handlers -------------------------------------------- */

        /*------------------------------ Register Request Handlers ------------------------------------------ */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);

        /*------------------------------ Register Notification Handlers -------------------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);


        /*------------------------------ Register Message Message Serializers --------------------------------*/


        // Create AddReplica request to join the system
        if(props.containsKey("contact")) {
            AddReplicaMessage addReplica = new AddReplicaMessage(this.joinedInstance, myself);
            String[] contactIP = props.getProperty("contact").split(":");
            String ipAddr = contactIP[0];
            int contactPort = Integer.parseInt(contactIP[1]);
            Host contactHost = new Host(InetAddress.getByName(ipAddr), contactPort);
            openConnection(contactHost);
            sendMessage(addReplica, contactHost);
        } else {
            // I'm the first replica
            this.membership = new ArrayList<>();
            this.membership.add(this.myself);
            joinedInstance++;
        }
        // Initialize requests lists
        writeRequestsQ = new LinkedList<>();
        readRequestsQ = new LinkedList<>();
        state = new HashMap<>();


    }

    @Override
    public void init(Properties props) throws IOException {
        //Nothing to do here, we just wait for events from the application or agreement (?)
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
        registerMessageSerializer(cId, SendTag.MSG_ID, SendTag.serializer);
        registerMessageSerializer(cId, ReadTag.MSG_ID, ReadTag.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
            registerMessageHandler(cId, SendTag.MSG_ID, this::uponSendTag, this::uponMsgFail);
            registerMessageHandler(cId, ReadTag.MSG_ID, this::uponReadTag, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    /**
     * When receiving a write request from the application.
     * @param request from the application
     * @param sourceProto id of the requester protocol
     */
    private void uponWriteRequest(WriteRequest request, short sourceProto) {

    }

    /**
     * When receiving a tag requested earlier on {@link ABD#uponWriteRequest(WriteRequest, short)}
     * @param msg with the tags
     * @param host who send the message
     * @param sourceProto id of the requester protocol
     * @param channelId to send the message
     */
    private void uponSendTag(SendTag msg, Host host, short sourceProto, int channelId) {

    }

    /**
     * When receiving a read tag from a host who received a write request
     * @param msg
     * @param host
     * @param sourceProto
     * @param channelId
     */
    private void uponReadTag(ReadTag msg, Host host, short sourceProto, int channelId) {
        SendTag sendTag = new SendTag(tag);
    }


    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            //Obviously your agreement protocols will not decide things as soon as you receive the first message
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received " + request);
        BroadcastMessage msg = new BroadcastMessage(request.getInstance(), request.getOpId(), request.getOperation());
        logger.debug("Sending to: " + membership);
        membership.forEach(h -> sendMessage(msg, h));
    }
    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        request.getInstance();
        membership.add(request.getReplica());
    }
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
