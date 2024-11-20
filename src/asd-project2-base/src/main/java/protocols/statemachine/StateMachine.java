package protocols.statemachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.app.messages.RequestMessage;
import protocols.statemachine.messages.ForwardOperationMessage;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.LeadershipMessage;
import protocols.statemachine.messages.OperationMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.requests.OrderRequest;
import protocols.statemachine.timers.JoinTimer;
import protocols.statemachine.timers.LeadershipTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

public class StateMachine extends GenericProtocol {
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;
    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    // Current State
    private enum State {
        JOINING,
        ACTIVE
    }

    private State state;

    // Membership States
    private final Host self;
    private List<Host> membership;
    private Host currentLeader;
    private boolean isLeader;

    // Operations States
    private int nextOperationInstance;
    private int lastExecutedInstance;
    private Map<Integer, OperationMessage> pendingOperations;
    private Map<Integer, OperationMessage> decidedOperations;

    // Application State
    private Map<String, byte[]> applicationState;

    // Network State
    private final int channelId;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.nextOperationInstance = 0;
        this.lastExecutedInstance = -1;

        this.pendingOperations = new HashMap<>();
        this.decidedOperations = new HashMap<>();

        this.isLeader = false;
        this.currentLeader = null;
        this.membership = new LinkedList<>();

        this.applicationState = new HashMap<>();

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);

        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        // Channel Configuration
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port);
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        /*--------------------- Register Message Serializers ----------------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, ForwardOperationMessage.MSG_ID, ForwardOperationMessage.serializer);
        registerMessageSerializer(channelId, LeadershipMessage.MSG_ID, LeadershipMessage.serializer);

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_ID, null, this::uponJoinMessage);
        registerMessageHandler(channelId, ForwardOperationMessage.MSG_ID, null, this::uponForwardOperationMessage);
        registerMessageHandler(channelId, LeadershipMessage.MSG_ID, null, this::uponLeadershipMessage);

        /*--------------------- Register Timeout Handlers ----------------------------- */
        registerTimerHandler(JoinTimer.TIMER_ID, this::uponJoinTimer);
        registerTimerHandler(LeadershipTimer.TIMER_ID, this::uponLeadershipTimer);
    }

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelReadyNotification(channelId, self));

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();

        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }

        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");

            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);

            if (membership.get(0).equals(self)) {
                isLeader = true;
                currentLeader = self;
                logger.info("I am the leader");
            } else {
                isLeader = false;
                currentLeader = membership.get(0);
                logger.info("I am not the leader");
            }

            triggerNotification(new JoinedNotification(membership, 0));
        } else {
            state = State.JOINING;
            isLeader = false;
            currentLeader = null;
            logger.info("Starting in JOINING as I am not part of initial membership");

            if (!initialMembership.isEmpty()) {
                Host contactNode = initialMembership.get(0);
                openConnection(contactNode);
            }
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received order request from {}", request);
    }

    private void uponJoinMessage(RequestMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received join request from {}", msg);
    }

    private void uponForwardOperationMessage(RequestMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received forward operation request from {}", msg);
    }

    private void uponLeadershipMessage(RequestMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received leadership request from {}", msg);
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received decided notification from {}", notification);
    }

    /*--------------------------------- Timer Events ---------------------------------------- */
    private void uponJoinTimer(ProtoTimer protoTimer, long l) {
        logger.debug("Join timer expired");
    }

    private void uponLeadershipTimer(ProtoTimer protoTimer, long l) {
        logger.debug("Leadership timer expired");
    }

    /*--------------------------------- TCP Channel Events ---------------------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause {}", event.getNode(), event.getCause());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
