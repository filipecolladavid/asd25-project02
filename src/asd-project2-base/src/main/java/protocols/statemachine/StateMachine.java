package protocols.statemachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import protocols.agreement.Agreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.LeadershipMessage;
import protocols.statemachine.messages.OperationMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;

import protocols.statemachine.requests.OrderRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
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

    // Membership States
    private final Host self;
    private List<Host> membership;
    private Host currentLeader;
    private boolean isLeader;

    // Operations States
    private int nextOperationInstance;
    private Map<UUID, OperationMessage> pendingOperations;
    private HashSet<UUID> decidedOperations;

    // Network State
    private final int channelId;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.nextOperationInstance = 0;

        this.pendingOperations = new HashMap<>();
        this.decidedOperations = new HashSet<UUID>();

        this.isLeader = false;
        this.currentLeader = null;
        this.membership = new LinkedList<>();

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

        logger.info("Self:[{}] SMR Channel ID {}", self, channelId);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        // subscribeNotification(JoinedNotification.NOTIFICATION_ID,
        // this::uponJoinedNotification);
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        /*--------------------- Register Message Serializers ----------------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, OperationMessage.MSG_ID, OperationMessage.serializer);
        registerMessageSerializer(channelId, LeadershipMessage.MSG_ID, LeadershipMessage.serializer);

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, OperationMessage.MSG_ID, this::uponForwardOperationMessage);
        registerMessageHandler(channelId, LeadershipMessage.MSG_ID, this::uponLeadershipMessage);

        /*--------------------- Register Timeout Handlers ----------------------------- */
    }

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelReadyNotification(channelId, self));

        // String host = props.getProperty("initial_membership");
        String leaderHost = "localhost:34000";
        String[] leader = leaderHost.split(",");
        List<Host> initialMembership = new LinkedList<>();

        for (String s : leader) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            if (h.equals(self)) {
                continue;
            }
            initialMembership.add(h);
        }

        if (!this.membership.contains(self)) {
            initialMembership.add(self);
        }

        if (initialMembership.contains(self)) {
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);

            if (membership.get(0).equals(self)) {
                isLeader = true;
                currentLeader = self;
            } else {
                isLeader = false;
                currentLeader = membership.get(0);

                UUID opID = UUID.randomUUID();
                OperationMessage joinOperationMessage = new OperationMessage(opID,
                        OperationMessage.OperationType.JOIN_REQUEST, self, this.nextOperationInstance++);
                this.pendingOperations.put(opID, joinOperationMessage);
                sendMessage(joinOperationMessage, currentLeader);
            }
        } else {
            isLeader = false;
            currentLeader = null;

            if (!initialMembership.isEmpty()) {
                Host contactNode = initialMembership.get(0);
                openConnection(contactNode);
            }
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
    }

    private void uponForwardOperationMessage(OperationMessage msg, Host host, short sourceProto, int channelId) {
        if (!this.pendingOperations.containsKey(msg.getOperationId())) {
            this.pendingOperations.put(msg.getOperationId(), msg);

            if (this.isLeader) {
                this.membership.forEach(replica -> {
                    if (!replica.equals(self) && !replica.equals(host)) {
                        sendMessage(msg, replica);
                    }
                });

                ProposeRequest newProposeRequest = new ProposeRequest(
                        this.nextOperationInstance++,
                        msg.getOperationId(),
                        msg.getOperationPayload());
                sendRequest(newProposeRequest, Agreement.PROTOCOL_ID);
            }
        }
    }

    private void uponLeadershipMessage(LeadershipMessage msg, Host host, short sourceProto, int channelId) {
        // logger.info("Received leadership request from {}", msg);
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        byte[] operation = notification.getOperationPayload();

        try {
            ByteBuf operationBuf = Unpooled.wrappedBuffer(operation);
            OperationMessage operationToBeExecuted = OperationMessage.serializer.deserialize(operationBuf);

            if (this.pendingOperations.containsKey(operationToBeExecuted.getOperationId())) {
                if (notification.getDecisionType() == DecidedNotification.DecisionType.COMMIT) {
                    if (operationToBeExecuted.getOperationType().equals(OperationMessage.OperationType.JOIN_REQUEST)) {
                        updateMembership(notification.getMembership());
                        // if (this.membership.contains(operationToBeExecuted.getOperationRequester()))
                        // {
                        // } else {
                        // this.membership.add(operationToBeExecuted.getOperationRequester());
                        // }
                    }
                }
            }

            logger.info("Self:[{}] Membership for host -> {}", self, this.membership);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateMembership(HashSet<Host> newMembership) {
        LinkedHashSet<Host> orderedMembership = new LinkedHashSet<>();
        if (currentLeader != null) {
            orderedMembership.add(currentLeader);
            newMembership.stream()
                    .filter(h -> !h.equals(currentLeader))
                    .forEach(orderedMembership::add);
            this.membership = new LinkedList<>(orderedMembership);
        }
    }

    public void updateJoinRequest(OperationMessage operationMessage) {
        this.pendingOperations.remove(operationMessage.getOperationId());
        this.decidedOperations.add(operationMessage.getOperationId());
    }

    /*--------------------------------- Timer Events ---------------------------------------- */

    /*--------------------------------- TCP Channel Events ---------------------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        // logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        // logger.info("Connection to {} is down, cause {}", event.getNode(),
        // event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        // logger.info("Connection to {} failed, cause {}", event.getNode(),
        // event.getCause());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        // logger.info("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        // logger.info("Connection from {} is down, cause: {}", event.getNode(),
        // event.getCause());
    }

}
