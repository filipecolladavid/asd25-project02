package protocols.statemachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.Agreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.LeaderDecidedNotification;
import protocols.agreement.notifications.NodeDecidedNotification;
import protocols.agreement.requests.JoinRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.OperationMessage;
import protocols.statemachine.messages.StateTransferMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;
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

    // private static final int EXECUTION_INTERVAL = 50;

    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    /* Membership States */
    private final Host self;
    private List<Host> membership;
    private Host currentLeader;
    private boolean isLeader;

    /* Operation States */
    private boolean readyOperationInstance;
    private int nextOperationInstance;
    private Queue<Map.Entry<UUID, Object>> pendingOperations;
    private HashSet<UUID> decidedOperations;

    /* Network State */
    private final int channelID;

    /* Application State */
    private HashMap<String, byte[]> applicationState;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        /* Init State */
        this.isLeader = false;
        this.currentLeader = null;
        this.membership = new LinkedList<>();

        this.readyOperationInstance = false;
        this.decidedOperations = new HashSet<UUID>();
        this.pendingOperations = new ConcurrentLinkedQueue<>();

        this.nextOperationInstance = 0;

        this.applicationState = new HashMap<>();

        /* Init Self Host */
        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));
        logger.info("Self:[{}] Initializing State Machine with Adress {} and Port {}", self, address, port);

        /* Init Network State */
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port);
        channelID = createChannel(TCPChannel.NAME, channelProps);

        /* Init Message Handlers */
        registerMessageHandler(channelID, JoinMessage.MSG_ID, this::uponReceivedJoinMessage);
        registerMessageHandler(channelID, OperationMessage.MSG_ID, this::uponReceivedOperationMessage);
        registerMessageHandler(channelID, StateTransferMessage.MSG_ID, this::uponReceivedStateTransferMessage);

        /* Init Message Serializers */
        registerMessageSerializer(channelID, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelID, OperationMessage.MSG_ID, OperationMessage.serializer);
        registerMessageSerializer(channelID, StateTransferMessage.MSG_ID, StateTransferMessage.serializer);

        /* Init Notification Handlers */
        subscribeNotification(NodeDecidedNotification.NOTIFICATION_ID, this::uponNodeDecidedNotification);
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
        subscribeNotification(LeaderDecidedNotification.NOTIFICATION_ID, this::uponLeaderDecidedNotification);

        /* Init Request Handlers */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /* Init Channel Handlers */
        registerChannelEventHandler(channelID, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelID, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelID, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelID, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelID, InConnectionDown.EVENT_ID,
                this::uponInConnectionDown);

        // registerTimerHandler(ExecutionPendingOperationTimer.TIMER_ID,
        // this::uponExecutePendingOperation);
        // setupPeriodicTimer(new ExecutionPendingOperationTimer(), EXECUTION_INTERVAL,
        // EXECUTION_INTERVAL);
    }

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelReadyNotification(channelID, self));
        String[] leaderAddressParts = props.getProperty("initial_membership").split(",")[0].split(":");
        String leaderHostname = leaderAddressParts[0];
        int leaderPort = Integer.parseInt(leaderAddressParts[1]);

        List<Host> initialMembership = new LinkedList<>();
        try {
            Host leaderHost = new Host(InetAddress.getByName(leaderHostname), leaderPort);
            initialMembership.add(leaderHost);
            if (leaderPort == self.getPort()) {
                this.isLeader = true;
                this.currentLeader = self;
                this.readyOperationInstance = true;
            } else {
                this.isLeader = false;
                this.currentLeader = leaderHost;
                initialMembership.add(self);
                sendJoinMessage();
            }
            this.membership = new LinkedList<>(initialMembership);
        } catch (UnknownHostException e) {
            throw new AssertionError("Error parsing initial_membership", e);
        }
    }

    /* Receive a new Join Message */
    public void uponReceivedJoinMessage(JoinMessage msg, Host host, short sourceProto, int channelId) {
        if (!this.readyOperationInstance) {
            logger.info("Self:[{}] Not ready to receive operation messages", self);
            return;
        }

        if (!this.isLeader) {
            sendMessage(msg, this.currentLeader);
            return;
        }

        // this.pendingOperations.offer(new
        // AbstractMap.SimpleEntry<>(msg.getOperationId(), msg));
        JoinRequest newJoinRequest = new JoinRequest(this.nextOperationInstance++, msg.getOperationId(),
                msg.getRequester());
        sendRequest(newJoinRequest, Agreement.PROTOCOL_ID);
    }

    /* Receive a new Operation Message */
    public void uponReceivedOperationMessage(OperationMessage msg, Host host, short sourceProto, int channelId) {
        if (!this.readyOperationInstance) {
            logger.info("Self:[{}] Not ready to receive operation messages", self);
            return;
        }

        if (this.decidedOperations.contains(msg.getOperationId())) {
            logger.info("Self:[{}] Operation {} already decided", self, msg.getOperationId());
            return;
        }

        if (!this.isLeader) {
            sendMessage(msg, this.currentLeader);
            return;
        }

        ProposeRequest proposeRequest = new ProposeRequest(msg.getInstanceNumber(),
                msg.getOperationId(), msg.getOperation());
        sendRequest(proposeRequest, Agreement.PROTOCOL_ID);

        // this.pendingOperations.offer(new
        // AbstractMap.SimpleEntry<>(msg.getOperationId(), msg));
    }

    /* Receive a new State Transfer Message after being accepted */
    public void uponReceivedStateTransferMessage(StateTransferMessage msg, Host host, short sourceProto,
            int channelId) {
        if (!msg.getLeader().equals(self)) {
            try {
                this.membership = msg.getMembership();
                this.applicationState = StateMachine.deserializeState(msg.getState());
                this.readyOperationInstance = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /* Receive a new Node Decided Notification */
    public void uponNodeDecidedNotification(NodeDecidedNotification notification, short sourceProto) {
        Host joiningNode = notification.getJoiningNode();
        if (this.membership.contains(joiningNode)) {
            logger.info("Self:[{}] Node {} already in membership", self, joiningNode);
            return;
        }

        this.membership.add(joiningNode);

        if (this.isLeader)
            sendStateTransferMessage(joiningNode);
    }

    /* Receive a new Decided Notification */
    public void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        if (this.decidedOperations.contains(notification.getOperationId())) {
            logger.info("Self:[{}] Operation {} already decided", self, notification.getOperationId());
            return;
        }

        DecidedNotification.DecisionType decisionType = notification.getDecisionType();
        if (decisionType == DecidedNotification.DecisionType.COMMIT) {
            this.nextOperationInstance++;
            this.membership = new LinkedList<>(notification.getMembership());
            this.decidedOperations.add(notification.getOperationId());

            byte[] operationPayload = notification.getOperationPayload();
            if (operationPayload != null && operationPayload.length > 0) {
                executeOperation(notification.getOperationId(), operationPayload);
            }
        }
    }

    public void uponLeaderDecidedNotification(LeaderDecidedNotification notification, short sourceProto) {
        Host newLeader = notification.getNewLeaderHost();
        this.currentLeader = newLeader;
        this.isLeader = newLeader.equals(self);

        if (isLeader) {
            this.readyOperationInstance = true;
        } else {
            openConnection(this.currentLeader);
        }
    }

    /* Receive a new Order Request */
    public void uponOrderRequest(OrderRequest request, short sourceProto) {
        if (!this.readyOperationInstance) {
            logger.info("Self:[{}] Not ready to receive order requests", self);
            return;
        }

        if (this.decidedOperations.contains(request.getOpId())) {
            logger.info("Self:[{}] Operation {} already decided", self, request.getOpId());
            return;
        }

        if (!this.isLeader) {
            OperationMessage msg = new OperationMessage(request.getOpId(), self,
                    this.nextOperationInstance++, request.getOperation());
            openConnection(this.currentLeader);
            sendMessage(msg, this.currentLeader);
            return;
        }

        ProposeRequest proposeRequest = new ProposeRequest(this.nextOperationInstance++,
                request.getOpId(), request.getOperation());
        sendRequest(proposeRequest, Agreement.PROTOCOL_ID);
    }

    /* Execute Operation and Send Notification */
    private void executeOperation(UUID operationId, byte[] payload) {
        if (payload == null)
            return;

        ExecuteNotification executedNotification = new ExecuteNotification(operationId, payload);
        triggerNotification(executedNotification);
    }

    /* Execute Operation from Pending Operations */
    private void uponExecutePendingOperation(ProtoTimer timer, long timerId) {
        if (this.pendingOperations.isEmpty()) {
            logger.debug("No pending operations to execute");
            return;
        }

        Map.Entry<UUID, Object> entry;
        while ((entry = pendingOperations.poll()) != null) {
            UUID operationId = entry.getKey();
            Object operation = entry.getValue();

            try {
                if (operation instanceof OperationMessage) {
                    OperationMessage opMsg = (OperationMessage) operation;
                    ProposeRequest proposeRequest = new ProposeRequest(this.nextOperationInstance++,
                            operationId, opMsg.getOperation());
                    sendRequest(proposeRequest, Agreement.PROTOCOL_ID);
                } else if (operation instanceof JoinMessage) {
                    JoinMessage joinMsg = (JoinMessage) operation;
                    JoinRequest joinRequest = new JoinRequest(this.nextOperationInstance++,
                            operationId, joinMsg.getRequester());
                    sendRequest(joinRequest, Agreement.PROTOCOL_ID);
                }
            } catch (Exception e) {
                logger.error("Error executing operation {}: {}", operationId, e.getMessage());
            }
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Self:[{}] Connection to {} is up", self, event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.info("Self:[{}] Connection to {} is down, cause {}", self,
                event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event,
            int channelId) {
        logger.info("Self:[{}] Connection to {} failed, cause {}", self,
                event.getNode(), event.getCause());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Self:[{}] Connection from {} is up", self, event.getNode());

    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.info("Self:[{}] Connection from {} is down, cause {}", self,
                event.getNode(), event.getCause());
    }

    /* Send Join Message to Leader */
    public void sendJoinMessage() {
        UUID joinRequestID = UUID.randomUUID();
        JoinMessage joinOperationMessage = new JoinMessage(joinRequestID,
                self, membership, null);
        this.nextOperationInstance++;
        this.openConnection(this.currentLeader);
        sendMessage(joinOperationMessage, this.currentLeader);
    }

    /* Send State Transfer Message to Joining Node */
    public void sendStateTransferMessage(Host joiningNode) {
        try {
            byte[] leaderSerializedState = StateMachine.serializeState(this.applicationState);
            StateTransferMessage stateTransferMessage = new StateTransferMessage(leaderSerializedState,
                    self, joiningNode, this.membership);
            this.openConnection(joiningNode);
            sendMessage(stateTransferMessage, joiningNode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Serialize State for State Transfer */
    public static byte[] serializeState(HashMap<String, byte[]> state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeInt(state.size());

            for (Map.Entry<String, byte[]> entry : state.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                byte[] value = entry.getValue();
                dos.writeInt(value.length);
                dos.write(value);
            }

            return baos.toByteArray();
        }
    }

    /* Deserialize State for State Transfer */
    public static HashMap<String, byte[]> deserializeState(byte[] data) throws IOException {
        HashMap<String, byte[]> state = new HashMap<>();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                DataInputStream dis = new DataInputStream(bais)) {

            int numEntries = dis.readInt();

            for (int i = 0; i < numEntries; i++) {
                int keyLength = dis.readInt();
                byte[] keyBytes = new byte[keyLength];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valueLength = dis.readInt();
                byte[] value = new byte[valueLength];
                dis.readFully(value);

                state.put(key, value);
            }

            return state;
        }
    }
}
