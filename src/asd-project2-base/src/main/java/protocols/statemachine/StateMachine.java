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
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.Agreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.NodeDecidedNotification;
import protocols.agreement.requests.JoinRequest;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.OperationMessage;
import protocols.statemachine.messages.StateTransferMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
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

    // Operation States
    private boolean readyOperationInstance;
    private int nextOperationInstance;
    private HashSet<UUID> decidedOperations;

    // Network State
    private final int channelID;

    // Application State
    private HashMap<String, byte[]> applicationState;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.isLeader = false;
        this.currentLeader = null;
        this.membership = new LinkedList<>();

        this.readyOperationInstance = false;
        this.nextOperationInstance = 0;
        this.decidedOperations = new HashSet<UUID>();
        this.nextOperationInstance = 0;

        this.applicationState = new HashMap<>();

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port);
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelID = createChannel(TCPChannel.NAME, channelProps);

        // Channel Events
        registerChannelEventHandler(channelID, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelID, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelID, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelID, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelID, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        // Message Handlers
        registerMessageSerializer(channelID, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelID, OperationMessage.MSG_ID, OperationMessage.serializer);
        registerMessageSerializer(channelID, StateTransferMessage.MSG_ID, StateTransferMessage.serializer);

        registerMessageHandler(channelID, JoinMessage.MSG_ID, this::uponReceivedJoinMessage);
        registerMessageHandler(channelID, OperationMessage.MSG_ID, this::uponReceivedOperationMessage);
        registerMessageHandler(channelID, StateTransferMessage.MSG_ID, this::uponReceivedStateTransferMessage);

        // Notification Handlers
        subscribeNotification(NodeDecidedNotification.NOTIFICATION_ID, this::uponNodeDecidedNotification);
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        logger.info("Self:[{}] SMR Channel ID {}", self, channelID);
    };

    @Override
    public void init(Properties props) {
        triggerNotification(new ChannelReadyNotification(channelID, self));
        String[] leaderAddress = "localhost:34000".split(":");
        Host leaderHost;
        List<Host> initialMembership = new LinkedList<>();

        try {
            leaderHost = new Host(InetAddress.getByName(leaderAddress[0]), Integer.parseInt(leaderAddress[1]));
            initialMembership.add(leaderHost);
        } catch (UnknownHostException e) {
            throw new AssertionError("Error parsing initial_membership", e);
        }

        if (leaderHost.equals(self)) {
            this.isLeader = true;
            this.currentLeader = self;
            this.readyOperationInstance = true;
            logger.info("Self:[{}] is the leader", self);
        } else {
            this.isLeader = false;
            this.currentLeader = leaderHost;
            logger.info("Self:[{}] is not the leader", self);
            initialMembership.add(self);

            sendJoinMessage();
        }

        this.membership = new LinkedList<>(initialMembership);
    };

    public void sendJoinMessage() {
        UUID joinRequestID = UUID.randomUUID();
        JoinMessage joinOperationMessage = new JoinMessage(joinRequestID,
                self, membership, null);
        this.nextOperationInstance++;
        logger.info("Self:[{}] Sending join message to {}", self, this.currentLeader);
        this.openConnection(this.currentLeader);
        sendMessage(joinOperationMessage, this.currentLeader);
    }

    public void uponReceivedOperationMessage(OperationMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received operation message from {}", self, msg);

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
    };

    public void uponReceivedJoinMessage(JoinMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self[{}] Received join message from {}", self, msg);

        if (!this.readyOperationInstance) {
            logger.info("Self:[{}] Not ready to receive operation messages", self);
            return;
        }

        if (this.isLeader) {
            JoinRequest newJoinRequest = new JoinRequest(this.nextOperationInstance++, msg.getOperationId(),
                    msg.getRequester());
            sendRequest(newJoinRequest, Agreement.PROTOCOL_ID);
        } else {
            sendMessage(msg, this.currentLeader);
        }
    };

    public void uponReceivedStateTransferMessage(StateTransferMessage msg, Host host, short sourceProto,
            int channelId) {
        logger.info("Self:[{}] Received state transfer from {}", self, msg.getLeader());
        if (!msg.getLeader().equals(self)) {
            try {
                this.membership = msg.getMembership();
                this.applicationState = StateMachine.deserializeState(msg.getState());
                this.readyOperationInstance = true;

                logger.info("Self:[{}] State Transfered - New Membership - {}", self, this.membership);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Self:[{}] State transfer from {} to {} received", self, msg.getLeader(), msg.getReplica());
        }
    }

    public void uponNodeDecidedNotification(NodeDecidedNotification notification, short sourceProto) {
        logger.info("Self:[{}] Received new node decision from {}", self, notification);
        if (notification.getDecisionType() == NodeDecidedNotification.DecisionType.COMMIT) {
            if (notification.getOperationType() == NodeDecidedNotification.OperationType.JOIN) {
                if (this.membership.contains(notification.getJoiningNode())) {
                    logger.info("Self:[{}] Node {} already in membership", self, notification.getJoiningNode());
                    return;
                } else {
                    this.membership.add(notification.getJoiningNode());
                    logger.info("Self:[{}] Node {} added to membership", self, notification.getJoiningNode());
                    if (this.isLeader) {
                        try {
                            byte[] leaderSerializedState = StateMachine.serializeState(this.applicationState);
                            StateTransferMessage stateTransferMessage = new StateTransferMessage(leaderSerializedState,
                                    self, notification.getJoiningNode(), this.membership);
                            logger.info("Self:[{}] Sending state to {}", self, notification.getJoiningNode());
                            this.openConnection(notification.getJoiningNode());
                            sendMessage(stateTransferMessage, notification.getJoiningNode());

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    logger.info("Self:[{}] Current Membership after Join -> {}", self, this.membership);
                    return;
                }
            } else {
                if (!this.membership.contains(notification.getJoiningNode())) {
                    logger.info("Self:[{}] Node {} not in membership", self, notification.getJoiningNode());
                    return;
                } else {
                    this.membership.remove(notification.getJoiningNode());
                    logger.info("Self:[{}] Node {} removed from membership", self, notification.getJoiningNode());
                    return;
                }
            }
        } else {
            if (!this.isLeader) {
                logger.info("Self:[{}] Notification Aborted -> {}", self, notification);
                return;
            } else {
                logger.info("Self:[{}] Node Decided New Round -> {}", self, notification);
                JoinRequest newJoinRequest = new JoinRequest(this.nextOperationInstance++,
                        notification.getOperationID(), notification.getJoiningNode());
                sendRequest(newJoinRequest, Agreement.PROTOCOL_ID);
            }
        }
    }

    public void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.info("Self:[{}] Received decided notification from {}", self, notification);
        if (this.decidedOperations.contains(notification.getOperationId())) {
            logger.info("Self:[{}] Operation {} already decided", self, notification.getOperationId());
            return;
        }

        if (notification.getDecisionType() == DecidedNotification.DecisionType.COMMIT) {
            this.nextOperationInstance++;
            this.membership = new LinkedList<>(notification.getMembership());
            this.decidedOperations.add(notification.getOperationId());
            synchronized (this.applicationState) {
                // execute operation here
            }
            logger.info("Self:[{}] Notification Executed -> {}", self, notification.getOperationId());
            logger.info("Self:[{}] Current Membership -> {}", self, this.membership);
            return;
        }

        // byte[] operationPayload = notification.getOperationPayload();
        // try {
        // ByteBuf operationBuf = Unpooled.wrappedBuffer(operationPayload);
        // OperationMessage operationToBeExecuted =
        // OperationMessage.serializer.deserialize(operationBuf);
        // logger.info("Self:[{}] Received decided notification from {}", self,
        // notification);
        // if (this.decidedOperations.contains(operationToBeExecuted.getOperationId()))
        // {
        // logger.info("Self:[{}] Operation {} already decided", self,
        // operationToBeExecuted.getOperationId());
        // return;
        // }
        //
        // if (notification.getDecisionType() ==
        // DecidedNotification.DecisionType.COMMIT) {
        // this.nextOperationInstance++;
        // this.membership = new LinkedList<>(notification.getMembership());
        // this.decidedOperations.add(operationToBeExecuted.getOperationId());
        // synchronized (this.applicationState) {
        // // execute operation here
        // }
        // logger.info("Self:[{}] Notification Executed -> {}", self,
        // operationToBeExecuted.getOperationId());
        // return;
        // }
        //
        // if (notification.getDecisionType() == DecidedNotification.DecisionType.ABORT)
        // {
        // if (!this.isLeader) {
        // logger.info("Self:[{}] Notification Aborted -> {}", self,
        // operationToBeExecuted.getOperationId());
        // return;
        // } else {
        // // new round here
        // ProposeRequest newProposeRequest = new
        // ProposeRequest(this.nextOperationInstance++,
        // operationToBeExecuted.getOperationId(), operationPayload);
        // sendRequest(newProposeRequest, Agreement.PROTOCOL_ID);
        // }
        // }
        //
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
    };

    /*
     * --------------------------------- TCPChannel Events
     * ----------------------------
     */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Self:[{}] Connection to {} is up", self, event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.info("Self:[{}] Connection to {} is down, cause {}", self, event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.info("Self:[{}] Connection to {} failed, cause {}", self, event.getNode(), event.getCause());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Self:[{}] Connection from {} is up", self, event.getNode());

    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.info("Self:[{}] Connection from {} is down, cause {}", self, event.getNode(), event.getCause());
    }

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

    // Custom deserialization of the state map
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
};
