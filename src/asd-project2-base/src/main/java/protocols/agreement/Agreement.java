package protocols.agreement;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.messages.AcceptPaxosMessage;
import protocols.agreement.messages.DecidedMessage;
import protocols.agreement.messages.LearnPaxosMessage;
import protocols.agreement.messages.NewNodeMessage;
import protocols.agreement.messages.PreparePaxosMessage;
import protocols.agreement.messages.PromisePaxosMessage;
import protocols.agreement.messages.PreparePaxosMessage.OperationType;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.NodeDecidedNotification;
import protocols.agreement.requests.JoinRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.OperationMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

public class Agreement extends GenericProtocol {
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Agreement";
    private static final Logger logger = LogManager.getLogger(Agreement.class);

    /* Membership State */
    private final Host self;
    private LinkedList<Host> membership;
    private boolean isLeader;
    private Host currentLeader;

    /* Proposes States */
    private HashMap<UUID, PaxosInstance> listOfProposes;
    private HashMap<UUID, LearnPaxosMessage> decidedMessages;

    /* Channel State */
    private int channelId;

    public Agreement(Properties props)
            throws NumberFormatException, UnknownHostException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        /* Init Membership State */
        this.self = new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("p2p_port")));
        this.membership = new LinkedList<Host>();
        this.isLeader = false;
        this.currentLeader = null;

        /* Init Proposes State */
        this.listOfProposes = new HashMap<UUID, PaxosInstance>();
        this.decidedMessages = new HashMap<UUID, LearnPaxosMessage>();

        /* Init Channel State */
        this.channelId = 0;

        /* Init Request Handlers */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(JoinRequest.REQUEST_ID, this::uponJoinRequest);

        /* Init Subscribers */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
        try {
            List<Host> initialMembership = new LinkedList<>();
            String[] leaderAddressParts = props.getProperty("initial_membership").split(",")[0].split(":");
            String leaderHostname = leaderAddressParts[0];
            int leaderPort = Integer.parseInt(leaderAddressParts[1]);
            Host leaderHost = new Host(InetAddress.getByName(leaderHostname), leaderPort);
            initialMembership.add(leaderHost);

            if (leaderHost.equals(self)) {
                this.isLeader = true;
                this.currentLeader = self;
            } else {
                this.isLeader = false;
                this.currentLeader = leaderHost;
                initialMembership.add(self);
            }
            this.membership = new LinkedList<>(initialMembership);
        } catch (UnknownHostException e) {
            throw new AssertionError("Error parsing initial_membership", e);
        }
    }

    /* Receive Propose Requests from SMR */
    public void uponProposeRequest(ProposeRequest request, short sourceProto) {
        if (this.listOfProposes.containsKey(request.getOperationId())) {
            logger.info("Self:[{}] Received Propose Request for an already proposed operation", self);
            return;
        }

        if (!this.isLeader) {
            OperationMessage msg = new OperationMessage(request.getOperationId(), self,
                    1, request.getOperation());
            this.openConnection(this.currentLeader);
            sendMessage(msg, this.currentLeader);
            return;
        }

        PaxosInstance paxosInstance = new PaxosInstance(request.getOperationId(), 1,
                this.membership,
                PaxosInstance.InstanceType.REGULAR);

        paxosInstance
                .setProposedValue(new ProposedValue(ProposedValue.OperationType.REGULAR, null, request.getOperation()));
        this.listOfProposes.put(request.getOperationId(), paxosInstance);

        PreparePaxosMessage preparePaxosMessage = new PreparePaxosMessage(paxosInstance.getId(),
                paxosInstance.getBallot(),
                request.getOperationId(), PreparePaxosMessage.OperationType.REGULAR);

        for (Host host : this.membership) {
            this.openConnection(host);
            sendMessage(preparePaxosMessage, host);
        }
    }

    /* Receive Join Node Requests from SMR */
    public void uponJoinRequest(JoinRequest request, short sourceProto) {
        Host joiningNode = request.getJoiningNode();
        UUID operationId = request.getOperationId();
        int instance = request.getInstance();

        if (this.listOfProposes.containsKey(request.getOperationId())) {
            logger.info("Self:[{}] Received Join Request for an already proposed operation", self);
            return;
        }

        if (!this.isLeader) {
            JoinMessage joinMessage = new JoinMessage(operationId, joiningNode,
                    this.membership, new byte[0]);
            this.openConnection(this.currentLeader);
            sendMessage(joinMessage, this.currentLeader);
            return;
        }

        if (this.membership.size() < 3 && this.membership.contains(self)) {
            executeJoinAgreementWithoutQuourum(joiningNode, operationId, instance);
            return;
        }

        PaxosInstance paxosInstance = new PaxosInstance(operationId, 1,
                this.membership,
                PaxosInstance.InstanceType.JOIN);
        paxosInstance.setProposedValue(new ProposedValue(ProposedValue.OperationType.JOIN, joiningNode, null));
        this.listOfProposes.put(operationId, paxosInstance);

        PreparePaxosMessage preparePaxosMessage = new PreparePaxosMessage(paxosInstance.getId(),
                paxosInstance.getBallot(),
                operationId, PreparePaxosMessage.OperationType.JOIN);
        for (Host host : this.membership) {
            if (!host.equals(self)) {
                this.openConnection(host);
                sendMessage(preparePaxosMessage, host);
            }
        }
    }

    /* Execute Join Agreement when don't have quourum */
    public void executeJoinAgreementWithoutQuourum(Host joiningNode, UUID operationId, int instance) {
        this.membership.add(joiningNode);
        NodeDecidedNotification nodeDecidedNotification = new NodeDecidedNotification(
                operationId, NodeDecidedNotification.OperationType.JOIN,
                NodeDecidedNotification.DecisionType.COMMIT, joiningNode, instance, new byte[0]);
        triggerNotification(nodeDecidedNotification);

        for (Host host : this.membership) {
            if (!host.equals(self) && !host.equals(joiningNode)) {
                this.openConnection(host);
                NewNodeMessage newNodeMessage = new NewNodeMessage(
                        operationId, joiningNode, this.membership);
                sendMessage(newNodeMessage, host);
            }
        }
        return;
    }

    /* Create Shared TCP Channel with SMR Channel */
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        this.channelId = notification.getChannelId();
        registerSharedChannel(notification.getChannelId());
        try {

            /* Register Message Handlers */
            registerMessageHandler(this.channelId, PreparePaxosMessage.MESSAGE_ID,
                    this::uponPreparePaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, PromisePaxosMessage.MESSAGE_ID,
                    this::uponPromisePaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, AcceptPaxosMessage.MESSAGE_ID,
                    this::uponAcceptPaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, LearnPaxosMessage.MESSAGE_ID,
                    this::uponLearnAcceptPaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, NewNodeMessage.MSG_ID,
                    this::uponNewNodeMessage);
            registerMessageHandler(this.channelId, DecidedMessage.MESSAGE_ID,
                    this::uponDecidedMessage);

            /* Register Message Serailizers */
            registerMessageSerializer(this.channelId, PreparePaxosMessage.MESSAGE_ID,
                    PreparePaxosMessage.serializer);
            registerMessageSerializer(this.channelId, PromisePaxosMessage.MESSAGE_ID,
                    PromisePaxosMessage.serializer);
            registerMessageSerializer(this.channelId, AcceptPaxosMessage.MESSAGE_ID,
                    AcceptPaxosMessage.serializer);
            registerMessageSerializer(this.channelId, LearnPaxosMessage.MESSAGE_ID,
                    LearnPaxosMessage.serializer);
            registerMessageSerializer(this.channelId, NewNodeMessage.MSG_ID,
                    NewNodeMessage.serializer);
            registerMessageSerializer(this.channelId, DecidedMessage.MESSAGE_ID,
                    DecidedMessage.serializer);

            /* Register Channel Events */
            registerChannelEventHandler(this.channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
            registerChannelEventHandler(this.channelId, OutConnectionFailed.EVENT_ID,
                    this::uponOutConnectionFailed);
            registerChannelEventHandler(this.channelId, OutConnectionUp.EVENT_ID,
                    this::uponOutConnectionUp);
            registerChannelEventHandler(this.channelId, InConnectionUp.EVENT_ID,
                    this::uponInConnectionUp);
            registerChannelEventHandler(this.channelId, InConnectionDown.EVENT_ID,
                    this::uponInConnectionDown);

        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }

    /* Receive Prepare Paxos Messages */
    public void uponPreparePaxosMessage(PreparePaxosMessage msg, Host host, short sourceProto, int channelId) {
        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());

        int ballot = msg.getBallot();
        OperationType operationType = msg.getOperationType();

        if (paxosInstance == null) {
            PaxosInstance.InstanceType instanceType = operationType == PreparePaxosMessage.OperationType.JOIN
                    ? PaxosInstance.InstanceType.JOIN
                    : PaxosInstance.InstanceType.REGULAR;
            paxosInstance = new PaxosInstance(msg.getPaxosInstanceID(), ballot,
                    this.membership,
                    instanceType);
            this.listOfProposes.put(msg.getOperationID(), paxosInstance);
        }

        if (ballot >= paxosInstance.getBallot()) {
            paxosInstance.setBallot(ballot);
            paxosInstance.setPromise(host, true);
        }

        PromisePaxosMessage promisePaxosMessage;

        if (operationType == PreparePaxosMessage.OperationType.JOIN)
            promisePaxosMessage = new PromisePaxosMessage(
                    paxosInstance.getId(), paxosInstance.getBallot(), msg.getOperationID(),
                    PromisePaxosMessage.OperationType.JOIN);
        else
            promisePaxosMessage = new PromisePaxosMessage(paxosInstance.getId(),
                    paxosInstance.getBallot(), msg.getOperationID(), PromisePaxosMessage.OperationType.REGULAR);

        if (!host.equals(self)) {
            this.openConnection(host);
            sendMessage(promisePaxosMessage, host);
        }
    }

    /*
     * Receive Promise Paxos from Agreement and send Accept Paxos Message to
     * Agreement if has quorum
     */
    public void uponPromisePaxosMessage(PromisePaxosMessage msg, Host host, short sourceProto, int channelId) {
        if (!this.isLeader) {
            logger.info("I'm not the leader, I'm not going to process this message");
            return;
        }
        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());
        PromisePaxosMessage.OperationType operationType = msg.getOperationType();

        if (paxosInstance == null) {
            if (operationType == PromisePaxosMessage.OperationType.JOIN) {
                paxosInstance = new PaxosInstance(msg.getPaxosInstanceID(), msg.getBallot(),
                        this.membership, PaxosInstance.InstanceType.JOIN);
                this.listOfProposes.put(msg.getOperationID(), paxosInstance);
            }

            if (operationType == PromisePaxosMessage.OperationType.REGULAR) {
                paxosInstance = new PaxosInstance(msg.getPaxosInstanceID(), msg.getBallot(),
                        this.membership, PaxosInstance.InstanceType.REGULAR);
                this.listOfProposes.put(msg.getOperationID(), paxosInstance);
            }
        }

        paxosInstance.setPromise(host, true);
        if (hasQuorum(paxosInstance.getPromises())) {
            AcceptPaxosMessage acceptMsg = new AcceptPaxosMessage(paxosInstance.getId(), paxosInstance.getBallot(),
                    msg.getOperationID());
            for (Host member : this.membership) {
                if (!member.equals(self)) {
                    this.openConnection(member);
                    sendMessage(acceptMsg, member);
                }
            }
        }
    }

    /* Receive Accept Paxos Messages and Send Learn Paxos Message to Agreement */
    public void uponAcceptPaxosMessage(AcceptPaxosMessage msg, Host host, short sourceProto, int channelId) {
        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());
        int msgBallot = msg.getBallot();
        int paxosBallot = paxosInstance.getBallot();

        if (paxosInstance == null || msgBallot < paxosBallot) {
            return;
        }

        if (msgBallot >= paxosBallot) {
            paxosInstance.setBallot(msgBallot);
            paxosInstance.setAccept(host, true);

            LearnPaxosMessage learnPaxosMessage = new LearnPaxosMessage(
                    paxosInstance.getId(),
                    paxosBallot,
                    msg.getOperationID());
            this.openConnection(this.currentLeader);
            sendMessage(learnPaxosMessage, this.currentLeader);
        }
    }

    /* Receive Learn Paxos Messages and send Decided Message to SMR */
    public void uponLearnAcceptPaxosMessage(LearnPaxosMessage msg, Host host, short sourceProto, int channelId) {
        UUID operationId = msg.getOperationID();
        PaxosInstance paxosInstance = this.listOfProposes.get(operationId);
        if (paxosInstance == null || msg.getBallot() < paxosInstance.getBallot()) {
            return;
        }
        ProposedValue proposedValue = paxosInstance.getProposedValue();

        paxosInstance.setAccept(host, true);
        if (hasQuorum(paxosInstance.getAccepts())) {
            if (proposedValue != null && proposedValue.getType() == ProposedValue.OperationType.JOIN) {
                Host joiningNode = proposedValue.getJoiningNode();
                if (joiningNode != null && !this.membership.contains(joiningNode)) {
                    this.membership.add(joiningNode);
                    notifyJoinDecisionToStateMachine(operationId, joiningNode);
                }
            }
            cleanProposes(operationId);
            if (!this.isLeader) {
                return;
            }

            if (this.decidedMessages.containsKey(operationId)) {
                logger.info("Self:[{}] Already sent a decided message for {}", self, operationId);
                return;
            }

            this.decidedMessages.put(operationId, msg);
            byte[] operationPayload = null;
            if (proposedValue != null) {
                operationPayload = proposedValue.getValue();
            }

            for (Host member : this.membership) {
                DecidedMessage decidedMessage = new DecidedMessage(operationId, this.membership, operationPayload);
                this.openConnection(member);
                sendMessage(decidedMessage, member);
            }
        }
    }

    public void uponNewNodeMessage(NewNodeMessage msg, Host host, short sourceProto, int channelId) {
        this.membership = new LinkedList<>(msg.getUpdatedMembership());
        NodeDecidedNotification nodeDecidedNotification = new NodeDecidedNotification(msg.getOperationId(),
                NodeDecidedNotification.OperationType.JOIN,
                NodeDecidedNotification.DecisionType.COMMIT,
                msg.getJoiningNode(), 1, new byte[0]);
        triggerNotification(nodeDecidedNotification);
    }

    public void uponDecidedMessage(DecidedMessage msg, Host host, short sourceProto, int channelId) {
        this.membership = new LinkedList<>(msg.getMembership());
        DecidedNotification notification = new DecidedNotification(msg.getOperationID(),
                DecidedNotification.DecisionType.COMMIT, this.membership,
                msg.getOperationPayload());
        triggerNotification(notification);
    }

    public void uponMsgFail(ProtoMessage msg, Host host, short destProto,
            Throwable throwable, int channelId) {
        logger.info("Message {} to {} failed, reason: {}", msg, host, throwable);
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

    private void notifyJoinDecisionToStateMachine(UUID operationId, Host joiningNode) {
        NodeDecidedNotification nodeDecidedNotification = new NodeDecidedNotification(operationId,
                NodeDecidedNotification.OperationType.JOIN,
                NodeDecidedNotification.DecisionType.COMMIT,
                joiningNode,
                0,
                null);
        triggerNotification(nodeDecidedNotification);
    }

    public boolean hasQuorum(HashMap<Host, Boolean> promises) {
        int count = 0;
        for (Boolean accepted : promises.values()) {
            if (accepted)
                count++;
        }
        return count > membership.size() / 2;
    }

    private void cleanProposes(UUID operationID) {
        this.listOfProposes.remove(operationID);
    }

}
