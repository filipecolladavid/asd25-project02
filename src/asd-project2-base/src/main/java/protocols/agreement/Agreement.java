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
import protocols.agreement.messages.PromisePaxosMessage.OperationType;
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
import pt.unl.fct.di.novasys.network.data.Host;

class ProposedValue {
    public enum OperationType {
        REGULAR,
        JOIN
    }

    private final OperationType type;
    private Host joiningNode;
    private byte[] value;

    public ProposedValue(OperationType type, Host joiningNode, byte[] value) {
        this.type = type;
        this.joiningNode = joiningNode;
        this.value = value;
    }

    public OperationType getType() {
        return type;
    }

    public Host getJoiningNode() {
        return joiningNode;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setJoiningNode(Host joiningNode) {
        this.joiningNode = joiningNode;
    }
}

class PaxosInstance {
    public enum InstanceType {
        REGULAR,
        JOIN
    }

    UUID id;
    UUID operationId;
    int ballot;
    HashMap<Host, Boolean> promises;
    HashMap<Host, Boolean> accepts;
    int promisesFailureCounter;
    int acceptsFailureCounter;
    boolean isDecided;
    InstanceType instanceType;
    List<Host> members;
    Host joiningNode;
    ProposedValue proposedValue;

    public PaxosInstance(UUID operationId, int ballot, List<Host> agreementMembers, InstanceType instanceType) {
        this.id = UUID.randomUUID();
        this.ballot = ballot;
        this.operationId = operationId;
        this.promises = new HashMap<Host, Boolean>();
        this.accepts = new HashMap<Host, Boolean>();
        this.promisesFailureCounter = 0;
        this.acceptsFailureCounter = 0;
        this.isDecided = false;
        this.instanceType = instanceType;
        this.members = agreementMembers;
        this.joiningNode = null;
    }

    public UUID getId() {
        return this.id;
    }

    public UUID getOperationId() {
        return this.operationId;
    }

    public int getBallot() {
        return this.ballot;
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public HashMap<Host, Boolean> getPromises() {
        return this.promises;
    }

    public void setPromise(Host host, boolean promise) {
        this.promises.put(host, promise);
    }

    public void setAccept(Host host, boolean accept) {
        this.accepts.put(host, accept);
    }

    public HashMap<Host, Boolean> getAccepts() {
        return this.accepts;
    }

    public InstanceType getInstanceType() {
        return this.instanceType;
    }

    public int getPromisesFailureCounter() {
        return this.promisesFailureCounter;
    }

    public void setPromisesFailureCounter() {
        this.promisesFailureCounter++;
    }

    public int getAcceptsFailureCounter() {
        return this.acceptsFailureCounter;
    }

    public void setAcceptsFailureCounter() {
        this.acceptsFailureCounter++;
    }

    public Host getJoiningNode() {
        return this.joiningNode;
    }

    public void setJoiningNode(Host joiningNode) {
        this.joiningNode = joiningNode;
    }

    public void setProposedValue(ProposedValue value) {
        this.proposedValue = value;
    }

    public ProposedValue getProposedValue() {
        return this.proposedValue;
    }

}

public class Agreement extends GenericProtocol {
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Agreement";
    private static final Logger logger = LogManager.getLogger(Agreement.class);

    // Membership State
    private final Host self;
    private LinkedList<Host> membership;
    private boolean isLeader;
    private Host currentLeader;

    // Proposes States
    private HashMap<UUID, PaxosInstance> listOfProposes;
    private HashMap<UUID, LearnPaxosMessage> decidedMessages;

    // Channel State
    private int channelId;

    public Agreement(Properties props)
            throws NumberFormatException, UnknownHostException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("p2p_port")));

        this.membership = new LinkedList<Host>();
        this.listOfProposes = new HashMap<UUID, PaxosInstance>();
        this.decidedMessages = new HashMap<UUID, LearnPaxosMessage>();

        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(JoinRequest.REQUEST_ID, this::uponJoinRequest);

        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) {
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
        } else {
            this.isLeader = false;
            this.currentLeader = leaderHost;
            initialMembership.add(self);
        }

        this.membership = new LinkedList<>(initialMembership);

    }

    public void uponProposeRequest(ProposeRequest request, short sourceProto) {
        if (this.listOfProposes.containsKey(request.getOperationId())) {
            logger.info("Self:[{}] Received Propose Request for an already proposed operation", self);
            return;
        }

        if (!this.isLeader) {
            logger.info("Self:[{}] Received Propose Request from non-leader", self);
            OperationMessage msg = new OperationMessage(request.getOperationId(), self, 1, request.getOperation());
            this.openConnection(this.currentLeader);
            sendMessage(msg, this.currentLeader, Agreement.PROTOCOL_ID);
            return;
        }

        logger.info("Self:[{}] Received Propose Request from {}", self, request);
        PaxosInstance paxosInstance = new PaxosInstance(request.getOperationId(), 1, this.membership,
                PaxosInstance.InstanceType.REGULAR);
        this.listOfProposes.put(request.getOperationId(), paxosInstance);
        PreparePaxosMessage preparePaxosMessage = new PreparePaxosMessage(paxosInstance.getId(),
                paxosInstance.getBallot(),
                request.getOperationId(), PreparePaxosMessage.OperationType.REGULAR);
        for (Host host : this.membership) {
            if (!host.equals(self)) {
                this.openConnection(host);
                sendMessage(
                        preparePaxosMessage,
                        host);
            }
        }
    }

    public void uponJoinRequest(JoinRequest request, short sourceProto) {
        if (this.listOfProposes.containsKey(request.getOperationId())) {
            logger.info("Self:[{}] Received Join Request for an already proposed operation", self);
            return;
        }

        if (!this.isLeader) {
            logger.info("Self:[{}] Received Join Request from non-leader", self);
            JoinMessage msg = new JoinMessage(request.getOperationId(), request.getJoiningNode(), this.membership,
                    new byte[0]);
            this.openConnection(this.currentLeader);
            sendMessage(msg, this.currentLeader, Agreement.PROTOCOL_ID);
            return;
        }

        if (this.membership.size() < 3 && this.membership.contains(self)) {
            logger.info("Self:[{}] Join Request but I'm the only member", self);
            this.membership.add(request.getJoiningNode());

            NodeDecidedNotification nodeDecidedNotification = new NodeDecidedNotification(request.getOperationId(),
                    NodeDecidedNotification.OperationType.JOIN, NodeDecidedNotification.DecisionType.COMMIT,
                    request.getJoiningNode(), request.getInstance(), new byte[0]);
            triggerNotification(nodeDecidedNotification);

            for (Host host : this.membership) {
                logger.info("Self:[{}] Sending Join Message to {}", self, host);
                if (!host.equals(self) && !host.equals(request.getJoiningNode())) {
                    this.openConnection(host);
                    NewNodeMessage joinNotification = new NewNodeMessage(
                            request.getOperationId(),
                            request.getJoiningNode(),
                            this.membership);
                    sendMessage(joinNotification, host);
                }
            }

            return;
        }

        logger.info("Self:[{}] Received Join Request from {}", self, request);
        PaxosInstance paxosInstance = new PaxosInstance(request.getOperationId(), 1, this.membership,
                PaxosInstance.InstanceType.JOIN);
        paxosInstance
                .setProposedValue(new ProposedValue(ProposedValue.OperationType.JOIN, request.getJoiningNode(), null));
        this.listOfProposes.put(request.getOperationId(), paxosInstance);

        PreparePaxosMessage preparePaxosMessage = new PreparePaxosMessage(paxosInstance.getId(),
                paxosInstance.getBallot(),
                request.getOperationId(),
                PreparePaxosMessage.OperationType.JOIN);

        for (Host host : this.membership) {
            logger.info("Self:[{}] Sending Prepare Paxos Message to {}", self, host);
            if (!host.equals(self)) {
                this.openConnection(host);
                sendMessage(preparePaxosMessage, host);
            }
        }
    }

    public void uponPreparePaxosMessage(PreparePaxosMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received Prepare Paxos Message from {}", self, host);

        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());
        int messageBallot = msg.getBallot();

        if (paxosInstance == null) {
            logger.info("Self:[{}] Received Prepare Paxos Message for unknown operation", self);
            PaxosInstance.InstanceType instanceType = msg.getOperationType() == PreparePaxosMessage.OperationType.JOIN
                    ? PaxosInstance.InstanceType.JOIN
                    : PaxosInstance.InstanceType.REGULAR;
            paxosInstance = new PaxosInstance(msg.getPaxosInstanceID(), messageBallot, this.membership, instanceType);
            this.listOfProposes.put(msg.getOperationID(), paxosInstance);
        }

        if (messageBallot >= paxosInstance.getBallot()) {
            logger.info("Self:[{}] Accepting prepare ballot with {}", self, messageBallot);
            paxosInstance.setBallot(messageBallot);
            paxosInstance.setPromise(self, true);

            if (msg.getOperationType() == PreparePaxosMessage.OperationType.JOIN) {
                PromisePaxosMessage promisePaxosMessage = new PromisePaxosMessage(paxosInstance.getId(),
                        paxosInstance.getBallot(),
                        msg.getOperationID(),
                        PromisePaxosMessage.OperationType.JOIN);

                if (!host.equals(self)) {
                    this.openConnection(host);
                    sendMessage(promisePaxosMessage, host);
                }
            } else {
                PromisePaxosMessage promisePaxosMessage = new PromisePaxosMessage(paxosInstance.getId(),
                        paxosInstance.getBallot(),
                        msg.getOperationID(),
                        PromisePaxosMessage.OperationType.REGULAR);
                if (!host.equals(self)) {
                    this.openConnection(host);
                    sendMessage(promisePaxosMessage, host);
                }
            }
        }
    }

    public void uponPromisePaxosMessage(PromisePaxosMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received Promise Paxos Message from {}", self, host);
        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());

        if (paxosInstance == null) {
            if (msg.getOperationType() == OperationType.JOIN) {
                logger.info("Self:[{}] Received Promise Paxos Message for unknown operation", self);
                paxosInstance = new PaxosInstance(msg.getPaxosInstanceID(), msg.getBallot(), this.membership,
                        PaxosInstance.InstanceType.JOIN);
                this.listOfProposes.put(msg.getOperationID(), paxosInstance);
            }
            if (msg.getOperationType() == OperationType.REGULAR) {
                logger.info("Self:[{}] Received Promise Paxos Message for unknown operation", self);
                paxosInstance = new PaxosInstance(msg.getPaxosInstanceID(), msg.getBallot(), this.membership,
                        PaxosInstance.InstanceType.REGULAR);
                this.listOfProposes.put(msg.getOperationID(), paxosInstance);
            }
        }

        paxosInstance.setPromise(host, true);

        if (hasQuorum(paxosInstance.getPromises()) && this.isLeader) {
            logger.info("Self:[{}] Accepting promise ballot with {}", self, msg.getBallot());

            AcceptPaxosMessage acceptPaxosMessage = new AcceptPaxosMessage(paxosInstance.getId(),
                    paxosInstance.getBallot(),
                    msg.getOperationID());

            for (Host member : this.membership) {
                if (!member.equals(self)) {
                    this.openConnection(member);
                    sendMessage(acceptPaxosMessage, member);
                }
            }
        }

    }

    public void uponAcceptPaxosMessage(AcceptPaxosMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received Accept Paxos Message from {}", self, host);

        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());

        if (paxosInstance == null || msg.getBallot() < paxosInstance.getBallot()) {
            return;
        }

        if (msg.getBallot() >= paxosInstance.getBallot()) {
            logger.info("Self:[{}] Accepting accept ballot with {}", self, msg.getBallot());
            paxosInstance.setBallot(msg.getBallot());
            paxosInstance.setAccept(self, true);
            logger.info("Accepts in Accept => {}", paxosInstance.getAccepts());

            logger.info("Self:[{}] Accepted ballot with {}", self, msg.getBallot());
            LearnPaxosMessage learnPaxosMessage = new LearnPaxosMessage(paxosInstance.getId(),
                    paxosInstance.getBallot(),
                    msg.getOperationID());
            this.openConnection(this.currentLeader);
            sendMessage(learnPaxosMessage, this.currentLeader);
        }
    }

    public void uponLearnAcceptPaxosMessage(LearnPaxosMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received Learn Paxos Message from {}", self, host);
        PaxosInstance paxosInstance = this.listOfProposes.get(msg.getOperationID());

        if (paxosInstance == null || msg.getBallot() < paxosInstance.getBallot()) {
            return;
        }

        paxosInstance.setAccept(host, true);

        logger.info("Accepts => {}", paxosInstance.getAccepts());
        logger.info("Has Quorum ? => {}", hasQuorum(paxosInstance.getAccepts()));

        if (hasQuorum(paxosInstance.getAccepts())) {
            logger.info("QUORUM!");

            ProposedValue value = paxosInstance.getProposedValue();
            if (value != null && value.getType() == ProposedValue.OperationType.JOIN) {
                Host joiningNode = value.getJoiningNode();
                if (joiningNode != null && !this.membership.contains(joiningNode)) {
                    this.membership.add(joiningNode);
                    logger.info("Self:[{}] Adding node {} to membership", self, joiningNode);
                }
            }

            notifyDecisionToStateMachine(msg.getOperationID());
            cleanProposes(msg.getOperationID());

            if (this.isLeader) {

                if (this.decidedMessages.containsKey(msg.getOperationID())) {
                    logger.info("Self:[{}] Already sent a decided message for {}", self, msg.getOperationID());
                    return;
                }
                this.decidedMessages.put(msg.getOperationID(), msg);

                for (Host member : this.membership) {
                    if (!host.equals(self)) {
                        DecidedMessage decidedMessage = new DecidedMessage(msg.getOperationID(),
                                this.membership);
                        // this.openConnection(host);
                        sendMessage(decidedMessage, member);
                        logger.info("Self:[{}] Sending Decided Message to {} with {}", self, member, this.membership);
                    }
                }
            }
        }
    }

    public void uponDecidedMessage(DecidedMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received Decided Message from {} with {}", self, host, msg.getMembership());
        this.membership = new LinkedList<>(msg.getMembership());
        notifyDecisionToStateMachine(msg.getOperationID());
    }

    // Decided Message
    // operationID, decisionType, membership

    public void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);

        // PaxosInstance instance = this.listOfProposes.get(msg.getPaxosInstanceID());
        //
        // if (instance != null) {
        // return;
        // }
        //
        // if (msg.getOperationType() == OperationType.PREPARE) {
        // instance.setPromisesFailureCounter();
        // }
        //
        // if (msg.getOperationType() == OperationType.PROMISE) {
        // instance.setAcceptsFailureCounter();
        // }
        //
        // if (instance.getPromisesFailureCounter() >= this.membership.size() / 2 ||
        // instance.getAcceptsFailureCounter() >= this.membership.size() / 2) {
        // cleanProposes(msg.getPaxosInstanceID());
        // triggerNotification(new DecidedNotification(msg.getOperationID(),
        // DecidedNotification.DecisionType.ABORT,
        // this.membership));
        // }
    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        this.channelId = notification.getChannelId();
        registerSharedChannel(this.channelId);

        try {
            registerMessageHandler(this.channelId, PreparePaxosMessage.MESSAGE_ID, this::uponPreparePaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, PromisePaxosMessage.MESSAGE_ID, this::uponPromisePaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, AcceptPaxosMessage.MESSAGE_ID, this::uponAcceptPaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, LearnPaxosMessage.MESSAGE_ID, this::uponLearnAcceptPaxosMessage,
                    this::uponMsgFail);
            registerMessageHandler(this.channelId, NewNodeMessage.MSG_ID, this::uponNewNodeMessage);
            registerMessageHandler(this.channelId, DecidedMessage.MESSAGE_ID, this::uponDecidedMessage);

            registerMessageSerializer(this.channelId, PreparePaxosMessage.MESSAGE_ID, PreparePaxosMessage.serializer);
            registerMessageSerializer(this.channelId, PromisePaxosMessage.MESSAGE_ID, PromisePaxosMessage.serializer);
            registerMessageSerializer(this.channelId, AcceptPaxosMessage.MESSAGE_ID, AcceptPaxosMessage.serializer);
            registerMessageSerializer(this.channelId, LearnPaxosMessage.MESSAGE_ID, LearnPaxosMessage.serializer);
            registerMessageSerializer(this.channelId, NewNodeMessage.MSG_ID, NewNodeMessage.serializer);
            registerMessageSerializer(this.channelId, DecidedMessage.MESSAGE_ID, DecidedMessage.serializer);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }

    public void uponNewNodeMessage(NewNodeMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Self:[{}] Received new node message from {}", self, msg);

        this.membership = new LinkedList<>(msg.getUpdatedMembership());
        NodeDecidedNotification nodeDecidedNotification = new NodeDecidedNotification(msg.getOperationId(),
                NodeDecidedNotification.OperationType.JOIN, NodeDecidedNotification.DecisionType.COMMIT,
                msg.getJoiningNode(), 1, new byte[0]);
        triggerNotification(nodeDecidedNotification);
    }

    public boolean hasQuorum(HashMap<Host, Boolean> promises) {
        logger.info("Promises => {}", promises.size());
        logger.info("Membership => {}", membership.size());
        return promises.size() > membership.size() / 2;
        // int positiveResponses = (int) promises.values().stream().filter(v ->
        // v).count();
        // int size = membership.size();
        // return positiveResponses > size / 2;
    }

    private void cleanProposes(UUID operationID) {
        this.listOfProposes.remove(operationID);
    }

    private void notifyDecisionToStateMachine(UUID operationId) {
        DecidedNotification decidedNotification = new DecidedNotification(operationId,
                DecidedNotification.DecisionType.COMMIT, this.membership);

        logger.info("Self:[{}] Notifying StateMachine of Decision for {}", self, operationId);
        triggerNotification(decidedNotification);
    }
}
