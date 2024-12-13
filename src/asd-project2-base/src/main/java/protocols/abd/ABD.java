package protocols.abd;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.abd.messages.membership.*;
import protocols.abd.messages.writeread.*;
import protocols.abd.operation.MembershipOperation;
import protocols.abd.operation.Operation;
import protocols.abd.operation.ReadWriteOperation;
import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest;
import protocols.abd.timer.HealthCheckTimer;
import protocols.abd.timer.StartOperationTimer;
import protocols.abd.requests.MembershipRequest;
import protocols.abd.notifications.ReadCompleteNotification;
import protocols.abd.notifications.WriteCompleteNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODOs
 * TODO - membership from scratch
 * TODO
 *  - periodic timer to manage instances
 *  - If one missed ack, ask to remove instance
 * TODO - when receiving a null value, delete the entry (?)
 */

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
    // Interval to execute pending operations
    private static final long RETRY_INTERVAL = 50;
    // Interval to print the current state of the replica  - for debugging.
    private static final long STATUS_LOG = 10000;
    // Interval for HEALTH_CHECK - if a replica needs to be removed.
    private static final long HEALTH_CHECK = 15000;

    private int channelID;

    private Host myself;
    /**
     * Membership replicated
     */
    // Equivalent to state in ABD
    private Set<Host> membership;
    private Set<Host> currentlyAlive;
    // Holds the replicas yet to join
    private Set<Host> pendingMembership;
    // Equivalent to tag in ABD
    private Pair <Integer, Host> membershipTag;
    private Boolean ready;
    private Boolean inMembershipOperation;
    private Boolean heathCheckComplete;
    private Boolean isLeader;

    /**
     * State Replicated
     */
    // The replicated key-value map
    private Map<String, byte[]> val;
    // Map of tags associated with each key
    private Map<String, Pair<Integer, Host>> tag;

    /**
     * Queues
     */
    // Maps the keys of the operations that are currently being processed
    private ConcurrentHashMap<String, Operation> inProgressOperations;
    // Holds the pending operations that have yet to be executed
    private ConcurrentLinkedDeque<Pair<String, Operation>> pendingOperations;

    // Unique Sequence number of operation of this process
    private AtomicInteger opSeq;

    // Keeps track of how many operations it has done
    private final static int MAX_CONCURRENT_OPERATIONS = 20;

    /**
     * System always assumed to start with 3 replicas.
     *
     * @param props properties of the current replica
     * @throws IOException
     * @throws HandlerRegistrationException
     */

    public ABD(Properties props) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.opSeq = new AtomicInteger(-1);
    }

    private void registerHandlers() throws HandlerRegistrationException {
        /*------------------------------ Register Timer Handlers -------------------------------------------- */
        registerTimerHandler(StartOperationTimer.TIMER_ID, this::uponStartOperation);
        setupPeriodicTimer(new StartOperationTimer(), RETRY_INTERVAL, RETRY_INTERVAL);
        registerTimerHandler(HealthCheckTimer.TIMER_ID, this::uponStartHealthCheck);
        setupPeriodicTimer(new HealthCheckTimer(), HEALTH_CHECK, HEALTH_CHECK);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Request Handlers ------------------------------------------ */
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);
        registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Message Message Serializers -------------------------------- */
        registerMessageSerializer(channelID, Ping.MSG_ID, Ping.serializer);
        registerMessageSerializer(channelID, Pong.MSG_ID, Pong.serializer);

        registerMessageSerializer(channelID, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelID, ReadTagMembership.MSG_ID, ReadTagMembership.serializer);
        registerMessageSerializer(channelID, ReadTagReplyMembership.MSG_ID, ReadTagReplyMembership.serializer);
        registerMessageSerializer(channelID, WriteMessageMembership.MSG_ID, WriteMessageMembership.serializer);
        registerMessageSerializer(channelID, AckMembership.MSG_ID, AckMembership.serializer);
        registerMessageSerializer(channelID, JoinReply.MSG_ID, JoinReply.serializer);
        registerMessageSerializer(channelID, JoinnedMessage.MSG_ID, JoinnedMessage.serializer);

        registerMessageSerializer(channelID, ReadTagReply.MSG_ID, ReadTagReply.serializer);
        registerMessageSerializer(channelID, ReadTag.MSG_ID, ReadTag.serializer);
        registerMessageSerializer(channelID, WriteMessage.MSG_ID, WriteMessage.serializer);
        registerMessageSerializer(channelID, Ack.MSG_ID, Ack.serializer);
        registerMessageSerializer(channelID, ReadMessage.MSG_ID, ReadMessage.serializer);
        registerMessageSerializer(channelID, ReadReply.MSG_ID, ReadReply.serializer);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Message Message Handlers ----------------------------------- */
        try {
            registerMessageHandler(channelID, Ping.MSG_ID, this::uponPingMessage, this::uponMsgFail);
            registerMessageHandler(channelID, Pong.MSG_ID, this::uponPongMessage, this::uponMsgFail);
            registerMessageHandler(channelID, JoinMessage.MSG_ID, this::uponJoinMessage, this::uponMsgFail);
            registerMessageHandler(channelID, ReadTagMembership.MSG_ID, this::uponReadTagMembership, this::uponMsgFail);
            registerMessageHandler(channelID, ReadTagReplyMembership.MSG_ID, this::uponReadTagReplyMembership, this::uponMsgFail);
            registerMessageHandler(channelID, WriteMessageMembership.MSG_ID, this::uponWriteMessageMembership, this::uponMsgFail);
            registerMessageHandler(channelID, AckMembership.MSG_ID, this::uponAckMembership, this::uponMsgFail);
            registerMessageHandler(channelID, JoinReply.MSG_ID, this::uponJoinReply, this::uponMsgFail);
            registerMessageHandler(channelID, JoinnedMessage.MSG_ID, this::uponJoinnedMessage, this::uponMsgFail);

            registerMessageHandler(channelID, ReadTagReply.MSG_ID, this::uponReadTagReply, this::uponMsgFail);
            registerMessageHandler(channelID, ReadTag.MSG_ID, this::uponReadTag, this::uponMsgFail);
            registerMessageHandler(channelID, WriteMessage.MSG_ID, this::uponWriteMessage, this::uponMsgFail);
            registerMessageHandler(channelID, Ack.MSG_ID, this::uponAck, this::uponMsgFail);
            registerMessageHandler(channelID, ReadMessage.MSG_ID, this::uponReadMessage, this::uponMsgFail);
            registerMessageHandler(channelID, ReadReply.MSG_ID, this::uponReadReply, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
        /*--------------------------------------------------------------------------------------------------- */
    }

    // Synchronized for shared thread states.

    private synchronized void setReady(boolean isReady) {
        this.ready = isReady;
    }

    private synchronized boolean isReady() {
        return this.ready;
    }

    private synchronized boolean isMembershipOperation() { return this.inMembershipOperation; }

    private synchronized void setIsMembershipOperation(boolean isMembershipOperation) {
        this.inMembershipOperation = isMembershipOperation;
    }

    private synchronized boolean inHealthCheck() {
        return this.heathCheckComplete;
    }

    private synchronized void setIsHealthCheckComplete(boolean inHealthCheck) {
        this.heathCheckComplete = inHealthCheck;
    }

    private synchronized void setIsLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    private synchronized boolean isLeader() {
        return this.isLeader;
    }

    private synchronized void updateTagAndValue(String key, Pair<Integer, Host> newTag, byte[] data) {
        tag.put(key, newTag);
        val.put(key, data);
    }

    @Override
    public void init(Properties props) throws IOException, HandlerRegistrationException {
        Properties channelProps = new Properties();

        int port = Integer.parseInt(props.getProperty("p2p_port"));

        // Creating TCP channel
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("p2p_port"));
        this.channelID = createChannel(TCPChannel.NAME, channelProps);

        // Register Handlers
        registerHandlers();

        // Initialize stuff
        this.opSeq.incrementAndGet();
        myself = new Host(
                InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("p2p_port"))
        );

        logger.info("[{}] Initializing ABD", myself);

        // State of the replica
        membership = Collections.newSetFromMap(new ConcurrentHashMap<>());
        currentlyAlive = Collections.newSetFromMap(new ConcurrentHashMap<>());
        pendingMembership = Collections.newSetFromMap(new ConcurrentHashMap<>());
        tag = new ConcurrentHashMap<>();
        val = new ConcurrentHashMap<>();
        inProgressOperations = new ConcurrentHashMap<>();
        pendingOperations = new ConcurrentLinkedDeque<>();
        setIsMembershipOperation(false);
        setIsHealthCheckComplete(false);
        setIsLeader(false);

        // Initial membership
        if(props.getProperty("contact") == null) {
            String[] membershipStr = props.getProperty("initial_membership").split(",");
            int i = 0;
            for (String s : membershipStr) {
                String ipAdr = s.split(":")[0];
                int p = Integer.parseInt(s.split(":")[1]);
                if (p != port) {
                    Host h = new Host(InetAddress.getByName(ipAdr), p);
                    membership.add(h);
                    currentlyAlive.add(h);
                }
                // If it's the first one
                else if (i == 0) {
                    setIsLeader(true);
                }
                i++;
            }
            membershipTag = Pair.of(0, myself);
            setReady(true);
            logger.info("[{}] is Ready!", myself);
        } else {
            // Request to join
            String c = props.getProperty("contact");
            String ipAdr = c.split(":")[0];
            int p = Integer.parseInt(c.split(":")[1]);
            Host contact = new Host(InetAddress.getByName(ipAdr), p);
            JoinMessage jm = new JoinMessage(myself);
            openConnection(contact);
            sendMessage(jm, contact);
            setReady(false);
        }
    }

    /**
     * What happens if a message fails
     *
     * @param msg that failed
     * @param host to who was supposed to be sent
     * @param destProto protocol destination
     * @param throwable what to throw
     * @param channelId to use
     */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", myself, msg, host, throwable);
    }

    /**
     * Upon receiving a join request from another replica
     * Membership operations always go the front
     *
     * @param msg to join
     * @param host to add to quorum
     * @param sourceProto of the sender protocol
     * @param channelId used to communicate
     */
    private void uponJoinMessage(JoinMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("[{}] {} is trying to join the system", myself, msg.getMyself());
        // Used to know the operation
        UUID uuid = UUID.randomUUID();
        Operation op = new MembershipOperation(new MembershipRequest(
                msg.getMyself()),
                opSeq.incrementAndGet(),
                uuid,
                msg.getMyself(),
                Action.JOIN
        );
        pendingOperations.addFirst(Pair.of(uuid.toString(), op));
    }

    /**
     * Upon receiving a Write request from a client
     *
     * @param request received
     * @param sourceProto of the sender protocol
     */
    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        if(ready) {
            logger.info("[{}] Received {} from application", myself, request);
            Operation op = new ReadWriteOperation(request, opSeq.incrementAndGet());
            pendingOperations.add(Pair.of(new String(request.getKey()), op));
        }
    }

    /**
     * Upon receiving a Read request from a client
     *
     * @param request received
     * @param sourceProto of the sender protocol
     */
    private void uponReadRequest(ReadRequest request, short sourceProto) {
        if(ready) {
            logger.info("[{}] Received {} from application", myself, request);
            Operation op = new ReadWriteOperation(request, opSeq.incrementAndGet());
            pendingOperations.add(Pair.of(new String(request.getKey()), op));
        }
    }

    /**
     * Starts healthcheck (Runs only on leader)
     *
     * @param protoTimer
     * @param l
     */
    private void uponStartHealthCheck(ProtoTimer protoTimer, long l) {
        if(isReady()) {
            if (!isMembershipOperation() && !inHealthCheck()) {
                Set<Host> missingElements = new HashSet<>(membership);
                missingElements.removeAll(currentlyAlive);
                logger.info("[{}] Starting Health Check", myself);
                if (!missingElements.isEmpty()) {
                    for (Host host : missingElements) {
                        logger.info("[{}] {} is not responding", myself, host);
                        // Used to know the operation
                        UUID uuid = UUID.randomUUID();
                        Operation op = new MembershipOperation(new MembershipRequest(
                                host),
                                opSeq.incrementAndGet(),
                                uuid,
                                host,
                                Action.REMOVE
                        );
                        pendingOperations.addFirst(Pair.of(uuid.toString(), op));
                    }
                } else {
                    // TODO - replace by reliable broadcast
                    Ping ping = new Ping();
                    for (Host host : membership) {
                        openConnection(host);
                        sendMessage(ping, host);
                    }
                }
                currentlyAlive.clear();
            }
        }
        // Try to rejoin
        else {
            if (!membership.isEmpty()) {
                Host contact = membership.iterator().next();
                openConnection(contact);
                sendMessage(new JoinMessage(myself), contact);
            }
        }
    }

    /**
     * When receiving a ping message for health check
     */
    private void uponPingMessage(Ping msg, Host host, short sourceProto, int channelId) {
        openConnection(host);
        sendMessage(new Pong(), host);

    }

    /**
     * When receiving a ping message for healthCheck.
     */
    private void uponPongMessage(Pong msg, Host host, short sourceProto, int channelId) {
        currentlyAlive.add(host);
    }




    /**
     * Timer used to execute queued operations
     * Membership operations only start after there are no inProgressOperations (can't stop them).
     * Once a membership operation is in progress, other pending operations are blocked.
     *
     * @param protoTimer timer
     */
    private void uponStartOperation(ProtoTimer protoTimer, long l) {
        if (isReady()) {
            if (!pendingOperations.isEmpty() && !isMembershipOperation()) {
                // Remove right away the pending operation (not the best idea)
                Pair<String, Operation> p = pendingOperations.poll();
                String key = p.getLeft();
                Operation op = p.getRight();

                if(!inProgressOperations.containsKey(key)) {
                    if (inProgressOperations.size() <= MAX_CONCURRENT_OPERATIONS) {
                        logger.info("[{}] Inserted key {} for request {} on inProgressOperations Map",
                                myself,
                                key,
                                op.getRequest()
                        );
                        inProgressOperations.put(key, op);
                        int seqNum = opSeq.get();
                        startOperation(Pair.of(seqNum, op), key);

                    } else {
                        logger.info("[{}] Maximum concurrent operations reached", myself);
                    }
                } else {
                    logger.info("[{}] Current key is mid operation", myself);
                }
            }
        }
    }

    private synchronized void waitOtherOperations(String key) {
        // Probably could get away with just checking the size. (Sorry complexity)
        while (!(inProgressOperations.containsKey(key) && inProgressOperations.size() == 1)) {
            try {
                wait(); // Wait until notified that inProgressOperations is empty
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore the interrupted status
                throw new RuntimeException("Operation interrupted", e);
            }
        }
    }


    /**
     * Used to parse the current pending operation.
     * Membership operations only start if all inProgress operations are done.
     *
     * @param request
     */
    private synchronized void startOperation(Pair<Integer, Operation> request, String key) {
        if (request.getRight().getRequest() instanceof WriteRequest) {
            startWriteOperation((ReadWriteOperation) request.getRight(), request.getLeft());
        } else if (request.getRight().getRequest() instanceof ReadRequest) {
            startReadOperation((ReadWriteOperation) request.getRight(), request.getLeft());
        } else if (request.getRight().getRequest() instanceof MembershipRequest) {
            setIsMembershipOperation(true);
            waitOtherOperations(key);
            startMembershipOperation((MembershipOperation) request.getRight(), request.getLeft());
        }
        else {
            throw new RuntimeException("Unknown request: " + request);
        }
    }
    /**
     * Starts the queued AddReplicaOperation
     *
     * @param op the Add Replica operation
     * @param opNumSeq the seq number of the operation
     */
    private void startMembershipOperation(MembershipOperation op, int opNumSeq) {
        logger.info("[{}] Starting Membership Operation", myself);

        // Updated tag to broadcast
        membershipTag = Pair.of(opNumSeq, myself);
        op.getAnswersReadTag().add(membershipTag);

        ReadTagMembership rt = new ReadTagMembership(opNumSeq, op.getPending().getLeft());

        // TODO - Replace by reliable broadcast
        for (Host peer: membership) {
            openConnection(peer);
            sendMessage(rt, peer);
        }
    }

    /**
     * Starts the queued Write operation
     *
     * @param op the Write operation being executed
     */
    private void startWriteOperation(ReadWriteOperation op, int opNumSeq) {
        // Also stores the operation UUID for returning it later
        WriteRequest request = (WriteRequest) op.getRequest();
        op.setPending(Pair.of(request.getOpId(), request.getData()));

        // Create tag to broadcast - and add to set
        Pair<Integer, Host> tag = Pair.of(opNumSeq, myself);
        op.getAnswersReadTag().add(tag);

        ReadTag readTag = new ReadTag(opNumSeq, request.getKey());

        // TODO - Replace by reliable broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(readTag, peer);
        }
    }

    /**
     * Starts the queued Read operation
     *
     * @param op the Write operation being executed
     */
    private void startReadOperation(ReadWriteOperation op, int opNumSeq) {
        ReadRequest request = (ReadRequest) op.getRequest();
        ReadMessage rm = new ReadMessage(opNumSeq, request.getKey());
        op.setPending(Pair.of(request.getOpId(), null));

        // TODO - Replace by reliable broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(rm, peer);
        }

        // Add my read reply to the answers set -- won't send to myself
        ReadReply rp = new ReadReply(
                opNumSeq,
                request.getKey(),
                tag.get(new String(request.getKey())),
                val.get(new String(request.getKey()))
        );
        op.getAnswersReadReply().add(rp);
    }

    /**
     * When receiving a read tag for membership from a host who received a request to add a replica.<br>
     *
     * @param msg containing the operation seq
     * @param host that sent the message
     */
    private void uponReadTagMembership(ReadTagMembership msg, Host host, short sourceProto, int channelID) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            ReadTagReplyMembership readTagReply = new ReadTagReplyMembership(membershipTag, msg.getOpID());

            openConnection(host);
            sendMessage(readTagReply, host);
        }
    }

    /**
     * When receiving a read tag from a host who received a write request.<br>
     * If no tag for that key exists, sends dummy tag with 0 (it will never be chosen, tags >= 1).
     *
     * @param msg containing the key and the current operation sequence number
     * @param host that sent the message
     */
    private void uponReadTag(ReadTag msg, Host host, short sourceProto, int channelId) {
        if(isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            String key = new String(msg.getKey());
            Pair<Integer, Host> pair = tag.get(key);
            if (pair == null) {
                // 0 means nothing was written yet.
                pair = Pair.of(-1, myself);

            }
            // Send our tag for the key requested
            ReadTagReply readTagReply = new ReadTagReply(pair, msg.getOpSec(), key.toCharArray());
            openConnection(host);
            sendMessage(readTagReply, host);
        }
    }

    /**
     * Returns the max tag integer value from all the tags received.
     *
     * @param answers received from the quorum.
     * @return the max of all tags
     */
    private int getMaxSQTag(HashSet<Pair<Integer, Host>> answers) {
        int max = Integer.MIN_VALUE;
        for (Pair<Integer, Host> pair : answers) {
            if(pair.getKey() > max) {
                max = pair.getLeft();
            }
        }
        return max;
    }

    /**
     * When receiving a ReadTag reply for the membership change previously sent.
     *
     * @param msg containing the operation
     * @param host original sender
     */
    private void uponReadTagReplyMembership(ReadTagReplyMembership msg, Host host, short sourceProto, int channelID) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if(inProgressOperations.containsKey(msg.getPeerOpID().toString())) {
                MembershipOperation op = (MembershipOperation) inProgressOperations.get(msg.getPeerOpID().toString());
                op.getAnswersReadTag().add(msg.getTag());
                // If we are trying to remove one replica, quorum is smaller
                int remove = 0;
                if (op.getAction().equals(Action.REMOVE)) remove = 1;
                if (op.getAnswersReadTag().size() == ((membership.size()+1-remove)/2)+1) {
                    int new_tag = getMaxSQTag(op.getAnswersReadTag());
                    opSeq.incrementAndGet();
                    op.incrementOpSeq();
                    WriteMessageMembership wmm = new WriteMessageMembership(
                            op.getOpSeq(),
                            Pair.of(new_tag+1, myself),
                            op.getPending().getRight(),
                            msg.getPeerOpID(),
                            op.getAction()
                    );

                    for (Host peer : membership) {
                        openConnection(peer);
                        sendMessage(wmm, peer);
                    }

                    op.getAnswersAck().add(myself);
                }
            }
        }
    }

    /**
     * When receiving a tag requested earlier on {@link ABD#uponWriteRequest(WriteRequest, short)}.
     *
     * @param msg with the tags
     * @param host who send the message
     * @param sourceProto id of the requester protocol
     * @param channelId to send the message
     */
    private void uponReadTagReply(ReadTagReply msg, Host host, short sourceProto, int channelId) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            // Otherwise, probably an old ack
            if (inProgressOperations.containsKey(new String(msg.getKey())) ){
                ReadWriteOperation op = (ReadWriteOperation)inProgressOperations.get(new String(msg.getKey()));
                op.getAnswersReadTag().add(msg.getTag());
                // We have enough answers for a quorum
                if (op.getAnswersReadTag().size() == ((membership.size() + 1)/2)+1) {
                    logger.info("[{}] Operation: {}", myself, op);
                    int new_tag = getMaxSQTag(op.getAnswersReadTag());
                    opSeq.incrementAndGet();
                    op.incrementOpSeq();
                    WriteMessage wm = new WriteMessage(
                            op.getOpSeq(),
                            msg.getKey(),
                            Pair.of(new_tag + 1, myself),
                            op.getPending().getRight()
                    );
                    for (Host peer : membership) {
                        openConnection(peer);
                        sendMessage(wm, peer);
                    }

                    // Write here - not sending to myself
                    logger.info("[{}] Updating values", myself);
                    tag.put(new String(wm.getKey()), wm.getTag());
                    val.put(new String(wm.getKey()), wm.getData() == null ? new byte[0] : wm.getData());

                    op.getAnswersAck().add(myself);
                    // pending is now null - still saves the UUID for replying to the application
                    op.setPending(Pair.of(op.getPending().getLeft(), null));
                }
            }
        }
    }

    /**
     * When receiving a WriteMessage to change the membership
     *
     * @param msg received
     * @param host of the original sender
     */
    private void uponWriteMessageMembership(WriteMessageMembership msg, Host host, short sourceProto, int channelID) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            this.membershipTag = msg.getTag();
            switch (msg.getAction()) {
                case JOIN:
                    pendingMembership.add(msg.getReplica());
                    break;
                case REMOVE:
                    if(msg.getReplica().equals(myself)) {
                        setReady(false);
                        return;
                    }
                    membership.remove(msg.getReplica());
                    break;
            }

            AckMembership ack = new AckMembership(
                    msg.getOpSeq(),
                    msg.getOpID(),
                    msg.getAction()
            );

            openConnection(host);
            sendMessage(ack, host);
        }
    }

    /**
     * When receiving WriteMessage, which instructs the replica to write the new value and new tag.
     *
     * @param msg containing the key, value and tag of the message.
     * @param host who send the message
     * @param sourceProto id of the requester protocol
     * @param channelId to send the message
     */
    public void uponWriteMessage(WriteMessage msg, Host host, short sourceProto, int channelId) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);

            synchronized (this) {
                Pair<Integer, Host> currentTag = tag.get(new String(msg.getKey()));
                if (currentTag == null || msg.getTag().getLeft() > currentTag.getLeft()) {
                    updateTagAndValue(new String(msg.getKey()), msg.getTag(), msg.getData());
                }
            }

            openConnection(host);
            Ack ack = new Ack(msg.getOpSeq(), msg.getKey());
            sendMessage(ack, host);
        }
    }

    /**
     * When receiving a JoinReply with the current membership
     * All Reads and Writes are propagated, no need to share state.
     *
     * @param msg with the membership and membershipTag
     * @param host original sender
     */
    private void uponJoinReply(JoinReply msg, Host host, short sourceProto, int channelId) {

        this.membership = msg.getMembership();
        this.membershipTag = msg.getMembershipTag();
        this.currentlyAlive.addAll(this.membership);

        JoinnedMessage jm = new JoinnedMessage(opSeq.get(), msg.getOpID());

        // TODO - replace by reliable broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(jm, peer);
        }

        setReady(true);

        logger.info("[{}] Joinned the system", myself);
    }

    /**
     * When receiving a JoinnedMessage from a freshly joined replica. It updates the membership.
     */
    private void uponJoinnedMessage(JoinnedMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("[{}] Received {} from {}", myself, msg, host);
        inProgressOperations.remove(msg.getOpID().toString());
        pendingMembership.remove(host);
        membership.add(host);
        currentlyAlive.add(host);
    }

    /**
     * Ack membership
     * @param msg received with ack of membership operation
     * @param host who sent the msg
     */
    private void uponAckMembership(AckMembership msg, Host host, short sourceProto, int channelId) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if(inProgressOperations.containsKey(msg.getOpID().toString())) {
                MembershipOperation op = (MembershipOperation) inProgressOperations.get(msg.getOpID().toString());
                if (msg.getOpSeq() == op.getOpSeq()) {
                    op.getAnswersAck().add(host);
                    if (op.getAnswersAck().size() == (membership.size()+1)/2 + 1) {
                        switch (msg.getAction()) {
                            case JOIN:
                                logger.info("[{}] Sending state to new replica", myself);
                                pendingMembership.add(op.getPending().getRight());
                                // Membership still doesn't have new replica
                                JoinReply jr = new JoinReply(this.membership, membershipTag, msg.getOpID());
                                openConnection(op.getPending().getRight());
                                sendMessage(jr, op.getPending().getRight());
                                break;

                            case REMOVE:
                                logger.info("[{}] Remove replica", myself);
                                membership.remove(op.getPending().getRight());
                                break;
                        }
                        synchronized (this) {
                            inProgressOperations.remove(msg.getOpID().toString());
                            setIsHealthCheckComplete(true);
                            setIsMembershipOperation(false);
                            notifyAll(); // Wake up threads waiting for operations to complete
                        }
                    }
                }
            }
        }
    }

    /**
     * When receiving an ACK from an operation (Read/Write/Membership).
     * Must filter membership operations is instance of
     *
     * @param msg ack
     * @param host who send the message
     * @param sourceProto id of the requester protocol
     * @param channelId to send the message
     */
    private void uponAck(Ack msg, Host host, short sourceProto, int channelId) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if(inProgressOperations.containsKey(new String(msg.getKey()))) {
                ReadWriteOperation op = (ReadWriteOperation) inProgressOperations.get(new String(msg.getKey()));
                if (msg.getOpSeq() == op.getOpSeq()) {
                    op.getAnswersAck().add(host);
                    if (op.getAnswersAck().size() == (membership.size()+1)/2 + 1) {
                        if (op.getPending().getRight() == null) {
                            logger.info("[{}] Triggered Write Complete notification", myself);
                            triggerNotification(new WriteCompleteNotification(
                                    op.getPending().getLeft(),
                                    msg.getKey(),
                                    val.get(new String(msg.getKey())))
                            );
                        } else {
                            logger.info("[{}] Triggered Read Complete notification", myself);
                            triggerNotification(new ReadCompleteNotification(
                                    msg.getKey(),
                                    op.getPending().getRight(),
                                    op.getPending().getLeft())
                            );
                        }
                        synchronized (this) {
                            inProgressOperations.remove(new String(msg.getKey()));
                            notifyAll(); // Wake up threads waiting for operations to complete
                        }
                        // TODO - must find a better way to do this - after a timeout - future work (commit phases)
                        // pendingOperations.poll();
                    }
                }
            }
        }
    }

    /**
     * Upon receiving a read message from another replica.<br>
     * Sends latest tag we have associated with the key requested.
     *
     * @param msg containing the key and the operation sequence number
     * @param host - original sender of the messages
     * @param sourceProto - of the protocol from which the message was sent
     * @param channelId - used to send the message
     */
    public void uponReadMessage(ReadMessage msg, Host host, short sourceProto, int channelId) {
        if (isReady()) {
            String key = new String(msg.getKey());
            logger.info("[{}] Received ReadMessage for key: {} from {}", myself, key, host);

            Pair<Integer, Host> tagSend;
            byte[] valToSend;

            synchronized (this) {
                tagSend = tag.getOrDefault(key, Pair.of(0, myself));
                valToSend = val.getOrDefault(key, new byte[0]);
            }

            ReadReply rp = new ReadReply(
                    msg.getOpSeq(),
                    msg.getKey(),
                    tagSend,
                    valToSend
            );

            logger.debug("[{}] Sending ReadReply: {}", myself, rp);
            openConnection(host);
            sendMessage(rp, host);
        }
    }

    /**
     * Returns the ReadReply with the higher tag from all the ReadReply messages received from the quorum.
     *
     * @param answers received from the quorum
     * @return the higher tag
     */
    private ReadReply getMaxReply(Set<ReadReply> answers) {
        return answers.stream()
                .max(Comparator.comparing(reply -> reply.getTag() != null ? reply.getTag().getLeft() : 0))
                .orElseThrow(() -> new IllegalArgumentException("Answers set cannot be empty"));
    }

    /**
     * When receiving a ReadReply message.<br>
     * Waits for a quorum until it.<br>
     * Sends Message to Write the message.<br>
     *
     * @param msg containing the higher tags
     * @param host - original sender of the messages
     * @param sourceProto - of the protocol from which the message was sent
     * @param channelId - used to send the message
     */
    public void uponReadReply(ReadReply msg, Host host, short sourceProto, int channelId) {
        if (isReady()) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if (inProgressOperations.containsKey(new String(msg.getKey()))) {
                ReadWriteOperation op = (ReadWriteOperation)inProgressOperations.get(new String(msg.getKey()));
                if (msg.getPeerOpID() == op.getOpSeq()) {
                    op.getAnswersReadReply().add(msg);
                    if (op.getAnswersReadReply().size() == (membership.size() + 1) / 2 + 1) {
                        ReadReply msgMax = getMaxReply(op.getAnswersReadReply());
                        if (msgMax.getTag() == null) {
                            // No key was found can return zero byte char to the application
                            logger.info("[{}] Triggered Read Complete notification", myself);
                            triggerNotification(new ReadCompleteNotification(
                                    msg.getKey(),
                                    new byte[0],
                                    op.getPending().getLeft())
                            );
                            inProgressOperations.remove(new String(msg.getKey()));
                        } else {
                            Pair<Integer, Host> newTag = msgMax.getTag();
                            // Update the current pending value being read
                            UUID cur = op.getPending().getLeft();
                            op.setPending(Pair.of(cur, msgMax.getValue()));
                            op.incrementOpSeq();
                            opSeq.incrementAndGet();
                            WriteMessage wm = new WriteMessage(op.getOpSeq(), msgMax.getKey(), newTag, op.getPending().getRight());

                            // TODO - replace by reliable broadcast
                            for (Host peer : membership) {
                                openConnection(peer);
                                sendMessage(wm, peer);
                            }
                        }
                    }
                }
            }
        }
    }
}
