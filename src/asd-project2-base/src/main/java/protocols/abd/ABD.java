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
import protocols.abd.timer.StartOperationTimer;
import protocols.abd.requests.AddReplicaRequest;
import protocols.abd.requests.RemoveReplicaRequest;
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
import java.util.concurrent.ConcurrentLinkedQueue;
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
    private static final long RETRY_INTERVAL = 50;

    private int channelID;

    private Host myself;
    /**
     * Membership replicated
     */
    // Equivalent to state in ABD
    private HashSet<Host> membership;
    // Holds the replicas yet to join
    private HashSet<Host> pendingMembership;
    // Equivalent to tag in ABD
    private Pair <Integer, Host> membershipTag;
    private Boolean ready;

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
    private ConcurrentLinkedQueue<Pair<String, Operation>> pendingOperations;

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
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Request Handlers ------------------------------------------ */
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);
        registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Message Message Serializers -------------------------------- */
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

    // Ignoring membership part for now
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

        // Would probably need some concurrent structure for this
        membership = new HashSet<>();
        pendingMembership = new HashSet<>();
        tag = new ConcurrentHashMap<>();
        val = new ConcurrentHashMap<>();
        inProgressOperations = new ConcurrentHashMap<>();
        pendingOperations = new ConcurrentLinkedQueue<>();

        // Initial membership
        if(props.getProperty("contact") == null) {
            String[] membershipStr = props.getProperty("initial_membership").split(",");
            for (String s : membershipStr) {
                String ipAdr = s.split(":")[0];
                int p = Integer.parseInt(s.split(":")[1]);
                if (p != port) {
                    Host h = new Host(InetAddress.getByName(ipAdr), p);
                    membership.add(h);
                }
            }
            membershipTag = Pair.of(0, myself);
            ready = true;
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
            ready = false;
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
     *
     * @param msg to join
     * @param host to add to quorum
     * @param sourceProto of the sender protocol
     * @param channelId used to communicate
     */
    private void uponJoinMessage(JoinMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("{} is trying to join the system", msg.getMyself());
        // Used to know the operation
        UUID uuid = UUID.randomUUID();
        Operation op = new MembershipOperation(new AddReplicaRequest(
                msg.getMyself()),
                opSeq.incrementAndGet(),
                uuid,
                msg.getMyself()
        );
        pendingOperations.add(Pair.of(uuid.toString(), op));
    }

    /**
     * Upon receiving a Write request from a client
     *
     * @param request received
     * @param sourceProto of the sender protocol
     */
    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        if(ready) {
            logger.info("[{}]Received {} from application", myself, request);
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
            logger.info("[{}]Received {} from application", myself, request);
            Operation op = new ReadWriteOperation(request, opSeq.incrementAndGet());
            pendingOperations.add(Pair.of(new String(request.getKey()), op));
        }
    }

    /**
     * Timer used to execute queued operations
     *
     * @param protoTimer timer
     * @param l dunno
     */
    private void uponStartOperation(ProtoTimer protoTimer, long l) {
        if (ready) {
            if (!pendingOperations.isEmpty()) {
                Pair<String, Operation> p = pendingOperations.poll();
                String key = p.getLeft();
                Operation op = p.getRight();
                if(!inProgressOperations.containsKey(key)) {
                    if (inProgressOperations.size() <= MAX_CONCURRENT_OPERATIONS) {
                        logger.info("[{}] Inserted key {} on inProgressOperations Map", myself, key);
                        inProgressOperations.put(key, op);
                        int seqNum = opSeq.get();
                        startOperation(Pair.of(seqNum, op));

                    } else {
                        logger.info("[{}] Maximum concurrent operations reached", myself);
                    }
                } else {
                    logger.info("[{}] Current key is mid operation", myself);
                }
            }
        }
    }

    private void startOperation(Pair<Integer, Operation> request) {

        if(request.getRight().getRequest() instanceof WriteRequest) {
            startWriteOperation((ReadWriteOperation) request.getRight(), request.getLeft());
        }

        else if (request.getRight().getRequest() instanceof ReadRequest) {
            startReadOperation( (ReadWriteOperation) request.getRight(), request.getLeft());
        }

        else if (request.getRight().getRequest() instanceof AddReplicaRequest) {
            startMembershipOperation((MembershipOperation) request.getRight(), request.getLeft());
        }

        else if (request.getRight().getRequest() instanceof RemoveReplicaRequest) {

        } else {
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
        logger.info("[{}]Starting Add Replica Operation", myself);

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
        if (ready) {
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
     * @param sourceProto id of the sending protocol
     * @param channelId used to send the message
     */
    private void uponReadTag(ReadTag msg, Host host, short sourceProto, int channelId) {
        if(ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            String key = new String(msg.getKey());
            Pair<Integer, Host> pair = tag.get(key);
            if (pair == null) {
                // 0 means nothing was written yet.
                pair = Pair.of(0, myself);

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

    private void uponReadTagReplyMembership(ReadTagReplyMembership msg, Host host, short sourceProto, int channelID) {
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            // Probably an old operation
            logger.info("[{}] Upon Read TagReply Membership key: {}", myself);
            if(inProgressOperations.containsKey(msg.getPeerOpID().toString())) {
                MembershipOperation op = (MembershipOperation) inProgressOperations.get(msg.getPeerOpID().toString());
                op.getAnswersReadTag().add(msg.getTag());
                if (op.getAnswersReadTag().size() == ((membership.size()+1)/2)+1) {
                    logger.info("[{}] QUORUM for Operation: {}", myself, op);
                    int new_tag = getMaxSQTag(op.getAnswersReadTag());
                    opSeq.incrementAndGet();
                    op.incrementOpSeq();
                    WriteMessageMembership wmm = new WriteMessageMembership(
                            op.getOpSeq(),
                            Pair.of(new_tag+1, myself),
                            op.getPending().getRight(),
                            msg.getPeerOpID(),
                            Action.JOIN
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
        if (ready) {
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
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            this.membershipTag = msg.getTag();
            switch (msg.getAction()) {
                case JOIN:
                    pendingMembership.add(msg.getReplica());
                    break;
                case REMOVE:
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
    private void uponWriteMessage(WriteMessage msg, Host host, short sourceProto, int channelId) {
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            Pair<Integer, Host> pair = tag.get(new String(msg.getKey()));
            if (pair == null || msg.getTag().getLeft() > pair.getLeft()) {
                tag.put(new String(msg.getKey()), msg.getTag());
                val.put(new String(msg.getKey()), msg.getData());
            }
            openConnection(host);
            Ack ack = new Ack(msg.getOpSeq(), msg.getKey());
            sendMessage(ack, host);
        }
    }

    private void uponJoinReply(JoinReply msg, Host host, short sourceProto, int channelId) {

        this.membership = msg.getMembership();
        this.membershipTag = msg.getMembershipTag();
        this.ready = true;

        JoinnedMessage jm = new JoinnedMessage(opSeq.get());
        // TODO - replace by reliable broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(jm, peer);
        }

        logger.info("[{}] Joinned the system", myself);
    }

    private void uponJoinnedMessage(JoinnedMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("[{}] Received {} from {}", myself, msg, host);
        pendingMembership.remove(host);
        membership.add(host);
    }

    /**
     * Ack membership
     * @param msg received with ack of membership operation
     * @param host who sent the msg
     */
    private void uponAckMembership(AckMembership msg, Host host, short sourceProto, int channelId) {
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if(inProgressOperations.containsKey(msg.getOpID().toString())) {
                MembershipOperation op = (MembershipOperation) inProgressOperations.get(msg.getOpID().toString());
                if (msg.getOpSeq() == op.getOpSeq()) {
                    op.getAnswersAck().add(host);
                    if (op.getAnswersAck().size() == (membership.size()+1)/2 + 1) {
                        logger.info("[{}] Sending state to new replica", myself);
                        switch (msg.getAction()) {
                            case JOIN:
                                pendingMembership.add(op.getPending().getRight());
                                // Membership still doesn't have new replica
                                JoinReply jr = new JoinReply(this.membership, membershipTag);
                                openConnection(op.getPending().getRight());
                                sendMessage(jr, op.getPending().getRight());
                                break;

                            case REMOVE:
                                membership.remove(op.getPending().getRight());
                                break;
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
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if(inProgressOperations.containsKey(new String(msg.getKey()))) {
                ReadWriteOperation op = (ReadWriteOperation) inProgressOperations.get(new String(msg.getKey()));
                if (msg.getOpSeq() == op.getOpSeq()) {
                    op.getAnswersAck().add(host);
                    if (op.getAnswersAck().size() == (membership.size()+1)/2 + 1) {
                        if (op.getPending().getRight() == null) {
                            logger.info("[{}]Triggered Write Complete notification", myself);
                            triggerNotification(new WriteCompleteNotification(
                                    op.getPending().getLeft(),
                                    msg.getKey(),
                                    val.get(new String(msg.getKey())))
                            );
                            inProgressOperations.remove(new String(msg.getKey()));
                        } else {
                            logger.info("[{}]Triggered Read Complete notification", myself);
                            triggerNotification(new ReadCompleteNotification(
                                    msg.getKey(),
                                    op.getPending().getRight(),
                                    op.getPending().getLeft())
                            );
                            inProgressOperations.remove(new String(msg.getKey()));
                        }
                        // TODO - must find a better way to do this
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
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            Pair<Integer, Host> tagSend = tag.get(new String(msg.getKey()));
            byte[] valToSend = val.get(new String(msg.getKey()));
            if(tagSend == null) {
                tagSend = Pair.of(0, myself);
            }

            ReadReply rp = new ReadReply(
                    msg.getOpSeq(),
                    msg.getKey(),
                    tagSend,
                    valToSend
            );

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
        if (ready) {
            logger.info("[{}] Received {} from {}", myself, msg, host);
            if (inProgressOperations.containsKey(new String(msg.getKey()))) {
                ReadWriteOperation op = (ReadWriteOperation)inProgressOperations.get(new String(msg.getKey()));
                if (msg.getPeerOpID() == op.getOpSeq()) {
                    op.getAnswersReadReply().add(msg);
                    if (op.getAnswersReadReply().size() == (membership.size() + 1) / 2 + 1) {
                        ReadReply msgMax = getMaxReply(op.getAnswersReadReply());
                        if (msgMax.getTag() == null) {
                            // TODO - Key not found, what to do ?
                            logger.info("Key not found");

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
                    // TODO - must find a way to do this better
                    // op.getAnswersReadTag().clear();
                }
            }
        }
    }
}
