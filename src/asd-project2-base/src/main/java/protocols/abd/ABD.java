package protocols.abd;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.abd.messages.Ack;
import protocols.abd.notifications.ChannelReadyNotification;
import protocols.abd.notifications.JoinedNotification;
import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest;
import protocols.abd.timer.StartOperationTimer;
import protocols.abd.messages.ReadMessage;
import protocols.abd.messages.ReadReply;
import protocols.abd.messages.ReadTag;
import protocols.abd.requests.AddReplicaRequest;
import protocols.abd.requests.RemoveReplicaRequest;
import protocols.abd.messages.ReadTagReply;
import protocols.abd.messages.WriteMessage;
import protocols.abd.notifications.ReadCompleteNotification;
import protocols.abd.notifications.WriteCompleteNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
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
    private static final long RETRY_INTERVAL = 1000;

    private int channelID;

    // Boolean to verify if current replica is currently mid-instance
    private boolean inOperation;
    private Host myself;
    private Set<Host> membership;

    private Pair<UUID, byte[]> pending;
    private Map<char[], byte[]> val;
    // Map of tags associated with each key
    private Map<char[], Pair<Integer, Host>> tag;

    // Holds the current quorum of the write operation
    private HashSet<Pair<Integer, Host>> answersReadTag;
    // Holds the current quorum of the read operation
    private HashSet<ReadReply> answersReadReply;
    // Set containing the ack messages for the current instance
    private HashSet<Host> answersAck;


    // Holds the pending requests
    // When receiving request to join another replica, locally make the request here
    private Queue<Pair<Integer, ProtoRequest>> pendingRequests;

    // Current instance of the protocol
    private int sequenceNumOp;

    // Keeps track of how many operations it has done
    private final static int MAX_OPERATION_STREAK = 5;
    private int currentOpStreak = 0;


    /**
     * System always assumed to start with 3 replicas.
     * When adding replicas, add AddReplicaRequest to the pending queue
     * Questions:
     * How to deal with replicas "locking" the operation
     * to themselves ? (CoolDown mechanism Idk)
     *
     * How to deal with violation of linearizability?
     * -    Reads (look for smaller instances of a pending write ?)
     *
     * @param props properties of the current replica
     * @throws IOException
     * @throws HandlerRegistrationException
     */

    public ABD(Properties props) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);


    }

    private void registerHandlers() throws HandlerRegistrationException {
        /*------------------------------ Register Timer Handlers -------------------------------------------- */
        registerTimerHandler(StartOperationTimer.TIMER_ID, this::uponStartOperation);
        setupPeriodicTimer(new StartOperationTimer(), RETRY_INTERVAL, RETRY_INTERVAL);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Request Handlers ------------------------------------------ */
//        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
//        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
//        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);
        registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Notification Handlers -------------------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Message Message Serializers -------------------------------- */
//        registerMessageSerializer(channelID, AddReplicaMessage.MSG_ID, AddReplicaMessage.serializer);
        registerMessageSerializer(channelID, ReadTagReply.MSG_ID, ReadTagReply.serializer);
        registerMessageSerializer(channelID, ReadTag.MSG_ID, ReadTag.serializer);
        registerMessageSerializer(channelID, WriteMessage.MSG_ID, WriteMessage.serializer);
        registerMessageSerializer(channelID, Ack.MSG_ID, Ack.serializer);
        registerMessageSerializer(channelID, ReadMessage.MSG_ID, ReadMessage.serializer);
        registerMessageSerializer(channelID, ReadReply.MSG_ID, ReadReply.serializer);
//        /*--------------------------------------------------------------------------------------------------- */

        /*------------------------------ Register Message Message Handlers ----------------------------------- */
        try {
//            registerMessageHandler(channelID, AddReplicaMessage.MSG_ID, this::uponAddReplicaMessage, this::uponMsgFail);
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
        logger.info("Initializing ABD", myself);
        Properties channelProps = new Properties();

        int port = Integer.parseInt(props.getProperty("p2p_port"));

        // Creating TCP channel
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("p2p_port"));
        this.channelID = createChannel(TCPChannel.NAME, channelProps);

        // Register Handlers
        registerHandlers();

        // Initialize stuff
        inOperation = false;
        pendingRequests = new LinkedList<>();
        myself = new Host(
                InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("p2p_port"))
        );
        membership = new HashSet<>();
        tag = new HashMap<>();
        val = new HashMap<>();
        answersAck = new HashSet<>();
        answersReadTag = new HashSet<>();
        answersReadReply = new HashSet<>();
        pending = null;

        // Initial membership
        String[] membershipStr = props.getProperty("initial_membership").split(",");
        for (String s : membershipStr) {
            String ipAdr = s.split(":")[0];
            int p = Integer.parseInt(s.split(":")[1]);
            if (p != port) {
                Host h = new Host(InetAddress.getByName(ipAdr), p);
                membership.add(h);
            }
        }
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
//        int cId = notification.getChannelId();
//        myself = notification.getMyself();
//        logger.info("Channel {} created, I am {}", cId, myself);
//        // Allows this protocol to receive events from this channel.
//        registerSharedChannel(cId);
//        /*---------------------- Register Message Serializers ---------------------- */
//        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
//        registerMessageSerializer(cId, SendTag.MSG_ID, SendTag.serializer);
//        registerMessageSerializer(cId, ReadTag.MSG_ID, ReadTag.serializer);
//        /*---------------------- Register Message Handlers -------------------------- */
//        try {
//            registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
//            registerMessageHandler(cId, SendTag.MSG_ID, this::uponSendTag, this::uponMsgFail);
//            registerMessageHandler(cId, ReadTag.MSG_ID, this::uponReadTag, this::uponMsgFail);
//        } catch (HandlerRegistrationException e) {
//            throw new AssertionError("Error registering message handler.", e);
//        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
//        //We joined the system and can now start doing things
//        joinedInstance = notification.getJoinInstance();
//        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", sequenceNumOp, membership);
    }

    /**
     * What happens if a message fails ?
     *
     * @param msg that failed
     * @param host to who was supposed to be sent
     * @param destProto protocol destination
     * @param throwable what to throw
     * @param channelId to use
     */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /**
     * Upon receiving a Write request from a client
     *
     * @param request received
     * @param sourceProto of the sender protocol
     */
    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        logger.info("[{}]Received {} from application", myself, request);
        pendingRequests.add(Pair.of(sequenceNumOp, request));
    }

    /**
     * Upon receiving a Read request from a client
     *
     * @param request received
     * @param sourceProto of the sender protocol
     */
    private void uponReadRequest(ReadRequest request, short sourceProto) {
        logger.info("[{}]Received {} from application", myself, request);
        pendingRequests.add(Pair.of(sequenceNumOp, request));
    }

    /**
     * Timer used to execute queued operations
     *
     * @param protoTimer timer
     * @param l dunno
     */
    private void uponStartOperation(ProtoTimer protoTimer, long l) {
        if(inOperation) {
            logger.info("[{}] Still in previous operation, trying next window", myself);
            return;
        }

        // Let's give opportunity to other replicas - skip one timer cycle
        if(currentOpStreak >= MAX_OPERATION_STREAK) {
            logger.info("[{}] Already made to many consecutive operations", myself);
            currentOpStreak = 0;
            return;
        }
        if (!pendingRequests.isEmpty()) {
            // We have something, let's propose
            inOperation = true;
            Pair<Integer, ProtoRequest> request = pendingRequests.peek();
            startOperation(request);
        }
    }

    /**
     * Starts the queued Write operation
     *
     * @param request operation
     */
    private void startWriteOperation(WriteRequest request) {
        // Also stores the operation UUID for returning it later
        inOperation = true;
        pending = Pair.of(request.getOpId(), request.getData());

        // Create tag to broadcast
        Pair<Integer, Host> tag = Pair.of(sequenceNumOp, myself);
        answersReadTag.add(tag);
        ReadTag readTag = new ReadTag(sequenceNumOp, request.getKey());

        // TODO - Replace by reliable broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(readTag, peer);
        }
    }

    private void startReadOperation(ReadRequest request) {
        ReadMessage rm = new ReadMessage(sequenceNumOp, request.getKey());
        pending = Pair.of(request.getOpId(), null);

        // TODO - Replace by reliable broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(rm, peer);
        }

        // Add my read reply to the answers set -- won't send to myself
        ReadReply rp = new ReadReply(
                sequenceNumOp,
                request.getKey(),
                tag.get(request.getKey()),
                val.get(request.getKey())
        );
        answersReadReply.add(rp);
    }

    private void startOperation(Pair<Integer, ProtoRequest> request) {
        if(request.getRight() instanceof WriteRequest) {
            startWriteOperation( (WriteRequest) request.getRight());
        } else if (request.getRight() instanceof ReadRequest) {
            startReadOperation( (ReadRequest) request.getRight());
        } else if (request.getRight() instanceof AddReplicaRequest) {

        } else if (request.getRight() instanceof RemoveReplicaRequest) {

        } else {
            throw new RuntimeException("Unknown request: " + request);
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
        logger.info("Received {} from {}", msg, host);
        inOperation = true;
        // We can reset the operation streak, because we've received from another replica
        currentOpStreak = 0;
        char[] key = msg.getKey();
        Pair<Integer, Host> pair = tag.get(key);
        if (pair == null) {
            // 0 means nothing was written yet.
            pair = Pair.of(0, myself);

        }
        // Send our tag for the key requested
        ReadTagReply readTagReply = new ReadTagReply(pair, msg.getOpSec(), key);
        openConnection(host);
        sendMessage(readTagReply, host);
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
     * When receiving a tag requested earlier on {@link ABD#uponWriteRequest(WriteRequest, short)}.
     *
     * @param msg with the tags
     * @param host who send the message
     * @param sourceProto id of the requester protocol
     * @param channelId to send the message
     */
    private void uponReadTagReply(ReadTagReply msg, Host host, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, host);
        if (sequenceNumOp == msg.getPeerOpID()) {
            answersReadTag.add(msg.getTag());
            // We have enough answers for a quorum
            if (answersReadTag.size() >= (membership.size()+1)/2) {
                int new_tag = getMaxSQTag(answersReadTag);
                sequenceNumOp++;
                answersReadTag.clear();
                WriteMessage wm = new WriteMessage(
                        sequenceNumOp,
                        msg.getKey(),
                        Pair.of(new_tag+1, myself),
                        pending.getRight()
                );
                for (Host peer : membership) {
                    openConnection(peer);
                    sendMessage(wm, peer);
                }
                // Write here - not sending to myself
                tag.put(wm.getKey(), wm.getTag());
                val.put(wm.getKey(), wm.getData());
                answersAck.add(myself);
                // pending is now null - still saves the UUID for replying to the application
                pending = Pair.of(pending.getLeft(), null);
            }
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
        logger.info("Received {} from {}", msg, host);
        Pair<Integer, Host> pair = tag.get(msg.getKey());
        if (pair == null || msg.getTag().getLeft() > pair.getLeft()) {
            tag.put(msg.getKey(), msg.getTag());
            val.put(msg.getKey(), msg.getData());
        }
        openConnection(host);
        Ack ack = new Ack(msg.getOpSeq(), msg.getKey());
        sendMessage(ack, host);
        inOperation=false;
    }

    /**
     * When receiving an ACK from an operation (Read/Write).
     *
     * @param msg ack
     * @param host who send the message
     * @param sourceProto id of the requester protocol
     * @param channelId to send the message
     */
    private void uponAck(Ack msg, Host host, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, host);
        if (msg.getOpSeq() == sequenceNumOp) {
            answersAck.add(host);
            if (answersAck.size() >= (membership.size()+1)/2 + 1) {
                answersAck.clear();
                // Check weather if the current operation is a Write or a read
                if (pending.getRight() == null) {
                    logger.info("[{}]Triggered Write Complete notification", myself);
                    triggerNotification(new WriteCompleteNotification(
                            pending.getLeft(),
                            msg.getKey(),
                            val.get(msg.getKey()))
                    );
                } else {
                    logger.info("[{}]Triggered Read Complete notification", myself);
                    System.out.println(pending.getLeft());
                    System.out.println(pending.getRight());
                    triggerNotification(new ReadCompleteNotification(
                            msg.getKey(),
                            pending.getRight(),
                            pending.getLeft())
                    );
                }
                // Reset pending
                pending = null;
                inOperation = false;
                pendingRequests.poll();
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
        logger.info("Received {} from {}", msg, host);
        Pair<Integer, Host> tagSend = tag.get(msg.getKey());
        byte[] valToSend = val.get(msg.getKey());
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

    /**
     * Returns the ReadReply with the higher tag from all the ReadReply messages received from the quorum.
     *
     * @param answers received from the quorum
     * @return the higher tag
     */
    private ReadReply getMaxReply(HashSet<ReadReply> answers) {
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
        logger.info("Received {} from {}", msg, host);
        if(msg.getPeerOpID() == sequenceNumOp) {
            answersReadReply.add(msg);
            if(answersReadReply.size() >= (membership.size()+1)/2 + 1) {
                ReadReply msgMax = getMaxReply(answersReadReply);
                if(msgMax.getTag() == null) {
                    // TODO - Key not found, what to do ?
                    logger.info("Key not found");

                } else {
                    Pair<Integer, Host> newTag = msgMax.getTag();
                    // Update the current pending value being read
                    UUID cur = pending.getLeft();
                    pending = Pair.of(cur, msgMax.getValue());
                    sequenceNumOp++;
                    WriteMessage wm = new WriteMessage(sequenceNumOp, msgMax.getKey(), newTag, pending.getRight());

                    // TODO - replace by reliable broadcast
                    for(Host peer : membership) {
                        openConnection(peer);
                        sendMessage(wm, peer);
                    }
                }
            }
            answersReadTag.clear();
        }
    }

}
