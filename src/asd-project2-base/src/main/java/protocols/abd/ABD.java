package protocols.abd;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.abd.messages.*;
import protocols.abd.notifications.*;
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

    private int channelID;

//    private Host myself;
//    private int joinedInstance;
//    // Current members in the membership
//    private List<Host> membership;
//    // Set with timestamps - pair (SequenceNumb, process) - per keys
//    private HashSet<Pair<Integer, UUID>> tag;
//    private byte[] pending;
//    // Write Request Queue
//    private List<WriteRequest> writeRequestsQ;
//    // Read Request Queue
//    private List<ReadRequest> readRequestsQ;
//    // Local copy of the KV map
//    private HashMap<String, byte[]> state;
//    private HashSet<Pair<Integer, UUID>>


    // Myself
    private Host myself;
    // Set with all replicas
    private Set<Host> membership;
    // Hashmap of with timestamps (pair(sequenceNumber, Process) per key
    private Map<char[], Pair<Integer, Host>> tag;
    // Set with values of every object identified by key k
    private Map<char[], byte[]> val;
    // Current sequence number of operation of this process (opSeq)
    private int joinedInstance;
    // The current value we are trying to write
    private Pair<UUID, byte[]> pending;
    // Set containing the reply messages
    private HashSet<Pair<Integer, Host>> answersReadTag;
    private HashSet<ReadReply> answersReadReply;
    // Set containing the ack messages
    private HashSet<Host> answersAck;

    /**
     * Processes need to wait for at least 3 nodes to join the system to effectively start to quorum
     * Let's put membership management on hold.
     * Assume that once init is called they all join the system and are ready to quorum
     * Membership doesn't contain myself.
     *
     * @param props properties of the current replica
     * @throws IOException
     * @throws HandlerRegistrationException
     */

    public ABD(Properties props) throws IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = new HashSet<>();
        tag = new HashMap<>();
        val = new HashMap<>();
        pending = null;
        myself = new Host(
                InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("p2p_port"))
        );
    }

    private void registerHandlers() throws HandlerRegistrationException {
        /*------------------------------ Register Timer Handlers -------------------------------------------- */

        /*------------------------------ Register Request Handlers ------------------------------------------ */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);
        registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);

        /*------------------------------ Register Notification Handlers -------------------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);

        /*------------------------------ Register Message Message Serializers -------------------------------- */
        registerMessageSerializer(channelID, AddReplicaMessage.MSG_ID, AddReplicaMessage.serializer);
        registerMessageSerializer(channelID, ReadTagReply.MSG_ID, ReadTagReply.serializer);
        registerMessageSerializer(channelID, ReadTag.MSG_ID, ReadTag.serializer);
        registerMessageSerializer(channelID, WriteMessage.MSG_ID, WriteMessage.serializer);
        registerMessageSerializer(channelID, Ack.MSG_ID, Ack.serializer);
        registerMessageSerializer(channelID, ReadMessage.MSG_ID, ReadMessage.serializer);
        registerMessageSerializer(channelID, ReadReply.MSG_ID, ReadReply.serializer);

        /*------------------------------ Register Message Message Handlers ----------------------------------- */
        try {
            registerMessageHandler(channelID, AddReplicaMessage.MSG_ID, this::uponAddReplicaMessage, this::uponMsgFail);
            registerMessageHandler(channelID, ReadTagReply.MSG_ID, this::uponReadTagReply, this::uponMsgFail);
            registerMessageHandler(channelID, ReadTag.MSG_ID, this::uponReadTag, this::uponMsgFail);
            registerMessageHandler(channelID, WriteMessage.MSG_ID, this::uponWriteMessage, this::uponMsgFail);
            registerMessageHandler(channelID, Ack.MSG_ID, this::uponAck, this::uponMsgFail);
            registerMessageHandler(channelID, ReadMessage.MSG_ID, this::uponReadMessage, this::uponMsgFail);
            registerMessageHandler(channelID, ReadReply.MSG_ID, this::uponReadReply, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }
    }

    // Ignoring membership part for now
    @Override
    public void init(Properties props) throws IOException, HandlerRegistrationException {
        logger.info("Initializing ABD");
        Properties channelProps = new Properties();

        int port = Integer.parseInt(props.getProperty("p2p_port"));

        // Creating TCP channel
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address"));
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("p2p_port"));
        this.channelID = createChannel(TCPChannel.NAME, channelProps);

        // Register Handlers
        registerHandlers();

        String[] membershipStr = props.getProperty("initial_membership").split(",");

        for (String s : membershipStr) {
            String ipAdr = s.split(":")[0];
            int p = Integer.parseInt(s.split(":")[1]);
            if (p != port) {
                Host h = new Host(InetAddress.getByName(ipAdr), p);
                // Request to add myself
                AddReplicaMessage addReplicaMessage = new AddReplicaMessage(this.joinedInstance, myself);
                logger.info("Sent {}", addReplicaMessage);
                openConnection(h);
                sendMessage(addReplicaMessage, h);
            }
        }
        // Should only be changed after joining the system
        this.joinedInstance = 0;
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

    /**
     * When receiving a write request from the application.
     *
     * @param request from the application
     * @param sourceProto id of the requester protocol
     */
    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        logger.info("Received {} from application", request);

        // Using this as opSeq - currently considering only one write at a time.
        if(pending != null) {
            logger.info("I'm going to ignore this request for now... sry");
            return;
        }
        // TODO - Probably should be initialized in the constructor
        answersReadTag = new HashSet<>();
        answersAck = new HashSet<>();

        // Also stores the operation UUID for returning it later
        pending = Pair.of(request.getOpId(), request.getData());
        joinedInstance++;

        // Create tag to broadcast
        Pair<Integer, Host> tag = Pair.of(joinedInstance, myself);
        answersReadTag.add(tag);
        ReadTag readTag = new ReadTag(joinedInstance, request.getKey());

        // TODO - Replace by best effort broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(readTag, peer);
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
        if (joinedInstance == msg.getPeerOpID()) {
            answersReadTag.add(msg.getTag());
            // We have enough answers for a quorum
            if (answersReadTag.size() >= (membership.size()+1)/2 + 1) {
                int new_tag = getMaxSQTag(answersReadTag);
                joinedInstance++;
                answersReadTag.clear();
                WriteMessage wm = new WriteMessage(
                        joinedInstance,
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
        if (msg.getOpSeq() == joinedInstance) {
            answersAck.add(host);
            if (answersAck.size() >= (membership.size()+1)/2 + 1) {
                answersAck.clear();
                // Check weather if the current operation is a Write or a read
                if (pending.getRight() == null) {
                    logger.info("Triggered Write Complete notification");
                    triggerNotification(new WriteCompleteNotification(
                            pending.getLeft(),
                            msg.getKey(),
                            val.get(msg.getKey()))
                    );
                } else {
                    logger.info("Triggered Read Complete notification");
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
            }
        }
    }

    /**
     * Received a ReadRequest from the application
     *
     * @param request sent by the application
     * @param sourceProto id of the sending protocol
     */
    public void uponReadRequest(ReadRequest request, short sourceProto) {
        logger.info("Received {} from application", request);
        joinedInstance++;
        // TODO - Probably should be initialized in the constructor
        answersReadReply = new HashSet<>();
        ReadMessage rm = new ReadMessage(joinedInstance, request.getKey());

        pending = Pair.of(request.getOpId(), null);

        // TODO - Replace by best effort broadcast
        for (Host peer : membership) {
            openConnection(peer);
            sendMessage(rm, peer);
        }


        // Add my read reply to the answers set -- won't send to myself
        ReadReply rp = new ReadReply(
                joinedInstance,
                request.getKey(),
                tag.get(request.getKey()),
                val.get(request.getKey())
        );
        answersReadReply.add(rp);
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
                .max(Comparator.comparing(reply -> reply.getTag().getLeft()))
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
        if(msg.getPeerOpID() == joinedInstance) {
            answersReadReply.add(msg);
            if(answersReadTag.size() >= (membership.size()+1)/2 + 1) {
                ReadReply msgMax = getMaxReply(answersReadReply);
                Pair<Integer, Host> newTag = msgMax.getTag();
                // Update the current pending value being read
                UUID cur = pending.getLeft();
                pending = Pair.of(cur, msgMax.getValue());
                joinedInstance++;
                answersReadTag.clear();
                WriteMessage wm = new WriteMessage(joinedInstance, msgMax.getKey(), newTag, pending.getRight());
                // TODO - replace by reliable broadcast
                for(Host peer : membership) {
                    openConnection(peer);
                    sendMessage(wm, peer);
                }

            }
        }
    }

    /**
     * When receiving a AddReplicaMessage, a new replica is trying to join the system.
     *
     * @param msg - sent by the replica joining
     * @param host - original sender of the messages
     * @param sourceProto - of the protocol from which the message was sent
     * @param channelId - used to send the message
     */
    private void uponAddReplicaMessage(AddReplicaMessage msg, Host host, short sourceProto, int channelId) {
        logger.info("Received request to add {}", msg.getReplica());
        Host peer = msg.getReplica();
        membership.add(peer);
        logger.info("Added peer {}", peer);
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
//        //We joined the system and can now start doing things
//        joinedInstance = notification.getJoinInstance();
//        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
//        logger.debug("Received " + request);
//        BroadcastMessage msg = new BroadcastMessage(request.getInstance(), request.getOpId(), request.getOperation());
//        logger.debug("Sending to: " + membership);
//        membership.forEach(h -> sendMessage(msg, h));
    }

    /**
     * How should we use this ?????
     *
     * @param request
     * @param sourceProto
     */
    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
//        logger.debug("Received " + request);
//        request.getInstance();
//        membership.add(request.getReplica());
    }

    /**
     * How should we use this ?????
     *
     * @param request
     * @param sourceProto
     */
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
