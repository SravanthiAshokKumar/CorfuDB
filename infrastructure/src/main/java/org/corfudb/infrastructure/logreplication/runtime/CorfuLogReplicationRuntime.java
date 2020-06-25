package org.corfudb.infrastructure.logreplication.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.replication.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.StoppingState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.UnrecoverableState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.WaitingForConnectionsState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.IllegalTransitionException;
import org.corfudb.infrastructure.logreplication.runtime.fsm.NegotiatingState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.ReplicatingState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.VerifyingRemoteLeaderState;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Runtime to connect to a remote Corfu Log Replication Cluster.
 *
 * This class represents the Log Replication Runtime Finite State Machine, which defines
 * all states in which the leader node on the active cluster can be.
 *
 *
 *                                                      R-LEADER_LOSS
 *                                             +-------------------------------+
 *                              ON_CONNECTION  |                               |    ON_CONNECTION_DOWN
 *                                    UP       |       ON_CONNECTION_DOWN      |       (NON_LEADER)
 *                                    +----+   |          (R-LEADER)           |
 *                                    |    |   |   +-----------------------+   |        +-----+
 *                                    |    |   |   |                       |   |        |     |
 * +---------------+  ON_CONNECTION  ++----v---v---v--+                  +-+---+--------+-+   |
 * |               |       UP        |                |  R-REMOTE_LEADER_FOUND  |                <---+
 * |    WAITING    +---------------->+    VERIFYING   +------------------>                +---+
 * |      FOR      |                 |     REMOTE     |                  |   NEGOTIATING  |   | NEGOTIATION_FAILED
 * |  CONNECTIONS  +<----------------+     LEADER     |                  |                <---+
 * |               |  ON_CONNECTION  |                +<-----------+     |                +----+
 * +---------------+      DOWN       +-^----+---^----++            |     +-------+-----^--+    |
 *                       (ALL)         |    |   |    |             |             |     |       |
 *                                     |    |   |    |        R-LEADER_LOSS      |     +-------+
 *                                     +----+   +----+             |             |  ON_CONNECTION_UP
 *                              ON_CONNECTION     R-LEADER_NOT     |             |    (NON-LEADER)
 *                                  DOWN              FOUND        |             |
 *                                (NOT ALL)                        |     NEGOTIATION_COMPLETE
 *                                                                 |             |
 *                                                           ON_CONNECTION       |   ON_CONNECTION_UP
 *                                                               DOWN            |     (NON-LEADER)
 *                                                             (R-LEADER)        |      +-----+
 *                                                                 |             |      |     |
 *                                                                 |     +-------v------+-+   |
 *            +---------------+      ALL STATES                    +-----+                <---+
 *            |               |                                          |                |
 *            |   STOPPING    <---- L-LEADER_LOSS                        |  REPLICATING   |
 *            |               |                                          |                |
 *            |               |                                          |                +----+
 *            +---------------+                                          +--------------^-+    |
 *                                                                                      |      |
 *                                                                                      +------+
 *            +---------------+     ALL STATES
 *            |               |                                                     ON_CONNECTION_DOWN
 *            | UNRECOVERABLE <---- ON_ERROR                                           (NON-LEADER)
 *            |    STATE      |
 *            |               |
 *            +---------------+
 *
 *
 *
 *
 * States:
 * ------
 *
 * - WAITING_FOR_CONNECTIVITY    :: initial state, waiting for any connection to remote cluster to be established.
 * - VERIFYING_REMOTE_LEADER     :: verifying the leader endpoint on remote cluster (querying all connected nodes)
 * - NEGOTIATING                 :: negotiating against the leader endpoint
 * - REPLICATING                 :: replicating data to remote cluster through the leader endpoint
 * - STOPPING                    :: stop state machine, no error, just lost leadership so replication stops from this node
 * - UNRECOVERABLE_STATE         :: error state, unrecoverable error reported by replication, transport or cluster manager, despite
 *                                  being the leader node.
 *
 *
 * Events / Transitions:
 * --------------------
 *
 * - ON_CONNECTION_UP           :: connection to a remote endpoint comes UP
 * - ON_CONNECTION_DOWN         :: connection to a remote endpoint comes DOWN
 * - REMOTE_LEADER_NOT_FOUND,   :: remote leader not found
 * - REMOTE_LEADER_FOUND,       :: remote leader found
 * - REMOTE_LEADER_LOSS,        :: remote Leader Lost (remote node reports it is no longer the leader)
 * - LOCAL_LEADER_LOSS          :: local node looses leadership
 * - NEGOTIATION_COMPLETE,      :: negotiation succeeded and completed
 * - NEGOTIATION_FAILED,        :: negotiation failed
 * - STOPPING                   :: stop log replication server (fatal state)
 *
 * @author amartinezman
 *
 *
 */
@Slf4j
public class CorfuLogReplicationRuntime {

    public static final int DEFAULT_TIMEOUT = 5000;

    /**
     * Current state of the FSM.
     */
    private volatile LogReplicationRuntimeState state;

    /**
     * Map of all Log Replication Communication FSM States (reuse single instance for each state)
     */
    @Getter
    private Map<LogReplicationRuntimeStateType, LogReplicationRuntimeState> states = new HashMap<>();

    /**
     * Executor service for FSM state tasks
     */
    private ExecutorService communicationFSMWorkers;

    /**
     * Executor service for FSM event queue consume
     */
    private ExecutorService communicationFSMConsumer;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationRuntimeEvent> eventQueue = new LinkedBlockingQueue<>();

    private final LogReplicationClientRouter router;
    private final LogReplicationMetadataManager metadataManager;
    private final LogReplicationSourceManager sourceManager;
    private volatile Set<String> connectedEndpoints = ConcurrentHashMap.newKeySet();
    private volatile Optional<String> leaderEndpoint = Optional.empty();
    public final String remoteClusterId;

    /**
     * Default Constructor
     */
    public CorfuLogReplicationRuntime(LogReplicationRuntimeParameters parameters, LogReplicationMetadataManager metadataManager) {
        this.remoteClusterId = parameters.getRemoteClusterDescriptor().getClusterId();
        this.metadataManager = metadataManager;
        this.router = new LogReplicationClientRouter(parameters, this);
        this.router.addClient(new LogReplicationHandler());
        this.sourceManager = new LogReplicationSourceManager(parameters.getLocalCorfuEndpoint(),
                new LogReplicationClient(router, remoteClusterId), parameters.getReplicationConfig());
        this.communicationFSMWorkers = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("runtime-fsm-worker").build());
        this.communicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("runtime-fsm-consumer").build());

        initializeStates();
        this.state = states.get(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY);

        log.info("Log Replication Runtime State Machine initialized");
    }

    /**
     * Start Log Replication Communication FSM
     */
    public void start() {
        // Start Consumer Thread for this state machine (dedicated thread for event consumption)
        communicationFSMConsumer.submit(this::consume);
        router.connect();
    }

    /**
     * Initialize all states for the Log Replication Runtime FSM.
     */
    private void initializeStates() {
        /*
         * Log Replication Runtime State instances are kept in a map to be reused in transitions, avoid creating one
         * per every transition (reduce GC cycles).
         */
        states.put(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY, new WaitingForConnectionsState(this));
        states.put(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER, new VerifyingRemoteLeaderState(this, communicationFSMWorkers, router));
        states.put(LogReplicationRuntimeStateType.NEGOTIATING, new NegotiatingState(this, communicationFSMWorkers, router, metadataManager));
        states.put(LogReplicationRuntimeStateType.REPLICATING, new ReplicatingState(this, sourceManager));
        states.put(LogReplicationRuntimeStateType.STOPPING, new StoppingState(sourceManager));
        states.put(LogReplicationRuntimeStateType.UNRECOVERABLE, new UnrecoverableState());
    }

    /**
     * Input function of the FSM.
     *
     * This method enqueues runtime events for further processing.
     *
     * @param event LogReplicationRuntimeEvent to process.
     */
    public synchronized void input(LogReplicationRuntimeEvent event) {
        try {
            if (state.getType().equals(LogReplicationRuntimeStateType.STOPPING)) {
                // Not accepting events, in stopped state
                return;
            }
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     * <p>
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            if (state.getType() == LogReplicationRuntimeStateType.STOPPING) {
                log.info("Log Replication Communication State Machine has been stopped. No more events will be processed.");
                return;
            }

            //  Block until an event shows up in the queue.
            LogReplicationRuntimeEvent event = eventQueue.take();

            try {
                LogReplicationRuntimeState newState = state.processEvent(event);
                transition(state, newState);
                state = newState;
            } catch (IllegalTransitionException illegalState) {
                log.error("Illegal log replication event {} when in state {}", event.getType(), state.getType());
            }

            communicationFSMConsumer.submit(this::consume);

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to   final state
     */
    private void transition(LogReplicationRuntimeState from, LogReplicationRuntimeState to) {
        log.trace("Transition from {} to {}", from, to);
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }

    public synchronized void updateConnectedEndpoints(String endpoint) {
        connectedEndpoints.add(endpoint);
    }

    public synchronized void updateDisconnectedEndpoints(String endpoint) {
        connectedEndpoints.remove(endpoint);
    }

    public synchronized void setLeaderEndpoint(String leader) {
        leaderEndpoint = Optional.ofNullable(leader);
    }

    public synchronized Optional<String> getLeader() {
        return leaderEndpoint;
    }

    public synchronized Set<String> getConnectedEndpoints() {
        return connectedEndpoints;
    }

    /**
     * Retrieve total number of entries to be sent based on a given timestamp.
     *
     * This is required for progress status reporting.
     *
     * @param ts base (reference) timestamp
     *
     * @return pending number of entries to send
     */
    public long getNumEntriesToSend(long ts) {
        long ackTS = sourceManager.getLogReplicationFSM().getAckedTimestamp();
        return ts - ackTS;
    }

    /**
     * Stop Log Replication, regardless of current state.
     */
    public void stop() {
        log.info("Local leadership lost. Log Replication will immediately stop.");
        input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.LOCAL_LEADER_LOSS));
    }
}