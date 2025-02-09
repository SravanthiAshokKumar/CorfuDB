package org.corfudb.infrastructure.logreplication.replication.fsm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.ReplicationModel;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.send.LogEntrySender;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogicalGroupLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogicalGroupSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.RoutingQueuesLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.RoutingQueuesSnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * This class implements the Log Replication Finite State Machine.
 *
 * CorfuDB provides a Log Replication functionality, which allows logs to be automatically replicated from a primary
 * to a remote cluster. This feature is particularly useful in the event of failure or data corruption, so the system
 * can failover to the sink/secondary data-store.
 *
 * This functionality is initiated by the application through the LogReplicationSourceManager on the primary cluster and handled
 * through the LogReplicationSinkManager on the destination cluster. This implementation assumes that the application provides its own
 * communication channels.
 *
 * Log Replication on the source cluster is defined by an event-driven finite state machine, with 5 states
 * and 10 events/messages---which can trigger the transition between states.
 *
 * States:
 * ------
 *  - Initialized (initial state)
 *  - In_Log_Entry_Sync
 *  - In_Snapshot_Sync
 *  - Wait_Snapshot_Apply
 *  - Error
 *
 * Events:
 * ------
 *  - snapshot_sync_request
 *  - snapshot_sync_continue
 *  - snapshot_transfer_complete
 *  - snapshot_apply_in_progress
 *  - snapshot_apply_complete
 *  - log_entry_sync_request
 *  - log_entry_sync_continue
 *  - sync_cancel
 *  - replication_stop
 *  - replication_shutdown
 *
 *
 * The following diagram illustrates the Log Replication FSM state transition:
 *
 *
 *
 *                                          REPLICATION_STOP
 *                                              +------+
 *                                              |      |
 *                                              |      |
 *                LOG_ENTRY_SYNC_REQUEST   +----+------v-----+    SNAPSHOT_SYNC_REQUEST
 *             +---------------------------+   INITIALIZED   +-----------------------------+
 *             |                           +-^---^--+------^-+                             |
 *             |                             |   |  |      |                               |
 *             |                             |   |  |      |                               |
 *             |                             |   |  |      |                               |
 *             |                             |   |  |      |                               |
 *             |                             |   |  |      |                               |
 *             |                             |   |  |      |                               |     SYNC
 *             |                             |   |  |      |                               |    CANCEL
 *             |                             |   |  |      |                               |   +------+
 *             |                             |   |  |      |                               |   |      |
 *             |                             |   |  |      |                               |   |      |
 * +-----------v-----------+   REPLICATION   |   |  |      |   REPLICATION     +-----------v---+------v--+
 * |   IN_LOG_ENTRY_SYNC   +------STOP-------+   |  |      +------STOP---------+    IN_SNAPSHOT_SYNC     |
 * +----+-----^---+------^-+                     |  |                          +-^-----+---^-----+-----^-+
 *      |     |   |      |                       |  |                            |     |   |     |     |
 *      |     |   |      |                       |  |                            |     |   |     |     |
 *      |     |   +------+           REPLICATION |  | SNAPSHOT_TRANSFER          +-----+   |     |     |
 *      |     |   LOG_ENTRY              STOP    |  |      COMPLETE              SNAPSHOT  |     |     |
 *      |     | SYNC_CONTINUE                    |  |                              SYNC    |     |     |
 *      |     |                                  |  |                            CONTINUE  |     |     |
 *      |     |                                  |  |                                      |     |     |
 *      |     |                                  |  |                                      |     |     |
 *      |     |                                  |  |                                      |     |     |
 *      |     |                         +--------+--v-------------+  SNAPSHOT_SYNC_REQUEST |     |     |
 *      |     +-----SNAPSHOT_APPLY------+   WAIT_SNAPSHOT_APPLY   +------------------------+     |     |
 *      |              COMPLETE         +-^----+---^--------------+       SYNC_CANCEL            |     |
 *      |                                 |    |   |                                             |     |
 *      |                                 |    |   +-----------SNAPSHOT_TRANSFER_COMPLETE--------+     |
 *      |                                 +----+                                                       |
 *      |                             SNAPSHOT_APPLY                                                   |
 *      |                               IN_PROGRESS                                                    |
 *      |                                                                                              |
 *      +----------------------------------------SYNC_CANCEL-------------------------------------------+
 *                                          SNAPSHOT_SYNC_REQUEST
 *
 *
 *
 *
 *     +-------------+
 *     |   ERROR      <-----REPLICATION-----  ALL_STATES
 *     +-------------+       SHUTDOWN
 *
 *
 */
@Slf4j
public class LogReplicationFSM {

    @Getter
    private long topologyConfigId;

    /**
     * Current state of the FSM.
     */
    @Getter
    private volatile LogReplicationState state;

    /**
     * Map of all Log Replication FSM States (reuse single instance for each state)
     */
    @Getter
    private final Map<LogReplicationStateType, LogReplicationState> states = new HashMap<>();

    /**
     * Executor service for FSM state tasks (it can be shared across several LogReplicationFSMs)
     */
    @Getter
    private final ExecutorService logReplicationFSMWorkers;

    /**
     * Executor service for FSM event queue consume
     */
    private final ExecutorService logReplicationFSMConsumer;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * An observable object on the number of transitions of this state machine (for testing & visibility)
     */
    @VisibleForTesting
    @Getter
    private final ObservableValue<Integer> numTransitions = new ObservableValue(0);

    /**
     * Log Entry Reader (read incremental updated from Corfu Datatore)
     */
    @Getter
    private final LogEntryReader logEntryReader;

    /**
     * Snapshot Reader (read data from Corfu Datastore)
     */
    private final SnapshotReader snapshotReader;

    /**
     * Version on which snapshot sync is based on.
     */
    @Getter
    @Setter
    private long baseSnapshot = Address.NON_ADDRESS;

    /**
     * Acknowledged timestamp
     */
    @Getter
    @Setter
    private long ackedTimestamp = Address.NON_ADDRESS;

    /**
     * Log Entry Sender (send incremental updates to remote cluster)
     */
    @Getter
    private final LogEntrySender logEntrySender;

    /**
     * Snapshot Sender (send snapshot cut to remote cluster)
     */
    private final SnapshotSender snapshotSender;

    /**
     * Ack Reader for Snapshot and Log Entry Syncs
     */
    @Getter
    private final LogReplicationAckReader ackReader;

    @Getter
    private final LogReplicationSession session;

    /**
     * Constructor for LogReplicationFSM, custom read processor for data transformation.
     *
     * @param runtime           Corfu Runtime
     * @param dataSender        implementation of a data sender, both snapshot and log entry, this represents
     *                          the application callback for data transmission
     * @param readProcessor     read processor for data transformation
     * @param workers           FSM executor service for state tasks
     * @param ackReader         AckReader which listens to acks from the Sink and updates the replication status accordingly
     * @param session           Replication Session to the remote(Sink) cluster
     * @param replicationContext Replication context
     */
    public LogReplicationFSM(CorfuRuntime runtime, DataSender dataSender,
                             ReadProcessor readProcessor, ExecutorService workers, LogReplicationAckReader ackReader,
                             LogReplicationSession session, LogReplicationContext replicationContext) {
        this.snapshotReader = createSnapshotReader(runtime, session, replicationContext);
        this.logEntryReader = createLogEntryReader(runtime, session, replicationContext);

        this.ackReader = ackReader;
        this.snapshotSender = new SnapshotSender(runtime, snapshotReader, dataSender, readProcessor,
                replicationContext.getConfig(session).getMaxNumMsgPerBatch(), this);
        this.logEntrySender = new LogEntrySender(logEntryReader, dataSender, this);
        this.logReplicationFSMWorkers = workers;
        this.logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
            ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer-" + session.hashCode())
            .build());
        this.session = session;

        init(dataSender, session);
    }

    /**
     * Constructor for LogReplicationFSM, custom readers (as it is not expected to have custom
     * readers, this is used for FSM testing purposes only).
     *
     * @param runtime Corfu Runtime
     * @param snapshotReader snapshot logreader implementation
     * @param dataSender application callback for snapshot and log entry sync messages
     * @param logEntryReader log entry logreader implementation
     * @param readProcessor read processor (for data transformation)
     * @param workers FSM executor service for state tasks
     * @param ackReader AckReader which listens to acks from the Sink and updates the replication status accordingly
     * @param session   Replication Session to the remote(Sink) cluster
     * @param replicationContext Replication context
     */
    @VisibleForTesting
    public LogReplicationFSM(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                             LogEntryReader logEntryReader, ReadProcessor readProcessor,
                             ExecutorService workers, LogReplicationAckReader ackReader,
                             LogReplicationSession session, LogReplicationContext replicationContext) {

        this.snapshotReader = snapshotReader;
        this.logEntryReader = logEntryReader;
        this.ackReader = ackReader;
        this.snapshotSender = new SnapshotSender(runtime, snapshotReader, dataSender, readProcessor,
                replicationContext.getConfig(session).getMaxNumMsgPerBatch(), this);
        this.logEntrySender = new LogEntrySender(logEntryReader, dataSender, this);
        this.logReplicationFSMWorkers = workers;
        this.logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer-" + session.hashCode())
                .build());
        this.session = session;

        init(dataSender, session);
    }

    private SnapshotReader createSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                                LogReplicationContext replicationContext) {
        SnapshotReader snapshotReader;
        ReplicationModel model = session.getSubscriber().getModel();
        switch (model) {
            case FULL_TABLE:
                snapshotReader = new StreamsSnapshotReader(runtime, session, replicationContext);
                break;

            case LOGICAL_GROUPS:
                snapshotReader = new LogicalGroupSnapshotReader(runtime, session, replicationContext);
                break;

            case ROUTING_QUEUES:
                snapshotReader = new RoutingQueuesSnapshotReader(runtime, session, replicationContext);
                break;

            default:
                log.error("Unsupported replication model found: {}", model);
                throw new IllegalArgumentException("Unsupported replication model found: " + model);

        }
        return snapshotReader;
    }

    private LogEntryReader createLogEntryReader(CorfuRuntime runtime, LogReplicationSession session,
                                                LogReplicationContext replicationContext) {
        LogEntryReader logEntryReader;
        ReplicationModel model = session.getSubscriber().getModel();
        switch(model) {
            case FULL_TABLE:
                logEntryReader = new StreamsLogEntryReader(runtime, session, replicationContext);
                break;

            case LOGICAL_GROUPS:
                logEntryReader = new LogicalGroupLogEntryReader(runtime, session, replicationContext);
                break;

            case ROUTING_QUEUES:
                logEntryReader = new RoutingQueuesLogEntryReader(runtime, session, replicationContext);
                break;

            default:
                log.error("Unsupported Replication Model Found: {}", model);
                throw new IllegalArgumentException("Unsupported Replication Model Found: " +
                        session.getSubscriber().getModel());
        }
        return logEntryReader;
    }

    private void init(DataSender dataSender, LogReplicationSession session) {
        // Initialize Log Replication 5 FSM states - single instance per state
        initializeStates(snapshotSender, logEntrySender, dataSender);
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        logReplicationFSMConsumer.submit(this::consume);
        log.info("Log Replication FSM initialized for session={}", session);
    }

    /**
     * Initialize all states for the Log Replication FSM.
     *
     * @param snapshotSender reads and transmits snapshot syncs
     * @param logEntrySender reads and transmits log entry sync
     */
    private void initializeStates(SnapshotSender snapshotSender, LogEntrySender logEntrySender, DataSender dataSender) {
        /*
         * Log Replication State instances are kept in a map to be reused in transitions, avoid creating one
         * per every transition (reduce GC cycles).
         */
        states.put(LogReplicationStateType.INITIALIZED, new InitializedState(this));
        states.put(LogReplicationStateType.IN_SNAPSHOT_SYNC, new InSnapshotSyncState(this, snapshotSender));
        states.put(LogReplicationStateType.WAIT_SNAPSHOT_APPLY, new WaitSnapshotApplyState(this, dataSender));
        states.put(LogReplicationStateType.IN_LOG_ENTRY_SYNC, new InLogEntrySyncState(this, logEntrySender));
        states.put(LogReplicationStateType.ERROR, new ErrorState(this));
    }

    /**
     * Input function of the FSM.
     *
     * This method enqueues log replication events for further processing.
     *
     * @param event LogReplicationEvent to process.
     */
    public synchronized void input(LogReplicationEvent event) {
        try {
            if (state.getType().equals(LogReplicationStateType.ERROR)) {
                // Log: not accepting events, in stopped state
                return;
            }
            if (event.getType() != LogReplicationEventType.LOG_ENTRY_SYNC_CONTINUE) {
                log.trace("Enqueue event {} with ID {}", event.getType(), event.getEventId());
            }
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     *
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            if (state.getType() == LogReplicationStateType.ERROR) {
                log.info("Log Replication State Machine has been stopped. No more events will be processed.");
                return;
            }

            // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)
            // Block until an event shows up in the queue.
            LogReplicationEvent event = eventQueue.take();

            try {
                LogReplicationState newState = state.processEvent(event);
                log.trace("Transition from {} to {}", state, newState);
                transition(state, newState);
                state = newState;
                numTransitions.setValue(numTransitions.getValue() + 1);
            } catch (IllegalTransitionException illegalState) {
                // Ignore LOG_ENTRY_SYNC_REPLICATED events for logging purposes as they will likely come in frequently,
                // as it is used for update purposes but does not imply a transition.
                if (!event.getType().equals(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED)) {
                    log.error("Illegal log replication event {} when in state {}", event.getType(), state.getType());
                }
            }

            // For testing purpose to notify the event generator the stop of the event.
            if (event.getType() == LogReplicationEventType.REPLICATION_STOP) {
                synchronized (event) {
                    event.notifyAll();
                }
            }

            // Consume one event in the queue and re-submit, this is done so events are consumed in
            // a round-robin fashion for the case of multi-cluster replication.
            logReplicationFSMConsumer.submit(this::consume);

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to final state
     */
    void transition(LogReplicationState from, LogReplicationState to) {
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }

    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
        snapshotReader.setTopologyConfigId(topologyConfigId);
        logEntryReader.setTopologyConfigId(topologyConfigId);
        snapshotSender.updateTopologyConfigId(topologyConfigId);
        logEntrySender.updateTopologyConfigId(topologyConfigId);
    }

    /**
     * Shutdown Log Replication FSM
     */
    public void shutdown() {
        this.ackReader.shutdown();
        this.logReplicationFSMConsumer.shutdown();
        this.logReplicationFSMWorkers.shutdown();
    }
}
