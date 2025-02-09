package org.corfudb.runtime.collections.streaming;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class represents a Streaming task that is managed by {@link StreamPollingScheduler}. It binds a stream listener
 * {@link StreamListener} to a {@link DeltaStream}, every time it's scheduled to sync, it will read data for a specific
 * stream tag, transform it and propagate it to the listener.
 *
 * @param <K> - type of the protobuf KeySchema defined while the table was created.
 * @param <V> - type of the protobuf PayloadSchema defined by the table creator.
 * @param <M> - type of the protobuf metadata schema defined by table creation.
 *
 */

@Slf4j
public class StreamingTask<K extends Message, V extends Message, M extends Message> implements Runnable {

    // Pre-registered client call back.
    @Getter
    private final StreamListener listener;

    // The table id to schema map of the interested tables.
    protected final Map<UUID, TableSchema<K, V, M>> tableSchemas = new HashMap<>();

    @Getter
    private final String listenerId;

    private final CorfuRuntime runtime;

    private final ExecutorService workerPool;

    protected DeltaStream stream;

    protected final AtomicReference<StreamStatus> status = new AtomicReference<>();

    private volatile Throwable error;

    protected StreamingTask(CorfuRuntime runtime, ExecutorService workerPool, StreamListener listener,
                            String listenerId) {
        this.runtime = runtime;
        this.workerPool = workerPool;
        this.listener = listener;
        this.listenerId = listenerId;
    }

    public StreamingTask(CorfuRuntime runtime, ExecutorService workerPool, String namespace, String streamTag,
                         StreamListener listener,
                         List<String> tablesOfInterest,
                         long address,
                         int bufferSize) {
        this(runtime, workerPool, listener, String.format("listener_%s_%s_%s", listener, namespace, streamTag));
        TableRegistry registry = runtime.getTableRegistry();
        UUID streamId;
        // Federated tables do not have a stream tag, only an option isFederated set to true. Internally,
        // their stream tag is constructed when the table is opened. This stream tag is constructed differently than
        // other tables. So for subscribers on this tag, do not construct the streamId using the generic algorithm.
        if (streamTag.equals(ObjectsView.getLogReplicatorStreamId().toString())) {
            streamId = ObjectsView.getLogReplicatorStreamId();
        } else {
            streamId = TableRegistry.getStreamIdForStreamTag(namespace, streamTag);
        }
        this.stream = new DeltaStream(runtime.getAddressSpaceView(), streamId, address, bufferSize);

        for (String tableOfInterest : tablesOfInterest) {
            UUID tableId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(namespace, tableOfInterest));

            // Subscription on special tables(ProtobufDescriptor and Registry tables) is not possible
            // with the regular workflow as they are not opened using CorfuStore.openTable().
            // However, these tables do have the stream tag for Log Replication and it is useful to
            // subscribe to them for testing purposes. The below is special handling to allow for such a
            // subscription.
            if (streamId.equals(ObjectsView.getLogReplicatorStreamId()) && namespace.equals(
                TableRegistry.CORFU_SYSTEM_NAMESPACE) &&
                tableOfInterest.equals(TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
                tableSchemas.put(tableId, new TableSchema(tableOfInterest, ProtobufFileName.class, ProtobufFileDescriptor.class,
                        TableMetadata.class));
            } else if (streamId.equals(ObjectsView.getLogReplicatorStreamId()) && namespace.equals(
                TableRegistry.CORFU_SYSTEM_NAMESPACE) &&
                tableOfInterest.equals(TableRegistry.REGISTRY_TABLE_NAME)) {
                tableSchemas.put(tableId, new TableSchema(tableOfInterest, TableName.class, TableDescriptors.class,
                        TableMetadata.class));
            } else {
                // The table should be opened with full schema before subscription.
                Table<K, V, M> t = registry.getTable(namespace, tableOfInterest);
                if (!t.getStreamTags().contains(streamId)) {
                        throw new IllegalArgumentException(String.format("Interested table: %s does not " +
                            "have specified stream tag: %s", t.getFullyQualifiedTableName(), streamTag));
                }
                tableSchemas.put(tableId, new TableSchema<>(tableOfInterest, t.getKeyClass(), t.getValueClass(),
                        t.getMetadataClass()));
            }
        }
        status.set(StreamStatus.RUNNABLE);
    }

    public StreamStatus getStatus() {
        return status.get();
    }

    public void move(StreamStatus from, StreamStatus to) {
        Preconditions.checkState(status.compareAndSet(from, to),
                "move: failed to change %s to %s", from, to);
    }

    private Optional<CorfuStreamEntries> transform(ILogData logData) {
        Objects.requireNonNull(logData);
        // Avoid LogData de-compression if it does not contain any table of interest.
        if (logData.isHole() || Sets.intersection(logData.getStreams(), tableSchemas.keySet()).isEmpty()) {
            return Optional.empty();
        }

        long epoch = logData.getEpoch();
        MultiObjectSMREntry smrEntries = (MultiObjectSMREntry) logData.getPayload(runtime);

        Map<UUID, TableSchema<K, V, M>> filteredSchemas = logData.getStreams()
                .stream()
                .filter(tableSchemas::containsKey)
                .collect(Collectors.toMap(Function.identity(), tableSchemas::get));

        Map<TableSchema, List<CorfuStreamEntry>> streamEntries = new HashMap<>();

        filteredSchemas.forEach((streamId, schema) -> {
            // Only deserialize the interested streams to reduce overheads.
            List<CorfuStreamEntry> entryList = smrEntries.getSMRUpdates(streamId)
                    .stream()
                    .map(CorfuStreamEntry::fromSMREntry)
                    .collect(Collectors.toList());

            // Deduplicate entries per stream Id, ordering within a transaction is not guaranteed
            Map<Message, CorfuStreamEntry> observedKeys = new HashMap<>();
            entryList.forEach(entry -> observedKeys.put(entry.getKey(), entry));

            if (!entryList.isEmpty()) {
                streamEntries.put(schema, new ArrayList<>(observedKeys.values()));
            }
        });

        // This transaction data does not contain any table of interest, don't add to buffer.
        if (streamEntries.isEmpty()) {
            return Optional.empty();
        }

        CorfuStoreMetadata.Timestamp timestamp = CorfuStoreMetadata.Timestamp.newBuilder()
                .setSequence(logData.getGlobalAddress())
                .setEpoch(epoch)
                .build();

        return Optional.of(new CorfuStreamEntries(streamEntries, timestamp));
    }

    public DeltaStream getStream() {
        return this.stream;
    }

    private void produce() {
        Preconditions.checkState(status.get() == StreamStatus.SYNCING);
        Preconditions.checkState(stream.hasNext());
        ILogData logData = stream.next();
        Optional<CorfuStreamEntries> streamEntries = transform(logData);
        log.debug("producing {}@{} {} on {}", logData.getEpoch(), logData.getGlobalAddress(), logData.getType(), listenerId);

        streamEntries.ifPresent(e -> MicroMeterUtils.time(() -> listener.onNextEntry(e),
                "stream.notify.duration",
                "listener",
                listenerId));

        // Re-schedule, give other streams a chance to produce
        if (stream.hasNext()) {
            // need to make this a safe runnable
            workerPool.execute(this);
            // We need to make sure that the task can only run on a single thread therefore we must return immediately
            return;
        }

        // No more items to produce
        move(StreamStatus.SYNCING, StreamStatus.RUNNABLE);
    }

    public void propagateError() {
        Objects.requireNonNull(error);
        if (error instanceof TrimmedException) {
            listener.onError(new StreamingException(error, StreamingException.ExceptionCause.TRIMMED_EXCEPTION));
        } else {
            listener.onError(error);
        }
    }

    @Override
    public void run() {
        try {
            produce();
        } catch (Throwable throwable) {
            setError(throwable);
            log.error("StreamingTask: encountered exception {} during client notification callback, " +
                    "listener: {} name {} id {}", throwable, listener, listenerId, stream.getStreamId());
        }
    }

    public void setError(Throwable throwable) {
        status.set(StreamStatus.ERROR);
        this.error = throwable;
    }
}
