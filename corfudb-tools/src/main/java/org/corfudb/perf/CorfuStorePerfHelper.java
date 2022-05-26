package org.corfudb.perf;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.*;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

@Slf4j
public class CorfuStorePerfHelper {
    private final CorfuRuntime runtime;
    private final String diskPath;
    private final String tableName;
    private final CorfuStore corfuStore;

    private Table<CorfuStoreMetadata.TableName, CorfuStoreMetadata.TableName, Message> exampleTable;

    private static final String NAMESPACE = "PERF_TESTING";

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     * @param diskPath path to temp disk directory for loading large tables
     *                 that won't fit into memory
     */
    public CorfuStorePerfHelper(CorfuRuntime runtime, String diskPath, String tableName) {
        this.runtime = runtime;
        this.diskPath = diskPath;
        this.tableName = tableName;
        corfuStore = new CorfuStore(this.runtime);
        openExampleTable();
    }

    public void openExampleTable() {
        try {
            if (this.diskPath != null) {
                this.exampleTable = this.corfuStore.openTable(NAMESPACE,
                        this.tableName,
                        CorfuStoreMetadata.TableName.class,
                        CorfuStoreMetadata.TableName.class,
                        null,
                        TableOptions.fromProtoSchema(CorfuStoreMetadata.TableName.class,
                                TableOptions.builder()
                                        .persistentDataPath(Paths.get(this.diskPath)).build())
                );
            } else {
                this.exampleTable = this.corfuStore.openTable(NAMESPACE,
                        this.tableName,
                        CorfuStoreMetadata.TableName.class,
                        CorfuStoreMetadata.TableName.class,
                        null,
                        TableOptions.fromProtoSchema(CorfuStoreMetadata.TableName.class)
                );
            }
        } catch (Exception e) {
            log.error("Exception while trying to open {}, ", this.tableName, e);
        }
    }

    public void writeData(int batchSize, int entrySize, long numEntries, long numUpdates, String tableKeyPrefix) {

        long totalStartTime = 0;
        long endTime = 0;
        for (int i = 0; i < numUpdates; i++) {
            long itemsRemaining = numEntries;
            long perUpdateStartTime = 0;
            while (itemsRemaining > 0) {
                long startTime = System.currentTimeMillis();
                if (perUpdateStartTime == 0) {
                    perUpdateStartTime = startTime;
                }
                if (totalStartTime == 0) {
                    totalStartTime = startTime;
                }
                try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                    for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                        byte[] array = new byte[entrySize];
                        new Random().nextBytes(array);
                        CorfuStoreMetadata.TableName dummyKey = CorfuStoreMetadata.TableName.newBuilder()
                                .setTableName(tableKeyPrefix + itemsRemaining).build();
                        CorfuStoreMetadata.TableName dummyVal = CorfuStoreMetadata.TableName.newBuilder()
                                .setTableName(new String(array)).build();
                        txn.putRecord(this.exampleTable, dummyKey, dummyVal, null);
                    }
                    txn.commit();
                    endTime = System.currentTimeMillis();
                    MicroMeterUtils.time(Duration.ofMillis(endTime - startTime), "perf.write.timer",
                            "batchSize", Integer.toString(batchSize));
                }
            }
            MicroMeterUtils.time(Duration.ofMillis(endTime - perUpdateStartTime), "perf.total_write.timer",
                    "numEntries", Long.toString(numEntries));
        }
        log.info("writeData: of table {} with {} entries each updated {} times took {}ms", this.tableName,
               numEntries, numUpdates, endTime - totalStartTime);
    }

    public Set<CorfuStoreMetadata.TableName> listKeys() {
        Set<CorfuStoreMetadata.TableName> allKeys;
        long startTime = System.currentTimeMillis();
        try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
            allKeys = txn.keySet(this.exampleTable);
            txn.commit();
        }
        long elapsedNs = System.currentTimeMillis() - startTime;
        log.info("listKeys: for {} keys took {} ms", allKeys.size(), elapsedNs);
        MicroMeterUtils.time(Duration.ofMillis(elapsedNs), "perf.read_keys_timer",
                "numKeys", Long.toString(allKeys.size()));
        return allKeys;
    }

    public void readData(int batchSize) {
        Set<CorfuStoreMetadata.TableName> allKeys = listKeys();
        Iterator<CorfuStoreMetadata.TableName> iterator = allKeys.iterator();
        long totalStartTime = 0;
        long endTime = 0;

        while (iterator.hasNext()) {
            long perBatch = batchSize;
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                long startTime = System.currentTimeMillis();
                if (totalStartTime == 0) {
                    totalStartTime = startTime;
                }
                while (iterator.hasNext()) {
                    txn.getRecord(this.exampleTable, iterator.next());
                    if (--perBatch == 0) {
                        break;
                    }
                }
                txn.commit();
                endTime = System.currentTimeMillis();
                log.info("readData: Read {} items so far", batchSize);
                MicroMeterUtils.time(Duration.ofMillis(endTime - startTime), "perf.read_timer",
                            "batchSize", Long.toString(batchSize));
            }
        }

        long totalElapsedNs = endTime - totalStartTime;
        log.info("readData of table {} with {} items took {}ms", this.tableName, allKeys.size(),
                totalElapsedNs);
        MicroMeterUtils.time(Duration.ofMillis(totalElapsedNs), "perf.total_read_timer",
                "numEntries", Long.toString(batchSize));
    }
}
