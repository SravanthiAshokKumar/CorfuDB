package org.corfudb.perf;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.*;

import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class CorfuStorePerfHelper {
    private final CorfuRuntime runtime;
    private final String diskPath;
    private final CorfuStore corfuStore;

    private static final String PERF_NAMESPACE = "PERF_TESTING";
    private static final String EXAMPLE_TABLE_NAME = "ExampleTableNew";

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     * @param diskPath path to temp disk directory for loading large tables
     *                 that won't fit into memory
     */
    public CorfuStorePerfHelper(CorfuRuntime runtime, String diskPath) {
        this.runtime = runtime;
        this.diskPath = diskPath;
        corfuStore = new CorfuStore(this.runtime);
        openExampleTable();
    }

    private void openExampleTable () {
        try {
            Table<CorfuStoreMetadata.TableName, CorfuStoreMetadata.TableName, CorfuStoreMetadata.TableName> exampleTable =
                    corfuStore.openTable(
                            PERF_NAMESPACE,
                            EXAMPLE_TABLE_NAME,
                            CorfuStoreMetadata.TableName.class,
                            CorfuStoreMetadata.TableName.class,
                            null,
                            TableOptions.builder().persistentDataPath(Paths.get(this.diskPath)).build()
                    );
        } catch (Exception e) {
            log.info("Exception while opening table, ", e);
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
                TxBuilder tx = corfuStore.tx(PERF_NAMESPACE);
                for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                    byte[] array = new byte[entrySize];
                    new Random().nextBytes(array);
                    CorfuStoreMetadata.TableName dummyKey = CorfuStoreMetadata.TableName.newBuilder()
                            .setTableName(tableKeyPrefix + itemsRemaining).build();
                    CorfuStoreMetadata.TableName dummyVal = CorfuStoreMetadata.TableName.newBuilder()
                            .setTableName(new String(array)).build();
                    tx.update(EXAMPLE_TABLE_NAME, dummyKey, dummyVal, null);
                }
                tx.commit();
                endTime = System.currentTimeMillis();
//                log.info("writeData: of table {} per batch of batchSize {} took {}ms", EXAMPLE_TABLE_NAME,
//                        batchSize, endTime - totalStartTime);
            }
        }
        log.info("writeData: of table {} with {} entries each updated {} times took {}ms", EXAMPLE_TABLE_NAME,
               numEntries, numUpdates, endTime - totalStartTime);
    }

    public void listKeys() {
        long startTime = System.currentTimeMillis();
        Query q = corfuStore.query(PERF_NAMESPACE);
        int size = q.keySet(EXAMPLE_TABLE_NAME, null).size();
        log.info("listKeys for {} keys took {} ms", size, System.currentTimeMillis() - startTime);
    }

    public void readData(int batchSize) {
        //TODO
    }
}
