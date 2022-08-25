package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.testing.TestReadAfterDCN;
import org.junit.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class StreamingRaceConditionTest extends AbstractViewTest {
    public CorfuRuntime r;
    public static final String TEST_TABLE_NAME = "TestTableName";
    public static final String TEST_STREAM_TAG = "TestStreamTag";
    private final CorfuCompactorManagement.StringKey TEST_KEY = CorfuCompactorManagement.StringKey.newBuilder().setKey("TestKey").build();
    private final CorfuCompactorManagement.StringKey TEST_VALUE = CorfuCompactorManagement.StringKey.newBuilder().setKey("TestValue").build();
    private static final int NUM_UPDATES = 1000;

    public void setRuntime() {
        // This module *really* needs separate & independent runtimes.
        r = getDefaultRuntime().connect(); // side-effect of using AbstractViewTest::getRouterFunction
        r = getNewRuntime(getDefaultNode()).setCacheDisabled(true).connect();
    }

    @Test
    public void checkRaceConditionTest() {
        setRuntime();
        CorfuStore corfuStore = new CorfuStore(r);

        List<String> tablesOfInterest = new ArrayList<>();
        tablesOfInterest.add(TEST_TABLE_NAME);

        Table<CorfuCompactorManagement.StringKey, CorfuCompactorManagement.StringKey, Message> exampleTable;
        try {
            exampleTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    TEST_TABLE_NAME,
                    CorfuCompactorManagement.StringKey.class,
                    CorfuCompactorManagement.StringKey.class,
                    null,
                    TableOptions.builder().schemaOptions(CorfuOptions.SchemaOptions.newBuilder().addStreamTag(TEST_STREAM_TAG).build()).build());

            TestDCNHandler testDCNHandler = new TestDCNHandler(corfuStore);
            corfuStore.subscribeListener(testDCNHandler, CORFU_SYSTEM_NAMESPACE, TEST_STREAM_TAG, tablesOfInterest);

            for (int i = 0; i < NUM_UPDATES; i++) {
                try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                    txn.putRecord(exampleTable, TEST_KEY, TEST_VALUE, null);
                    txn.commit();
                } catch (Exception e) {
                    log.error("Exception while putRecord, " + e.getMessage());
                }
            }

            TimeUnit.SECONDS.sleep(10);

            Assert.assertEquals(NUM_UPDATES, testDCNHandler.getSuccess());

        } catch (Exception e) {
            log.error("Exception while trying to open " + TEST_TABLE_NAME + " : " + e.getMessage());
        }

    }

    public class TestDCNHandler implements StreamListener {

        private final CorfuStore corfuStore;
        @Getter
        private int success = 0;

        public TestDCNHandler(CorfuStore corfuStore) {
            this.corfuStore = corfuStore;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            for (List<CorfuStreamEntry> entryList : results.getEntries().values()) {
                for (CorfuStreamEntry entry : entryList) {
                    try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                        CorfuCompactorManagement.StringKey stringValue =
                                (CorfuCompactorManagement.StringKey) txn.getRecord(TEST_TABLE_NAME, entry.getKey()).getPayload();
                        log.info("Found key : {}", stringValue.getKey());
                        success++;
                        txn.delete(TEST_TABLE_NAME, entry.getKey());
                    } catch (Exception e) {
                        log.info("Key : {} not found in stream {}", entry.getKey(), TEST_TABLE_NAME);
                    }
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {

        }
    }
}

