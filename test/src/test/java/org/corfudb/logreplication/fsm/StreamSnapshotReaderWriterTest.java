package org.corfudb.logreplication.fsm;

import com.google.common.reflect.TypeToken;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.transmit.SnapshotReadMessage;
import org.corfudb.logreplication.transmit.StreamsSnapshotReader;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StreamSnapshotReaderWriterTest extends AbstractViewTest {
    static private final int NUM_KEYS = 5;
    static private final int NUM_STREAMS = 1;

    CorfuRuntime dataRuntime = null;
    CorfuRuntime readerRuntime = null;
    CorfuRuntime writerRuntime = null;
    CorfuRuntime testRuntime = null;

    Random random = new Random();
    HashMap<String, CorfuTable<Long, Long>> tables = new HashMap<>();

    /*
     * the in-memory data for corfu tables for verification.
     */
    HashMap<String, HashMap<Long, Long>> hashMap = new HashMap<String, HashMap<Long, Long>>();

    /*
     * store message generated by streamsnapshot reader and will play it at the writer side.
     */
    List<DataMessage> msgQ = new ArrayList<>();

    @Before
    public void setRuntime() {
        dataRuntime = getDefaultRuntime().connect();
        dataRuntime = getNewRuntime(getDefaultNode()).connect();
        readerRuntime = getNewRuntime(getDefaultNode()).connect();
        writerRuntime = getNewRuntime(getDefaultNode()).connect();
        testRuntime = getNewRuntime(getDefaultNode()).connect();
    }

    void openStreams(CorfuRuntime rt) {
        for (int i = 0; i < NUM_STREAMS; i++) {
            String name = "test" + Integer.toString(i);

            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            tables.put(name, table);
            hashMap.put(name, new HashMap<>());
        }
    }

    //Generate data and the same time push the data to the hashtable
    void generateData(int numKeys) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : tables.keySet()) {
                long key = i;
                tables.get(name).put(key, key);
                ((HashMap<Long, Long>)hashMap.get(name)).put(key, key);
            }
        }
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    void ckStreams() {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (CorfuTable map : tables.values()) {
            mcw1.addMap(map);
        }

         Token checkpointAddress = mcw1.appendCheckpoints(dataRuntime, "test");

        // Trim the log
        dataRuntime.getAddressSpaceView().prefixTrim(checkpointAddress);
        dataRuntime.getAddressSpaceView().gc();
    }

    void readMsgs(List<DataMessage> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read();
            msgQ.addAll(snapshotReadMessage.getMessages());
            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    void writeMsgs(List<DataMessage> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config);

        writer.reset(msgQ.get(0).metadata.getSnapshotTimestamp());

        for (DataMessage msg : msgQ) {
            writer.apply(msg);
        }
    }

    void verifyNoValue() {
        for (String name : tables.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            table.clear();
            assertThat(table.isEmpty() == true);
        }
    }

    void verifyData() throws Exception {
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);
            assertThat(hashMap.keySet().containsAll(table.keySet()));
            assertThat(table.keySet().containsAll(hashMap.keySet()));
            for (Long key : mapKeys.keySet()) {
                System.out.println("\ntable key " + key + " val " + table.get(key));
                if (table.get(key) == null)
                    throw new Exception("null value");
                assertThat(table.get(key) == mapKeys.get(key));
            }
        }
    }

    @Test
    public void test0() throws Exception {

        setRuntime();
        openStreams(dataRuntime);

        //generate some data in the streams
        //including a checkpoint in the streams
        generateData(NUM_KEYS);
        ckStreams();

        //update some data after checkpoint
        generateData(NUM_KEYS);

        //generate messages
        readMsgs(msgQ, hashMap.keySet(), readerRuntime);

        //call clear table
        verifyNoValue();

        //clear all tables, play messages
        writeMsgs(msgQ, hashMap.keySet(), writerRuntime);

        for (String name : hashMap.keySet()) {
            IStreamView sv = testRuntime.getStreamsView().get(CorfuRuntime.getStreamID(name));
            List<ILogData> dataList = sv.remaining();
            for (ILogData data : dataList) {
                System.out.println(data);
            }
            System.out.println("stream " + name + " numkeys " + dataList.size());
        }
        //verify data with hashtable
        verifyData();
    }
}
