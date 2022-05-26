package org.corfudb.perf;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class CorfuStorePerfMain {

    private enum OperationType {
        writeData,
        readData,
        readWriteData,
        listKeys
    }

    private final CorfuStorePerfHelper corfuStorePerfHelper;
    private final CorfuRuntime runtime;

    private static String host;
    private static int port;
    private static List<String> operations = new ArrayList<>();

    private static int numThreads = 1;
    private static long numItems = 10000;
    private static long numUpdates = 1;
    private static int itemSize = 1000;
    private static int batchSize = 1000;
    private static int maxCacheEntries = 5000;
    private static boolean tlsEnabled = false;
    private static String diskPath = null;
    private static String tableName = "ExampleTable";

    private static String keystore;
    private static String ks_password;
    private static String truststore;
    private static String truststore_password;

    final static int SYSTEM_EXIT_ERROR_CODE = 1;
    final static int SYSTEM_DOWN_RETRIES = 200;
    private static final String TABLE_KEY_PREFIX = "ExampleTableKeyNew";
    private static final String USAGE = "Usage: corfu-browser "
            + "--host=<host> "
            + "--port=<port> "
            + "--operation=<operation> "
            + "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] "
            + "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] "
            + "[--tlsEnabled=<tls_enabled>] "
            + "[--diskPath=<pathToTempDirForLargeTables>] "
            + "[--tableName=<tableName>] "
            + "[--maxCacheEntries=<maxCacheEntries>] "
            + "[--numThreads=<numThreads>] "
            + "[--numItems=<numItems>] "
            + "[--numUpdates=<numUpdates>] "
            + "[--batchSize=<itemsPerTransaction>] "
            + "[--itemSize=<sizeOfEachRecordValue>]\n"
            + "Options:\n"
            + "--host=<host>   Hostname\n"
            + "--port=<port>   Port\n"
            + "--operation=<listTables|infoTable|showTable|clearTable> Operation\n"
            + "--keystore=<keystore_file> KeyStore File\n"
            + "--ks_password=<keystore_password> KeyStore Password\n"
            + "--truststore=<truststore_file> TrustStore File\n"
            + "--truststore_password=<truststore_password> Truststore Password\n"
            + "--tlsEnabled=<tls_enabled>\n"
            + "--diskPath=<pathToTempDirForLargeTables> Path to Temp Dir\n"
            + "--tableName=<tableName> Name of table to perform operation on\n"
            + "--maxCacheEntries=<maxCacheEntries> Maximum entris in reacCache\n"
            + "--numThreads=<numThreads> Total Number of threads to run the experiment with\n"
            + "--numItems=<numItems> Total Number of items for writeData\n"
            + "--numUpdates=<numUpdates> Total Number of updates per item for writeData\n"
            + "--batchSize=<batchSize> Number of records per transaction\n"
            + "--itemSize=<itemSize> Size of each item in bytes for writeData";

    CorfuStorePerfMain () {
        CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder
                builder = CorfuRuntime.CorfuRuntimeParameters.builder()
                .cacheDisabled(true)
                .systemDownHandler(() -> System.exit(SYSTEM_EXIT_ERROR_CODE))
                .systemDownHandlerTriggerLimit(SYSTEM_DOWN_RETRIES)
                .metricsEnabled(true)
                .maxCacheEntries(maxCacheEntries)
                .tlsEnabled(tlsEnabled);
        if (tlsEnabled) {
            builder.tlsEnabled(tlsEnabled)
                    .keyStore(keystore)
                    .ksPasswordFile(ks_password)
                    .trustStore(truststore)
                    .tsPasswordFile(truststore_password);
        }

        runtime = CorfuRuntime.fromParameters(builder.build());

        String singleNodeEndpoint = String.format("%s:%d", host, port);
        runtime.parseConfigurationString(singleNodeEndpoint);
        log.info("Connecting to corfu cluster at {}", singleNodeEndpoint);
        runtime.connect();
        log.info("Successfully connected to {}", singleNodeEndpoint);

        corfuStorePerfHelper = new CorfuStorePerfHelper(runtime, diskPath, tableName);
    }

    public void runExperiment() {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        int i;
        switch (Enum.valueOf(CorfuStorePerfMain.OperationType.class, operations.get(0))) {
            case readData:
                i = numThreads;
                while (--i >= 0) {
                    executorService.execute(() -> corfuStorePerfHelper.readData(batchSize));
                }
                break;
            case writeData:
                i = numThreads;
                while (--i >= 0) {
                    int finalI = i;
                    executorService.execute(() -> corfuStorePerfHelper.writeData(batchSize,
                            itemSize, numItems, numUpdates, TABLE_KEY_PREFIX + finalI));
                }
                break;
            case readWriteData:
                i = numThreads/2;
                while (--i >= 0) {
                    int finalI1 = i;
                    executorService.execute(() -> corfuStorePerfHelper.writeData(batchSize,
                            itemSize, numItems, numUpdates, TABLE_KEY_PREFIX + finalI1));
                    executorService.execute(() -> corfuStorePerfHelper.readData(batchSize));
                }
                break;
            case listKeys:
                i = numThreads;
                while (--i >= 0) {
                    executorService.execute(() -> corfuStorePerfHelper.listKeys());
                }
                break;
        }
        executorService.shutdown();
    }

    public static void main(String[] args) {
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        CorfuStorePerfMain.parseArgs(opts);

        CorfuStorePerfMain corfuStorePerfMain = new CorfuStorePerfMain();
        corfuStorePerfMain.runExperiment();
    }


    public static void parseArgs (Map<String, Object> opts) {
        host = opts.get("--host").toString();
        port = Integer.parseInt(opts.get("--port").toString());
        operations.add(opts.get("--operation").toString());

        if (opts.get("--diskPath") != null) {
            diskPath = opts.get("--diskPath").toString();
        }
        if (opts.get("--numThreads") != null) {
            numThreads = Integer.parseInt(opts.get("--numThreads").toString());
        }
        if (opts.get("--numItems") != null) {
            numItems = Integer.parseInt(opts.get("--numItems").toString());
        }
        if (opts.get("--numUpdates") != null) {
            numUpdates = Integer.parseInt(opts.get("--numUpdates").toString());
        }
        if (opts.get("--batchSize") != null) {
            batchSize = Integer.parseInt(opts.get("--batchSize").toString());
        }
        if (opts.get("--itemSize") != null) {
            itemSize = Integer.parseInt(opts.get("--itemSize").toString());
        }
        if (opts.get("--tableName") != null) {
            tableName = opts.get("--tableName").toString();
        }
        if (opts.get("--maxCacheEntries") != null) {
            maxCacheEntries = Integer.parseInt(opts.get("--maxCacheEntries").toString());
        }
        if (opts.get("--tlsEnabled") != null) {
            tlsEnabled = Boolean.parseBoolean(opts.get("--tlsEnabled").toString());
        }
        if (tlsEnabled) {
            keystore = opts.get("--keystore").toString();
            ks_password = opts.get("--ks_password").toString();
            truststore = opts.get("--truststore").toString();
            truststore_password = opts.get("--truststore_password").toString();
        }
    }
}
