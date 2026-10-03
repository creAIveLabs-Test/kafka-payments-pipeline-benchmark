package bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 3: consume decisions and write them to Postgres in batches.
 * One INSERT ... SELECT FROM unnest(arrays) per poll batch, ON CONFLICT DO NOTHING,
 * and Kafka offsets are committed only after the database commit (at-least-once + idempotent = effectively once).
 * Records end-to-end latency: generator createdMs to ledger commit.
 */
final class Ledger {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String INSERT = """
        INSERT INTO ledger_entries (tx_id, account_id, merchant_id, amount_cents, decision, reason, velocity_5m, created_ms, posted_ms)
        SELECT * FROM unnest(?::text[]::uuid[], ?::text[], ?::text[], ?::bigint[], ?::text[], ?::text[], ?::int[], ?::bigint[], ?::bigint[])
        ON CONFLICT (tx_id) DO NOTHING
        """;

    private Ledger() {
    }

    static void run(Cfg cfg) throws Exception {
        int threads = cfg.integer("threads", 6);
        long expect = cfg.lng("expect", 2_000_000);
        String group = "ledger-" + cfg.runId();

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.jdbcUrl());
        hc.setMaximumPoolSize(threads);
        hc.setAutoCommit(false);
        HikariDataSource ds = new HikariDataSource(hc);

        Stats stats = new Stats("ledger", cfg);
        AtomicLong inserted = new AtomicLong();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong commitFailures = new AtomicLong();
        stats.startPrinter();

        // Several ledger instances can share the work, so "done" is decided by the table, not by this process's count.
        Thread monitor = new Thread(() -> {
            while (running.get()) {
                try {
                    Thread.sleep(3000);
                    try (Connection c = ds.getConnection(); Statement s = c.createStatement();
                         ResultSet rs = s.executeQuery("SELECT count(*) FROM ledger_entries")) {
                        rs.next();
                        if (rs.getLong(1) >= expect) {
                            cfg.doneFile().createNewFile();
                            running.set(false);
                        }
                        c.commit();
                    }
                } catch (Exception e) {
                    // keep trying; the consumers' own idle check is the fallback
                }
            }
        }, "ledger-done-monitor");
        monitor.setDaemon(true);
        monitor.start();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                Properties cp = Enricher.consumerProps(cfg, group);
                cp.put("max.poll.records", cfg.integer("max-poll-records", 5000));
                cp.put("fetch.min.bytes", 256 * 1024);
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
                     Connection c = ds.getConnection();
                     PreparedStatement ps = c.prepareStatement(INSERT)) {
                    consumer.subscribe(List.of("tx.decisions"));
                    while (running.get()) {
                        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(200));
                        if (cfg.doneFile().exists()) running.set(false);
                        if (recs.isEmpty()) {
                            if (stats.started() && System.currentTimeMillis() - stats.lastMs() > cfg.safetyIdleMs()) running.set(false);
                            continue;
                        }
                        int n = recs.count();
                        String[] txId = new String[n], acct = new String[n], mer = new String[n], dec = new String[n], reason = new String[n];
                        Long[] amount = new Long[n], created = new Long[n], posted = new Long[n];
                        Integer[] vel = new Integer[n];
                        int i = 0;
                        for (ConsumerRecord<String, String> r : recs) {
                            JsonNode d = JSON.readTree(r.value());
                            txId[i] = d.get("txId").asText();
                            acct[i] = d.get("accountId").asText();
                            mer[i] = d.get("merchantId").asText();
                            amount[i] = d.get("amountCents").asLong();
                            dec[i] = d.get("decision").asText();
                            reason[i] = d.get("reason").asText();
                            vel[i] = d.get("velocity5m").asInt();
                            created[i] = d.get("createdMs").asLong();
                            i++;
                        }
                        long now = System.currentTimeMillis();
                        for (int k = 0; k < n; k++) posted[k] = now;
                        ps.setArray(1, c.createArrayOf("text", txId));
                        ps.setArray(2, c.createArrayOf("text", acct));
                        ps.setArray(3, c.createArrayOf("text", mer));
                        ps.setArray(4, c.createArrayOf("bigint", amount));
                        ps.setArray(5, c.createArrayOf("text", dec));
                        ps.setArray(6, c.createArrayOf("text", reason));
                        ps.setArray(7, c.createArrayOf("int4", vel));
                        ps.setArray(8, c.createArrayOf("bigint", created));
                        ps.setArray(9, c.createArrayOf("bigint", posted));
                        inserted.addAndGet(ps.executeUpdate());
                        c.commit();                // database first ...
                        try {
                            consumer.commitSync();     // ... then offsets
                        } catch (org.apache.kafka.clients.consumer.RetriableCommitFailedException
                                 | org.apache.kafka.common.errors.RebalanceInProgressException
                                 | org.apache.kafka.clients.consumer.CommitFailedException e) {
                            // A rebalance moved our partitions. Not fatal: the batch is redelivered to the new owner
                            // and duplicates are absorbed downstream (ON CONFLICT in the ledger).
                            commitFailures.incrementAndGet();
                        }
                        long committed = System.currentTimeMillis();
                        for (int k = 0; k < n; k++) stats.recordLatencyMs(committed - created[k]);
                        stats.mark(n);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    running.set(false);
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.DAYS);

        long rows;
        Map<String, Long> decisions = new LinkedHashMap<>();
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM ledger_entries")) {
                rs.next();
                rows = rs.getLong(1);
            }
            try (ResultSet rs = s.executeQuery("SELECT decision, count(*) FROM ledger_entries GROUP BY 1 ORDER BY 1")) {
                while (rs.next()) decisions.put(rs.getString(1), rs.getLong(2));
            }
            c.commit();
        }
        ds.close();

        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("threads", threads);
        extra.put("offsetCommitsRejectedByRebalance", commitFailures.get());
        extra.put("rowsInserted", inserted.get());
        extra.put("duplicatesSkipped", stats.count() - inserted.get());
        extra.put("rowsInLedgerTable", rows);
        extra.put("decisions", decisions);
        stats.writeReport(extra);
    }
}
