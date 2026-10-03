package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Stage 1: consume transactions, attach account and merchant data, publish to tx.enriched.
 *
 * Cache-aside per poll batch: one MGET to Redis for all distinct accounts and merchants in the batch,
 * one Postgres query for the misses, then SETEX the misses back with a jittered TTL
 * (jitter avoids many keys expiring at the same moment).
 */
final class Enricher {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String IN = "transactions";
    private static final String OUT = "tx.enriched";

    private Enricher() {
    }

    static String accountValue(String tier, boolean risk, String country) {
        return tier + "|" + (risk ? 1 : 0) + "|" + country;
    }

    static String merchantValue(String name, String category, String country) {
        return name + "|" + category + "|" + country;
    }

    static void run(Cfg cfg) throws Exception {
        int threads = cfg.integer("threads", 6);
        long idleExitMs = cfg.lng("idle-exit-ms", 20_000);
        String group = "enricher-" + cfg.runId();

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.jdbcUrl());
        hc.setMaximumPoolSize(threads + 2);
        HikariDataSource ds = new HikariDataSource(hc);
        JedisPoolConfig jc = new JedisPoolConfig();
        jc.setMaxTotal(threads * 2);
        JedisPool redis = new JedisPool(jc, cfg.redisHost(), cfg.redisPort());

        Stats stats = new Stats("enricher");
        AtomicLong cacheHits = new AtomicLong();
        AtomicLong cacheMisses = new AtomicLong();
        AtomicBoolean running = new AtomicBoolean(true);
        stats.startPrinter();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(cfg, group));
                     KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(cfg))) {
                    consumer.subscribe(List.of(IN));
                    while (running.get()) {
                        ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(200));
                        if (recs.isEmpty()) {
                            if (stats.started() && System.currentTimeMillis() - stats.lastMs() > idleExitMs) running.set(false);
                            continue;
                        }
                        List<ConsumerRecord<String, String>> list = new ArrayList<>(recs.count());
                        List<ObjectNode> txs = new ArrayList<>(recs.count());
                        Set<String> acctIds = new LinkedHashSet<>();
                        Set<String> merIds = new LinkedHashSet<>();
                        for (ConsumerRecord<String, String> r : recs) {
                            ObjectNode tx = (ObjectNode) JSON.readTree(r.value());
                            list.add(r);
                            txs.add(tx);
                            acctIds.add(tx.get("accountId").asText());
                            merIds.add(tx.get("merchantId").asText());
                        }
                        Map<String, String> accts;
                        Map<String, String> mers;
                        try (Jedis j = redis.getResource()) {
                            accts = lookup(j, "acct:", new ArrayList<>(acctIds), ids -> loadAccounts(ds, ids), cacheHits, cacheMisses);
                            mers = lookup(j, "mer:", new ArrayList<>(merIds), ids -> loadMerchants(ds, ids), cacheHits, cacheMisses);
                        }
                        long now = System.currentTimeMillis();
                        for (int i = 0; i < txs.size(); i++) {
                            ObjectNode tx = txs.get(i);
                            String[] a = split(accts.get(tx.get("accountId").asText()), "UNKNOWN|0|US");
                            String[] m = split(mers.get(tx.get("merchantId").asText()), "Unknown|OTHER|US");
                            tx.put("accountTier", a[0]);
                            tx.put("accountRisk", "1".equals(a[1]));
                            tx.put("accountCountry", a[2]);
                            tx.put("merchantName", m[0]);
                            tx.put("merchantCategory", m[1]);
                            tx.put("merchantCountry", m[2]);
                            tx.put("enrichedMs", now);
                            producer.send(new ProducerRecord<>(OUT, list.get(i).key(), JSON.writeValueAsString(tx)));
                        }
                        producer.flush();      // output is durable before we commit input offsets (at-least-once)
                        consumer.commitSync();
                        stats.mark(txs.size());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    running.set(false);
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.DAYS);
        redis.close();
        ds.close();

        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("threads", threads);
        extra.put("cacheHitsDistinctKeys", cacheHits.get());
        extra.put("cacheMissesDistinctKeys", cacheMisses.get());
        long lookups = cacheHits.get() + cacheMisses.get();
        extra.put("cacheHitRatio", lookups == 0 ? 0 : Math.round(cacheHits.get() * 1000.0 / lookups) / 1000.0);
        stats.writeReport(cfg.resultsDir(), extra);
    }

    private static Map<String, String> lookup(Jedis j, String prefix, List<String> ids,
                                              Function<List<String>, Map<String, String>> loader,
                                              AtomicLong hits, AtomicLong misses) {
        String[] keys = new String[ids.size()];
        for (int i = 0; i < keys.length; i++) keys[i] = prefix + ids.get(i);
        List<String> vals = j.mget(keys);
        Map<String, String> out = new HashMap<>(ids.size() * 2);
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            String v = vals.get(i);
            if (v != null) out.put(ids.get(i), v);
            else missing.add(ids.get(i));
        }
        hits.addAndGet(out.size());
        misses.addAndGet(missing.size());
        if (!missing.isEmpty()) {
            Map<String, String> loaded = loader.apply(missing);
            Pipeline p = j.pipelined();
            for (Map.Entry<String, String> e : loaded.entrySet()) {
                out.put(e.getKey(), e.getValue());
                p.setex(prefix + e.getKey(), 600 + ThreadLocalRandom.current().nextInt(120), e.getValue());
            }
            p.sync();
        }
        return out;
    }

    private static Map<String, String> loadAccounts(HikariDataSource ds, List<String> ids) {
        return query(ds, "SELECT account_id, tier, risk_flag, home_country FROM accounts WHERE account_id = ANY(?)", ids,
            rs -> accountValue(rs.getString(2), rs.getBoolean(3), rs.getString(4)));
    }

    private static Map<String, String> loadMerchants(HikariDataSource ds, List<String> ids) {
        return query(ds, "SELECT merchant_id, name, category, country FROM merchants WHERE merchant_id = ANY(?)", ids,
            rs -> merchantValue(rs.getString(2), rs.getString(3), rs.getString(4)));
    }

    interface RowFn {
        String apply(ResultSet rs) throws Exception;
    }

    private static Map<String, String> query(HikariDataSource ds, String sql, List<String> ids, RowFn fn) {
        Map<String, String> out = new HashMap<>();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            Array arr = c.createArrayOf("text", ids.toArray());
            ps.setArray(1, arr);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.put(rs.getString(1), fn.apply(rs));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    private static String[] split(String v, String def) {
        return (v == null ? def : v).split("\\|", -1);
    }

    static Properties consumerProps(Cfg cfg, String group) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, cfg.integer("max-poll-records", 2000));
        c.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 64 * 1024);
        c.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
        c.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        return c;
    }

    static Properties producerProps(Cfg cfg) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return p;
    }
}
