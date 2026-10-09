package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Stage 2 (Kafka Streams): per-key velocity over a sliding 5-minute window, kept in a RocksDB
 * state store (backed by a changelog topic), then rules produce APPROVE / DECLINE / REVIEW.
 *
 * Because the input is keyed by account, all of one account's events are processed in order by
 * one stream task, so the velocity count needs no locks.
 */
final class Scorer {
    static final String STORE = "velocity";
    private static final long WINDOW_MS = 5 * 60 * 1000;
    private static final int MAX_TIMESTAMPS = 64;

    private Scorer() {
    }

    static void run(Cfg cfg) throws Exception {
        int threads = cfg.integer("threads", 6);
        String guarantee = cfg.str("guarantee", StreamsConfig.AT_LEAST_ONCE);

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "scorer-" + cfg.runId());
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, guarantee);
        p.put(StreamsConfig.STATE_DIR_CONFIG, cfg.resultsDir() + "/state-" + cfg.str("instance", "0"));
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, StreamsConfig.EXACTLY_ONCE_V2.equals(guarantee) ? 200 : 1000);
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 64L * 1024 * 1024);
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 2000);
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), cfg.integer("session-timeout-ms", 10_000));
        p.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 3_000);
        p.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 10);
        p.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 256 * 1024);
        p.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "lz4");

        Stats stats = new Stats("scorer", cfg);
        StreamsBuilder b = new StreamsBuilder();
        b.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE), Serdes.String(), Serdes.ByteArray())
            .withCachingEnabled());
        b.stream("tx.enriched", Consumed.with(Serdes.String(), Serdes.String()))
            .processValues(() -> new ScoreProcessor(stats), STORE)
            .to("tx.decisions", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(b.build(), p);
        streams.setUncaughtExceptionHandler(e -> {
            e.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        stats.startPrinter();
        streams.start();
        while (true) {
            Thread.sleep(1000);
            KafkaStreams.State st = streams.state();
            if (st == KafkaStreams.State.ERROR || st == KafkaStreams.State.NOT_RUNNING) break;
            if (cfg.doneFile().exists()) break;
            if (stats.started() && System.currentTimeMillis() - stats.lastMs() > cfg.safetyIdleMs()) break;
        }
        streams.close(Duration.ofSeconds(30));

        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("threads", threads);
        extra.put("processingGuarantee", guarantee);
        stats.writeReport(extra);
    }

    static final class ScoreProcessor implements FixedKeyProcessor<String, String, String> {
        private static final ObjectMapper JSON = new ObjectMapper();
        private final Stats stats;
        private FixedKeyProcessorContext<String, String> ctx;
        private KeyValueStore<String, byte[]> store;

        ScoreProcessor(Stats stats) {
            this.stats = stats;
        }

        @Override
        public void init(FixedKeyProcessorContext<String, String> context) {
            this.ctx = context;
            this.store = context.getStateStore(STORE);
        }

        @Override
        public void process(FixedKeyRecord<String, String> rec) {
            try {
                ObjectNode tx = (ObjectNode) JSON.readTree(rec.value());
                // Replayed data carries its original time; velocity must use it, not the replay clock.
                long ts = tx.has("eventMs") ? tx.get("eventMs").asLong() : tx.get("createdMs").asLong();
                long[] kept = window(store.get(rec.key()), ts);
                store.put(rec.key(), encode(kept));
                int velocity = kept.length;

                long amount = tx.get("amountCents").asLong();
                String tier = tx.get("accountTier").asText();
                boolean risky = tx.get("accountRisk").asBoolean();
                String category = tx.get("merchantCategory").asText();
                boolean crossBorder = !tx.get("merchantCountry").asText().equals(tx.get("accountCountry").asText());

                String decision;
                String reason;
                if (risky && "GAMBLING".equals(category)) {
                    decision = "DECLINE";
                    reason = "risk_account_gambling";
                } else if (velocity > 25 && !"CORPORATE".equals(tier)) {
                    decision = "DECLINE";
                    reason = "velocity_limit";
                } else if (amount > 150_000 && "BASIC".equals(tier)) {
                    decision = "REVIEW";
                    reason = "high_amount_basic_tier";
                } else if (crossBorder && amount > 100_000) {
                    decision = "REVIEW";
                    reason = "cross_border_high_amount";
                } else {
                    decision = "APPROVE";
                    reason = "ok";
                }
                tx.put("velocity5m", velocity);
                tx.put("decision", decision);
                tx.put("reason", reason);
                tx.put("scoredMs", System.currentTimeMillis());
                ctx.forward(rec.withValue(JSON.writeValueAsString(tx)));
                stats.mark(1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /** Keeps timestamps inside the 5-minute window ending at ts, plus ts itself. */
        static long[] window(byte[] prev, long ts) {
            int n = prev == null ? 0 : prev.length / 8;
            long[] tmp = new long[Math.min(n, MAX_TIMESTAMPS - 1) + 1];
            int k = 0;
            if (prev != null) {
                ByteBuffer bb = ByteBuffer.wrap(prev);
                int skip = Math.max(0, n - (MAX_TIMESTAMPS - 1));
                for (int i = 0; i < n; i++) {
                    long t = bb.getLong();
                    if (i >= skip && t > ts - WINDOW_MS) tmp[k++] = t;
                }
            }
            tmp[k++] = ts;
            long[] out = new long[k];
            System.arraycopy(tmp, 0, out, 0, k);
            return out;
        }

        static byte[] encode(long[] ts) {
            ByteBuffer bb = ByteBuffer.allocate(ts.length * 8);
            for (long t : ts) bb.putLong(t);
            return bb.array();
        }
    }
}
