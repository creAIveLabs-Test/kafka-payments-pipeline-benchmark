package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Produces N synthetic card transactions to the "transactions" topic, keyed by account
 * (so each account's events stay in order on one partition).
 *
 * Options:
 *   --count=2000000      number of transactions
 *   --rate=0             target tx/sec (0 = as fast as possible)
 *   --whale-share=0      share of traffic from one huge corporate account (hot-partition test)
 *   --salt-buckets=0     if > 0, spread the whale account over N keys (composite key fix)
 */
final class Generator {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Generator() {
    }

    static void run(Cfg cfg) throws Exception {
        long count = cfg.lng("count", 2_000_000);
        long rate = cfg.lng("rate", 0);
        int accounts = cfg.integer("accounts", 500_000);
        int merchants = cfg.integer("merchants", 20_000);
        double whaleShare = cfg.dbl("whale-share", 0.0);
        int saltBuckets = cfg.integer("salt-buckets", 0);
        int partitions = cfg.integer("partitions", 12);
        String topic = cfg.str("topic", "transactions");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.LINGER_MS_CONFIG, cfg.integer("linger-ms", 10));
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.integer("batch-size", 256 * 1024));
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfg.str("compression", "lz4"));
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 256L * 1024 * 1024);

        Stats stats = new Stats("generator", cfg);
        AtomicLongArray perPartition = new AtomicLongArray(partitions);
        AtomicLong errors = new AtomicLong();
        SplittableRandom rnd = new SplittableRandom(42);
        String whale = Data.accountId(0);
        stats.startPrinter();
        long startMs = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (long i = 0; i < count; i++) {
                boolean isWhale = whaleShare > 0 && rnd.nextDouble() < whaleShare;
                int acct = isWhale ? 0 : 1 + Data.skewed(rnd, accounts - 1, 2.5);
                int mer = Data.skewed(rnd, merchants, 3.0);
                String txId = new UUID(rnd.nextLong(), rnd.nextLong()).toString();
                String accountId = Data.accountId(acct);
                String key = (isWhale && saltBuckets > 0)
                    ? accountId + "_" + Math.floorMod(txId.hashCode(), saltBuckets)
                    : accountId;

                ObjectNode tx = JSON.createObjectNode();
                tx.put("txId", txId);
                tx.put("accountId", accountId);
                tx.put("merchantId", Data.merchantId(mer));
                tx.put("amountCents", 100 + (long) (Math.pow(rnd.nextDouble(), 3) * 250_000));
                tx.put("currency", "USD");
                tx.put("cardBin", 400000 + rnd.nextInt(5000));
                tx.put("createdMs", System.currentTimeMillis());

                producer.send(new ProducerRecord<>(topic, key, JSON.writeValueAsString(tx)), (md, ex) -> {
                    if (ex != null) {
                        errors.incrementAndGet();
                    } else {
                        stats.mark(1);
                        perPartition.incrementAndGet(md.partition());
                    }
                });

                if (rate > 0 && i % 1000 == 0) {
                    long expectedMs = startMs + (i * 1000 / rate);
                    long sleep = expectedMs - System.currentTimeMillis();
                    if (sleep > 0) Thread.sleep(sleep);
                }
            }
            producer.flush();
        }

        long[] parts = new long[partitions];
        for (int i = 0; i < partitions; i++) parts[i] = perPartition.get(i);
        double avg = Arrays.stream(parts).average().orElse(0);
        long max = Arrays.stream(parts).max().orElse(0);
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("startMs", startMs);
        extra.put("errors", errors.get());
        extra.put("whaleShare", whaleShare);
        extra.put("saltBuckets", saltBuckets);
        extra.put("recordsPerPartition", parts);
        extra.put("hottestPartitionVsAverage", avg == 0 ? 0 : Math.round(max / avg * 100) / 100.0);
        extra.put("producer", Map.of("acks", "all", "idempotence", true, "lingerMs", p.get(ProducerConfig.LINGER_MS_CONFIG),
            "batchSize", p.get(ProducerConfig.BATCH_SIZE_CONFIG), "compression", p.get(ProducerConfig.COMPRESSION_TYPE_CONFIG)));
        stats.writeReport(extra);
    }
}
