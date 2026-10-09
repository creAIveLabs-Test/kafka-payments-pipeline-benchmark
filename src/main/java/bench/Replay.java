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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Replays the IBM TabFormer file into the "transactions" topic in file order (sorted by user, card and time),
 * keyed by account like the Generator, with the same producer settings, so runs are comparable.
 *
 * The message keeps the Generator's fields and adds the original transaction time (eventMs, used by the
 * Scorer's velocity window), channel and the issuer error. The fraud label is not sent.
 *
 * Options:
 *   --csv=path           card_transaction.v1.csv
 *   --count=0            rows to replay from the top of the file (0 = all)
 *   --rate=0             target tx/sec (0 = as fast as possible)
 */
final class Replay {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Replay() {
    }

    static void run(Cfg cfg) throws Exception {
        String csv = cfg.str("csv", "");
        long count = cfg.lng("count", 0);
        long rate = cfg.lng("rate", 0);
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
        stats.startPrinter();
        long startMs = System.currentTimeMillis();

        long rows;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            rows = Ibm.forEach(csv, count, r -> {
                ObjectNode tx = JSON.createObjectNode();
                tx.put("txId", Ibm.txId(r.index()));
                tx.put("accountId", r.accountId());
                tx.put("merchantId", r.merchantId());
                tx.put("amountCents", r.amountCents());
                tx.put("currency", "USD");
                tx.put("createdMs", System.currentTimeMillis());
                tx.put("eventMs", r.eventMs());
                tx.put("channel", r.channel());
                tx.put("issuerError", r.errors());

                producer.send(new ProducerRecord<>(topic, r.accountId(), JSON.writeValueAsString(tx)), (md, ex) -> {
                    if (ex != null) {
                        errors.incrementAndGet();
                    } else {
                        stats.mark(1);
                        perPartition.incrementAndGet(md.partition());
                    }
                });

                long i = r.index();
                if (rate > 0 && i % 1000 == 0) {
                    long sleep = startMs + (i * 1000 / rate) - System.currentTimeMillis();
                    if (sleep > 0) Thread.sleep(sleep);
                }
            });
            producer.flush();
        }

        long[] parts = new long[partitions];
        for (int i = 0; i < partitions; i++) parts[i] = perPartition.get(i);
        double avg = Arrays.stream(parts).average().orElse(0);
        long max = Arrays.stream(parts).max().orElse(0);
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("startMs", startMs);
        extra.put("source", "IBM TabFormer card_transaction.v1.csv");
        extra.put("rowsRead", rows);
        extra.put("errors", errors.get());
        extra.put("recordsPerPartition", parts);
        extra.put("hottestPartitionVsAverage", avg == 0 ? 0 : Math.round(max / avg * 100) / 100.0);
        extra.put("producer", Map.of("acks", "all", "idempotence", true, "lingerMs", p.get(ProducerConfig.LINGER_MS_CONFIG),
            "batchSize", p.get(ProducerConfig.BATCH_SIZE_CONFIG), "compression", p.get(ProducerConfig.COMPRESSION_TYPE_CONFIG)));
        stats.writeReport(extra);
    }
}
