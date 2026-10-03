package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-stage throughput and latency for one process.
 *
 * - Throughput: records from the first to the last record this process handled, plus the best 5-second window.
 * - Timeline: every second "epochMs,cumulativeCount" is appended to <name>-<pid>.timeline.csv and flushed,
 *   so it survives kill -9 (used to measure dips and recovery during fault injection).
 * - Latency (when recorded): end-to-end, generator createdMs to this stage finishing the record.
 *   The full histogram is saved (base64) so reports can merge several instances exactly.
 */
final class Stats {
    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final String stage;
    private final String fileName;
    private final String resultsDir;
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong firstMs = new AtomicLong();
    private volatile long lastMs;
    private final Recorder recorder = new Recorder(3_600_000L, 3);
    private final Histogram latency = new Histogram(3_600_000L, 3);
    private final ScheduledExecutorService scheduler;
    private FileWriter timeline;
    private long lastPrintedCount;
    private double peakRate;

    Stats(String stage, Cfg cfg) {
        this.stage = stage;
        String instance = cfg.str("instance", "");
        this.fileName = instance.isEmpty() ? stage : stage + "-" + instance;
        this.resultsDir = cfg.resultsDir();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, fileName + "-stats");
            t.setDaemon(true);
            return t;
        });
    }

    void mark(long n) {
        long now = System.currentTimeMillis();
        firstMs.compareAndSet(0, now);
        lastMs = now;
        count.addAndGet(n);
    }

    void recordLatencyMs(long ms) {
        recorder.recordValue(Math.max(0, Math.min(ms, 3_599_999L)));
    }

    long count() {
        return count.get();
    }

    long lastMs() {
        return lastMs;
    }

    boolean started() {
        return firstMs.get() != 0;
    }

    void startPrinter() {
        try {
            new File(resultsDir).mkdirs();
            long pid = ManagementFactory.getRuntimeMXBean().getPid();
            timeline = new FileWriter(new File(resultsDir, fileName + "-" + pid + ".timeline.csv"), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                timeline.write(System.currentTimeMillis() + "," + count.get() + "\n");
                timeline.flush();
            } catch (IOException ignored) {
                // best effort
            }
        }, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(() -> {
            long c = count.get();
            double rate = (c - lastPrintedCount) / 5.0;
            lastPrintedCount = c;
            synchronized (latency) {
                latency.add(recorder.getIntervalHistogram());
                if (rate > peakRate) peakRate = rate;
                String lat = latency.getTotalCount() > 0
                    ? String.format(" p50=%dms p99=%dms", latency.getValueAtPercentile(50), latency.getValueAtPercentile(99))
                    : "";
                System.out.printf("[%s] total=%,d rate=%,.0f/s%s%n", fileName, c, rate, lat);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    void writeReport(Map<String, Object> extra) throws Exception {
        scheduler.shutdownNow();
        Map<String, Object> r = new LinkedHashMap<>();
        long c = count.get();
        long durationMs = Math.max(1, lastMs - firstMs.get());
        r.put("stage", stage);
        r.put("file", fileName);
        r.put("records", c);
        r.put("firstMs", firstMs.get());
        r.put("lastMs", lastMs);
        r.put("durationMs", durationMs);
        r.put("avgRecordsPerSec", Math.round(c * 1000.0 / durationMs));
        synchronized (latency) {
            latency.add(recorder.getIntervalHistogram());
            r.put("peak5sRecordsPerSec", Math.round(peakRate));
            if (latency.getTotalCount() > 0) {
                Map<String, Object> l = new LinkedHashMap<>();
                l.put("p50", latency.getValueAtPercentile(50));
                l.put("p95", latency.getValueAtPercentile(95));
                l.put("p99", latency.getValueAtPercentile(99));
                l.put("max", latency.getMaxValue());
                r.put("endToEndLatencyMs", l);
                ByteBuffer buf = ByteBuffer.allocate(latency.getNeededByteBufferCapacity());
                int len = latency.encodeIntoCompressedByteBuffer(buf);
                byte[] bytes = new byte[len];
                buf.flip();
                buf.get(bytes);
                r.put("latencyHistogram", Base64.getEncoder().encodeToString(bytes));
            }
        }
        if (extra != null) r.putAll(extra);
        File dir = new File(resultsDir);
        dir.mkdirs();
        JSON.writeValue(new File(dir, fileName + ".json"), r);
        if (timeline != null) timeline.close();
        System.out.printf("[%s] done: %,d records in %,d ms = %,d records/s%n", fileName, c, durationMs, r.get("avgRecordsPerSec"));
    }
}
