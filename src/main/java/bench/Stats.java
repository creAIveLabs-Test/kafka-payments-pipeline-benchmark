package bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-stage throughput and latency.
 * Throughput is measured from the first to the last record the stage processed,
 * plus the best 5-second window ("peak"). Latency (when recorded) is end-to-end:
 * time from the generator creating a transaction to this stage finishing it.
 */
final class Stats {
    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final String stage;
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong firstMs = new AtomicLong();
    private volatile long lastMs;
    private final Recorder recorder = new Recorder(3_600_000L, 3);
    private final Histogram latency = new Histogram(3_600_000L, 3);
    private final ScheduledExecutorService printer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, stage() + "-stats");
        t.setDaemon(true);
        return t;
    });
    private long lastPrintedCount;
    private double peakRate;

    Stats(String stage) {
        this.stage = stage;
    }

    private String stage() {
        return stage;
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
        printer.scheduleAtFixedRate(() -> {
            long c = count.get();
            double rate = (c - lastPrintedCount) / 5.0;
            lastPrintedCount = c;
            synchronized (latency) {
                latency.add(recorder.getIntervalHistogram());
                if (rate > peakRate) peakRate = rate;
                String lat = latency.getTotalCount() > 0
                    ? String.format(" p50=%dms p99=%dms", latency.getValueAtPercentile(50), latency.getValueAtPercentile(99))
                    : "";
                System.out.printf("[%s] total=%,d rate=%,.0f/s%s%n", stage, c, rate, lat);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    void writeReport(String resultsDir, Map<String, Object> extra) throws Exception {
        printer.shutdownNow();
        Map<String, Object> r = new LinkedHashMap<>();
        long c = count.get();
        long durationMs = Math.max(1, lastMs - firstMs.get());
        r.put("stage", stage);
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
            }
        }
        if (extra != null) r.putAll(extra);
        File dir = new File(resultsDir);
        dir.mkdirs();
        JSON.writeValue(new File(dir, stage + ".json"), r);
        System.out.printf("[%s] done: %,d records in %,d ms = %,d records/s%n", stage, c, durationMs, r.get("avgRecordsPerSec"));
    }
}
