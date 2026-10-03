package bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * report --dir=results/runs/<name>   -> <dir>/RESULTS.md and one line in results/summary.jsonl
 * report --summary=true              -> results/SUMMARY.md comparing every run
 *
 * Merges several instances of a stage (stage.json or stage-N.json), merges their latency histograms exactly,
 * and, when fault.json exists, measures the throughput dip and recovery time from the per-second timelines.
 */
final class Report {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Report() {
    }

    /** One stage, all instances combined. */
    static final class Agg {
        int instances;
        long records;
        long firstMs = Long.MAX_VALUE;
        long lastMs;
        long peakSum;
        long duplicates;
        long ledgerRows;
        String notes = "";
        JsonNode decisions;
        Histogram latency;

        long avg() {
            return records == 0 ? 0 : Math.round(records * 1000.0 / Math.max(1, lastMs - firstMs));
        }
    }

    static void run(Cfg cfg) throws Exception {
        if (!"false".equals(cfg.str("summary", "false"))) {
            summary(cfg);
            return;
        }
        File dir = new File(cfg.str("dir", cfg.resultsDir()));
        String name = cfg.str("name", dir.getName());
        JsonNode gen = read(dir, "generator");
        JsonNode env = read(dir, "run");
        JsonNode fault = read(dir, "fault");
        Agg enr = agg(dir, "enricher");
        Agg sco = agg(dir, "scorer");
        Agg led = agg(dir, "ledger");

        long rows = led.ledgerRows;
        long pipelineMs = Math.max(1, led.lastMs - gen.path("startMs").asLong());
        long pipelineRate = Math.round(rows * 1000.0 / pipelineMs);

        StringBuilder md = new StringBuilder("# Run: " + name + "\n\n");
        if (!env.isMissingNode()) {
            md.append("| Setting | Value |\n|---|---|\n");
            env.fields().forEachRemaining(e -> md.append("| ").append(e.getKey()).append(" | ").append(e.getValue().asText()).append(" |\n"));
            md.append('\n');
        }
        md.append("## End-to-end pipeline\n\n");
        md.append(String.format("- **%,d transactions** committed to the ledger, from generator start to the last commit in **%.1f s** = **%,d transactions/sec end to end**%n",
            rows, pipelineMs / 1000.0, pipelineRate));
        if (led.latency != null && led.latency.getTotalCount() > 0) {
            md.append(String.format("- End-to-end latency (created to committed in Postgres): p50 %d ms, p95 %d ms, p99 %d ms, max %d ms%n",
                led.latency.getValueAtPercentile(50), led.latency.getValueAtPercentile(95),
                led.latency.getValueAtPercentile(99), led.latency.getMaxValue()));
        }
        md.append(String.format("- Ledger rows: %,d of %,d expected; duplicates skipped by ON CONFLICT: %,d; decisions: %s%n%n",
            rows, gen.path("records").asLong(), led.duplicates, led.decisions));

        md.append("## Per stage (all instances combined)\n\n| Stage | Instances | Records | Avg records/sec | Peak 5 s records/sec (sum of instances) | Notes |\n|---|---|---|---|---|---|\n");
        md.append(String.format("| Generator | 1 | %,d | %,d | %,d | hottest partition = %sx average |%n", gen.path("records").asLong(),
            gen.path("avgRecordsPerSec").asLong(), gen.path("peak5sRecordsPerSec").asLong(), gen.path("hottestPartitionVsAverage").asText()));
        stage(md, "Enricher (Redis + Postgres)", enr);
        stage(md, "Scorer (Kafka Streams + RocksDB)", sco);
        stage(md, "Ledger (Postgres batch insert)", led);
        md.append("\nRecords per partition (generator): ").append(gen.path("recordsPerPartition")).append("\n\n");

        ObjectNode faultResult = null;
        if (!fault.isMissingNode()) {
            faultResult = analyzeFault(dir, fault);
            md.append("## Fault injection\n\n| | |\n|---|---|\n");
            md.append("| Fault | ").append(fault.path("type").asText()).append(" (").append(fault.path("target").asText()).append(") |\n");
            md.append(String.format("| Injected at | %,d ledger rows committed |%n", fault.path("atRows").asLong()));
            if (fault.has("restartedAfterSec")) md.append("| Restarted after | ").append(fault.path("restartedAfterSec").asText()).append(" s |\n");
            faultResult.fields().forEachRemaining(e -> md.append("| ").append(e.getKey()).append(" | ").append(e.getValue().asText()).append(" |\n"));
            md.append(String.format("| Data check | %,d rows of %,d expected, %,d duplicates absorbed |%n%n", rows, gen.path("records").asLong(), led.duplicates));
        }
        Files.writeString(new File(dir, "RESULTS.md").toPath(), md.toString());

        ObjectNode line = JSON.createObjectNode();
        line.put("name", name);
        line.put("records", rows);
        line.put("expected", gen.path("records").asLong());
        line.put("pipelineSeconds", Math.round(pipelineMs / 100.0) / 10.0);
        line.put("pipelineTxPerSec", pipelineRate);
        line.put("enricherPerSec", enr.avg());
        line.put("scorerPerSec", sco.avg());
        line.put("ledgerPerSec", led.avg());
        line.put("instances", Math.max(1, led.instances));
        line.put("p50ms", led.latency == null ? 0 : led.latency.getValueAtPercentile(50));
        line.put("p99ms", led.latency == null ? 0 : led.latency.getValueAtPercentile(99));
        line.put("hottestPartitionVsAverage", gen.path("hottestPartitionVsAverage").asDouble());
        line.put("duplicates", led.duplicates);
        line.put("mode", env.path("kafka").asText().contains("3 brokers") ? "3 brokers, RF 3" : "1 broker, RF 1");
        if (faultResult != null) {
            line.put("fault", fault.path("type").asText());
            line.put("faultDip", faultResult.path("Lowest throughput after fault").asText());
            line.put("faultRecovery", faultResult.path("Recovered after").asText().replaceAll(" \\(.*", ""));
        }
        if (!env.isMissingNode()) line.set("settings", env);
        Files.writeString(new File(cfg.resultsDir(), "summary.jsonl").toPath(), JSON.writeValueAsString(line) + "\n",
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.print(md);
    }

    /**
     * Baseline = median ledger rows/sec in the 15 s before the fault.
     * Impact window = up to 90 s after the fault, cut off 5 s before the ledger's last active second
     * (so the normal end of the run isn't counted as an outage).
     * Recovered after = seconds from the fault until throughput (3-second average) stays at or above 50% of baseline.
     */
    private static ObjectNode analyzeFault(File dir, JsonNode fault) throws Exception {
        TreeMap<Long, Long> perSec = timeline(dir, "ledger");
        long faultSec = fault.path("injectedMs").asLong() / 1000;
        List<Long> before = new ArrayList<>();
        for (long s = faultSec - 15; s < faultSec; s++) {
            long v = perSec.getOrDefault(s, 0L);
            if (v > 0) before.add(v);
        }
        ObjectNode r = JSON.createObjectNode();
        if (before.isEmpty() || perSec.isEmpty()) {
            r.put("Note", "not enough data before the fault to measure a baseline");
            return r;
        }
        long[] b = before.stream().mapToLong(Long::longValue).sorted().toArray();
        long baseline = b[b.length / 2];
        long lastActive = perSec.descendingMap().entrySet().stream().filter(e -> e.getValue() > 0)
            .map(Map.Entry::getKey).findFirst().orElse(faultSec);
        long windowEnd = Math.min(faultSec + 90, lastActive - 5);
        long dip = Long.MAX_VALUE;
        int zeroSecs = 0;
        long lastLow = -1;
        for (long s = faultSec; s <= windowEnd; s++) {
            long v = perSec.getOrDefault(s, 0L);
            dip = Math.min(dip, v);
            if (v == 0) zeroSecs++;
            double avg3 = (v + perSec.getOrDefault(s + 1, 0L) + perSec.getOrDefault(s + 2, 0L)) / 3.0;
            if (avg3 < 0.5 * baseline) lastLow = s;
        }
        r.put("Baseline throughput before fault (ledger rows/sec)", String.format("%,d", baseline));
        r.put("Lowest throughput after fault", windowEnd < faultSec ? "-" : String.format("%,d rows/sec", dip));
        r.put("Seconds with zero throughput", zeroSecs);
        r.put("Recovered after", (lastLow < 0 ? 0 : lastLow + 1 - faultSec) + " s (throughput back above 50% of baseline and stayed there)");
        return r;
    }

    /** Sums per-second deltas of every timeline file of a stage (one file per process, so restarts are counted too). */
    private static TreeMap<Long, Long> timeline(File dir, String stage) throws Exception {
        TreeMap<Long, Long> perSec = new TreeMap<>();
        File[] files = dir.listFiles((d, n) -> n.startsWith(stage) && n.endsWith(".timeline.csv"));
        if (files == null) return perSec;
        for (File f : files) {
            long prev = 0;
            for (String l : Files.readAllLines(f.toPath())) {
                String[] p = l.split(",");
                if (p.length != 2) continue;
                long sec = Long.parseLong(p[0]) / 1000;
                long c = Long.parseLong(p[1]);
                perSec.merge(sec, Math.max(0, c - prev), Long::sum);
                prev = c;
            }
        }
        return perSec;
    }

    private static Agg agg(File dir, String stage) throws Exception {
        Agg a = new Agg();
        File[] files = dir.listFiles((d, n) -> n.endsWith(".json") && (n.equals(stage + ".json") || n.matches(stage + "-\\d+\\.json")));
        if (files == null) return a;
        Arrays.sort(files);
        for (File f : files) {
            JsonNode s = JSON.readTree(f);
            a.instances++;
            a.records += s.path("records").asLong();
            if (s.path("firstMs").asLong() > 0) a.firstMs = Math.min(a.firstMs, s.path("firstMs").asLong());
            a.lastMs = Math.max(a.lastMs, s.path("lastMs").asLong());
            a.peakSum += s.path("peak5sRecordsPerSec").asLong();
            a.duplicates += s.path("duplicatesSkipped").asLong();
            a.ledgerRows = Math.max(a.ledgerRows, s.path("rowsInLedgerTable").asLong());
            if (s.has("decisions")) a.decisions = s.get("decisions");
            String kind = s.has("cacheHitRatio") ? "cache hit ratio " + s.path("cacheHitRatio").asText() + ", "
                : s.has("processingGuarantee") ? s.path("processingGuarantee").asText() + ", " : "";
            a.notes = kind + s.path("threads").asText() + " threads per instance";
            if (s.has("latencyHistogram")) {
                Histogram h = Histogram.decodeFromCompressedByteBuffer(
                    ByteBuffer.wrap(Base64.getDecoder().decode(s.path("latencyHistogram").asText())), 0);
                if (a.latency == null) a.latency = h;
                else a.latency.add(h);
            }
        }
        if (a.firstMs == Long.MAX_VALUE) a.firstMs = 0;
        return a;
    }

    private static void stage(StringBuilder md, String label, Agg a) {
        md.append(String.format("| %s | %d | %,d | %,d | %,d | %s |%n", label, a.instances, a.records, a.avg(), a.peakSum, a.notes));
    }

    private static void summary(Cfg cfg) throws Exception {
        File f = new File(cfg.resultsDir(), "summary.jsonl");
        List<JsonNode> rows = new ArrayList<>();
        for (String l : Files.readAllLines(f.toPath())) if (!l.isBlank()) rows.add(JSON.readTree(l));
        StringBuilder md = new StringBuilder("# All runs\n\n");
        md.append("| Run | Kafka | Instances per stage | Rows / expected | End-to-end tx/sec | Enricher / Scorer / Ledger avg | p50 ms | p99 ms | Hottest partition | Duplicates absorbed | Fault: lowest rate / recovery |\n");
        md.append("|---|---|---|---|---|---|---|---|---|---|---|\n");
        for (JsonNode r : rows) {
            String faultCol = r.has("fault") ? r.path("fault").asText() + ": " + r.path("faultDip").asText() + " / " + r.path("faultRecovery").asText() : "-";
            md.append(String.format("| %s | %s | %d | %,d / %,d | **%,d** | %,d / %,d / %,d | %d | %d | %.2fx | %,d | %s |%n",
                r.path("name").asText(), r.path("mode").asText("1 broker, RF 1"), Math.max(1, r.path("instances").asInt()),
                r.path("records").asLong(), r.path("expected").asLong(r.path("records").asLong()), r.path("pipelineTxPerSec").asLong(),
                r.path("enricherPerSec").asLong(), r.path("scorerPerSec").asLong(), r.path("ledgerPerSec").asLong(),
                r.path("p50ms").asLong(), r.path("p99ms").asLong(), r.path("hottestPartitionVsAverage").asDouble(),
                r.path("duplicates").asLong(), faultCol));
        }
        Files.writeString(new File(cfg.resultsDir(), "SUMMARY.md").toPath(), md.toString());
        System.out.print(md);
    }

    private static JsonNode read(File dir, String name) throws Exception {
        File f = new File(dir, name + ".json");
        return f.exists() ? JSON.readTree(f) : JSON.missingNode();
    }
}
