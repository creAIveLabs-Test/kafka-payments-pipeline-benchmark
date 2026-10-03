package bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * report --dir=results/runs/<name>   -> <dir>/RESULTS.md and one line in results/summary.jsonl
 * report --summary                   -> results/SUMMARY.md comparing every run
 */
final class Report {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Report() {
    }

    static void run(Cfg cfg) throws Exception {
        if (!"false".equals(cfg.str("summary", "false"))) {
            summary(cfg);
            return;
        }
        File dir = new File(cfg.str("dir", cfg.resultsDir()));
        String name = cfg.str("name", dir.getName());
        JsonNode gen = read(dir, "generator");
        JsonNode enr = read(dir, "enricher");
        JsonNode sco = read(dir, "scorer");
        JsonNode led = read(dir, "ledger");
        JsonNode raw = read(dir, "raw-kafka");
        JsonNode env = read(dir, "run");

        long records = led.path("records").asLong();
        long pipelineMs = Math.max(1, led.path("lastMs").asLong() - gen.path("startMs").asLong());
        long pipelineRate = Math.round(records * 1000.0 / pipelineMs);

        StringBuilder md = new StringBuilder();
        md.append("# Run: ").append(name).append("\n\n");
        if (!env.isMissingNode()) {
            md.append("| Setting | Value |\n|---|---|\n");
            env.fields().forEachRemaining(e -> md.append("| ").append(e.getKey()).append(" | ").append(e.getValue().asText()).append(" |\n"));
            md.append('\n');
        }
        md.append("## End-to-end pipeline\n\n");
        md.append(String.format("- **%,d transactions** from generator start to the last ledger commit in **%.1f s** = **%,d transactions/sec end to end**%n",
            records, pipelineMs / 1000.0, pipelineRate));
        JsonNode lat = led.path("endToEndLatencyMs");
        if (!lat.isMissingNode()) {
            md.append(String.format("- End-to-end latency (created to committed in Postgres): p50 %d ms, p95 %d ms, p99 %d ms, max %d ms%n",
                lat.path("p50").asLong(), lat.path("p95").asLong(), lat.path("p99").asLong(), lat.path("max").asLong()));
        }
        md.append(String.format("- Ledger rows: %,d (duplicates skipped by ON CONFLICT: %,d); decisions: %s%n%n",
            led.path("rowsInLedgerTable").asLong(), led.path("duplicatesSkipped").asLong(), led.path("decisions")));

        md.append("## Per stage\n\n| Stage | Records | Avg records/sec | Peak 5 s records/sec | Notes |\n|---|---|---|---|---|\n");
        stage(md, "Generator (produce)", gen, "hottest partition = " + gen.path("hottestPartitionVsAverage").asText() + "x average");
        stage(md, "Enricher (Redis + Postgres)", enr, "cache hit ratio " + enr.path("cacheHitRatio").asText() + ", " + enr.path("threads").asText() + " threads");
        stage(md, "Scorer (Kafka Streams + RocksDB)", sco, sco.path("processingGuarantee").asText() + ", " + sco.path("threads").asText() + " threads");
        stage(md, "Ledger (Postgres batch insert)", led, led.path("threads").asText() + " threads");
        md.append('\n');

        md.append("Records per partition (generator): ").append(gen.path("recordsPerPartition")).append("\n\n");

        if (!raw.isMissingNode()) {
            md.append("## Raw Kafka (LinkedIn-style producer/consumer perf test)\n\n| Test | Records/sec | MB/sec | Avg latency ms | p99 latency ms |\n|---|---|---|---|---|\n");
            for (JsonNode t : raw.path("tests")) {
                md.append(String.format("| %s | %,d | %.1f | %s | %s |%n", t.path("name").asText(), t.path("recordsPerSec").asLong(),
                    t.path("mbPerSec").asDouble(), t.path("avgLatencyMs").asText("-"), t.path("p99LatencyMs").asText("-")));
            }
            md.append('\n');
        }
        Files.writeString(new File(dir, "RESULTS.md").toPath(), md.toString());

        ObjectNode line = JSON.createObjectNode();
        line.put("name", name);
        line.put("records", records);
        line.put("pipelineSeconds", Math.round(pipelineMs / 100.0) / 10.0);
        line.put("pipelineTxPerSec", pipelineRate);
        line.put("generatorPerSec", gen.path("avgRecordsPerSec").asLong());
        line.put("enricherPerSec", enr.path("avgRecordsPerSec").asLong());
        line.put("enricherPeakPerSec", enr.path("peak5sRecordsPerSec").asLong());
        line.put("scorerPerSec", sco.path("avgRecordsPerSec").asLong());
        line.put("scorerPeakPerSec", sco.path("peak5sRecordsPerSec").asLong());
        line.put("ledgerPerSec", led.path("avgRecordsPerSec").asLong());
        line.put("ledgerPeakPerSec", led.path("peak5sRecordsPerSec").asLong());
        line.put("p50ms", lat.path("p50").asLong());
        line.put("p99ms", lat.path("p99").asLong());
        line.put("hottestPartitionVsAverage", gen.path("hottestPartitionVsAverage").asDouble());
        if (!env.isMissingNode()) line.set("settings", env);
        File summary = new File(cfg.resultsDir(), "summary.jsonl");
        Files.writeString(summary.toPath(), JSON.writeValueAsString(line) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.print(md);
    }

    private static void summary(Cfg cfg) throws Exception {
        File f = new File(cfg.resultsDir(), "summary.jsonl");
        List<JsonNode> rows = new ArrayList<>();
        for (String l : Files.readAllLines(f.toPath())) if (!l.isBlank()) rows.add(JSON.readTree(l));
        StringBuilder md = new StringBuilder("# All runs\n\n");
        md.append("| Run | Records | End-to-end tx/sec | Enricher avg / peak | Scorer avg / peak | Ledger avg / peak | p50 ms | p99 ms | Hottest partition |\n");
        md.append("|---|---|---|---|---|---|---|---|---|\n");
        for (JsonNode r : rows) {
            md.append(String.format("| %s | %,d | **%,d** | %,d / %,d | %,d / %,d | %,d / %,d | %d | %d | %.2fx |%n",
                r.path("name").asText(), r.path("records").asLong(), r.path("pipelineTxPerSec").asLong(),
                r.path("enricherPerSec").asLong(), r.path("enricherPeakPerSec").asLong(),
                r.path("scorerPerSec").asLong(), r.path("scorerPeakPerSec").asLong(),
                r.path("ledgerPerSec").asLong(), r.path("ledgerPeakPerSec").asLong(),
                r.path("p50ms").asLong(), r.path("p99ms").asLong(), r.path("hottestPartitionVsAverage").asDouble()));
        }
        Files.writeString(new File(cfg.resultsDir(), "SUMMARY.md").toPath(), md.toString());
        System.out.print(md);
    }

    private static void stage(StringBuilder md, String label, JsonNode s, String notes) {
        md.append(String.format("| %s | %,d | %,d | %,d | %s |%n", label, s.path("records").asLong(),
            s.path("avgRecordsPerSec").asLong(), s.path("peak5sRecordsPerSec").asLong(), notes));
    }

    private static JsonNode read(File dir, String name) throws Exception {
        File f = new File(dir, name + ".json");
        return f.exists() ? JSON.readTree(f) : JSON.missingNode();
    }
}
