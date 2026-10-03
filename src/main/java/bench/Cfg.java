package bench;

import java.util.HashMap;
import java.util.Map;

/** --key=value arguments with environment-variable and default fallbacks. */
public final class Cfg {
    private final Map<String, String> values;

    private Cfg(Map<String, String> values) {
        this.values = values;
    }

    static Cfg parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--") && a.contains("=")) {
                int i = a.indexOf('=');
                m.put(a.substring(2, i), a.substring(i + 1));
            }
        }
        return new Cfg(m);
    }

    String str(String key, String def) {
        String v = values.get(key);
        if (v != null) return v;
        String env = System.getenv(key.toUpperCase().replace('-', '_').replace('.', '_'));
        return env != null ? env : def;
    }

    int integer(String key, int def) {
        return Integer.parseInt(str(key, String.valueOf(def)));
    }

    long lng(String key, long def) {
        return Long.parseLong(str(key, String.valueOf(def)));
    }

    double dbl(String key, double def) {
        return Double.parseDouble(str(key, String.valueOf(def)));
    }

    String bootstrap() {
        return str("bootstrap", "localhost:9092");
    }

    String runId() {
        return str("run-id", "local");
    }

    String resultsDir() {
        return str("results", "results");
    }

    /** Created by the ledger when every expected row is committed; all stages stop when it exists. */
    java.io.File doneFile() {
        return new java.io.File(resultsDir(), "DONE");
    }

    /** Safety net only: stop if nothing at all happens for this long. */
    long safetyIdleMs() {
        return lng("safety-idle-ms", 600_000);
    }

    String jdbcUrl() {
        return str("jdbc", "jdbc:postgresql://localhost:5432/payments?user=bench&password=bench&reWriteBatchedInserts=true");
    }

    String redisHost() {
        return str("redis-host", "localhost");
    }

    int redisPort() {
        return integer("redis-port", 6379);
    }
}
