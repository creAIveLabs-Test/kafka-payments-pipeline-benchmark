package bench;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads reference data into Postgres (source of truth) and warms Redis with a share of it,
 * so enrichment sees a realistic mix of cache hits and misses.
 */
final class Seed {
    private Seed() {
    }

    static void run(Cfg cfg) throws Exception {
        if ("ibm".equals(cfg.str("source", "synthetic"))) {
            runIbm(cfg);
            return;
        }
        int accounts = cfg.integer("accounts", 500_000);
        int merchants = cfg.integer("merchants", 20_000);
        double warmShare = cfg.dbl("warm-share", 0.8);

        try (Connection c = DriverManager.getConnection(cfg.jdbcUrl())) {
            long existing;
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM accounts")) {
                rs.next();
                existing = rs.getLong(1);
            }
            if (existing == accounts) {
                System.out.printf("postgres already has %,d accounts, skipping load%n", existing);
            } else {
                try (Statement s = c.createStatement()) {
                    s.execute("TRUNCATE accounts, merchants");
                }
                CopyManager copy = c.unwrap(PGConnection.class).getCopyAPI();
                StringBuilder sb = new StringBuilder(accounts * 40);
                for (int i = 0; i < accounts; i++) {
                    sb.append(Data.accountId(i)).append(',').append(Data.tier(i)).append(',')
                        .append(Data.riskFlag(i)).append(',').append(Data.accountCountry(i)).append('\n');
                }
                copy.copyIn("COPY accounts FROM STDIN WITH (FORMAT csv)", new StringReader(sb.toString()));
                sb.setLength(0);
                for (int m = 0; m < merchants; m++) {
                    sb.append(Data.merchantId(m)).append(',').append(Data.merchantName(m)).append(',')
                        .append(Data.category(m)).append(',').append(Data.merchantCountry(m)).append('\n');
                }
                copy.copyIn("COPY merchants FROM STDIN WITH (FORMAT csv)", new StringReader(sb.toString()));
                try (Statement s = c.createStatement()) {
                    s.execute("ANALYZE accounts");
                    s.execute("ANALYZE merchants");
                }
                System.out.printf("postgres: loaded %,d accounts and %,d merchants%n", accounts, merchants);
            }
            try (Statement s = c.createStatement()) {
                s.execute("TRUNCATE ledger_entries");
            }
        }

        int warmAccounts = (int) (accounts * warmShare);
        try (Jedis j = new Jedis(cfg.redisHost(), cfg.redisPort())) {
            j.flushAll();
            Pipeline p = j.pipelined();
            for (int i = 0; i < warmAccounts; i++) {
                p.setex("acct:" + Data.accountId(i), 3600, Enricher.accountValue(Data.tier(i), Data.riskFlag(i), Data.accountCountry(i)));
                if (i % 10_000 == 0) p.sync();
            }
            for (int m = 0; m < merchants; m++) {
                p.setex("mer:" + Data.merchantId(m), 3600, Enricher.merchantValue(Data.merchantName(m), Data.category(m), Data.merchantCountry(m)));
            }
            p.sync();
        }
        System.out.printf("redis: warmed %,d accounts (%.0f%%) and %,d merchants%n", warmAccounts, warmShare * 100, merchants);
    }

    /**
     * IBM TabFormer: accounts and merchants are the ones that appear in the rows being replayed.
     * Merchant category and country come from the file (MCC, Merchant State). The file has no account tier or
     * risk flag, so those stay derived from the id exactly like the synthetic data (stated in docs/IBM.md).
     */
    static void runIbm(Cfg cfg) throws Exception {
        String csv = cfg.str("csv", "");
        long count = cfg.lng("count", 0);
        double warmShare = cfg.dbl("warm-share", 0.8);
        Map<String, Integer> accounts = new LinkedHashMap<>();
        Map<String, String[]> merchants = new LinkedHashMap<>();
        long rows = Ibm.forEach(csv, count, r -> {
            accounts.putIfAbsent(r.accountId(), accounts.size());
            merchants.putIfAbsent(r.merchantId(), new String[] {"Merchant " + r.merchantId().substring(5), r.category(), r.merchantCountry()});
        });

        try (Connection c = DriverManager.getConnection(cfg.jdbcUrl())) {
            try (Statement s = c.createStatement()) {
                s.execute("TRUNCATE accounts, merchants, ledger_entries");
            }
            CopyManager copy = c.unwrap(PGConnection.class).getCopyAPI();
            StringBuilder sb = new StringBuilder(accounts.size() * 40);
            for (Map.Entry<String, Integer> a : accounts.entrySet()) {
                int i = a.getValue();
                sb.append(a.getKey()).append(',').append(Data.tier(i)).append(',').append(Data.riskFlag(i)).append(",US\n");
            }
            copy.copyIn("COPY accounts FROM STDIN WITH (FORMAT csv)", new StringReader(sb.toString()));
            sb.setLength(0);
            for (Map.Entry<String, String[]> m : merchants.entrySet()) {
                String[] v = m.getValue();
                sb.append(m.getKey()).append(',').append(v[0]).append(',').append(v[1]).append(",\"").append(v[2]).append("\"\n");
            }
            copy.copyIn("COPY merchants FROM STDIN WITH (FORMAT csv)", new StringReader(sb.toString()));
            try (Statement s = c.createStatement()) {
                s.execute("ANALYZE accounts");
                s.execute("ANALYZE merchants");
            }
        }
        System.out.printf("postgres: loaded %,d accounts and %,d merchants from %,d IBM rows%n", accounts.size(), merchants.size(), rows);

        int warmAccounts = (int) (accounts.size() * warmShare);
        try (Jedis j = new Jedis(cfg.redisHost(), cfg.redisPort())) {
            j.flushAll();
            Pipeline p = j.pipelined();
            for (Map.Entry<String, Integer> a : accounts.entrySet()) {
                int i = a.getValue();
                if (i >= warmAccounts) break;
                p.setex("acct:" + a.getKey(), 3600, Enricher.accountValue(Data.tier(i), Data.riskFlag(i), "US"));
            }
            int k = 0;
            for (Map.Entry<String, String[]> m : merchants.entrySet()) {
                String[] v = m.getValue();
                p.setex("mer:" + m.getKey(), 3600, Enricher.merchantValue(v[0], v[1], v[2]));
                if (++k % 10_000 == 0) p.sync();
            }
            p.sync();
        }
        System.out.printf("redis: warmed %,d accounts (%.0f%%) and %,d merchants%n", warmAccounts, warmShare * 100, merchants.size());
    }
}
