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

/**
 * Loads reference data into Postgres (source of truth) and warms Redis with a share of it,
 * so enrichment sees a realistic mix of cache hits and misses.
 */
final class Seed {
    private Seed() {
    }

    static void run(Cfg cfg) throws Exception {
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
}
