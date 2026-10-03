package bench;

import java.util.Arrays;

/** Entry point: java -jar bench.jar <command> [--key=value ...] */
public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        Cfg cfg = Cfg.parse(Arrays.copyOfRange(args, 1, args.length));
        switch (args[0]) {
            case "seed" -> Seed.run(cfg);
            case "generate" -> Generator.run(cfg);
            case "enrich" -> Enricher.run(cfg);
            case "score" -> Scorer.run(cfg);
            case "ledger" -> Ledger.run(cfg);
            case "report" -> Report.run(cfg);
            default -> usage();
        }
    }

    private static void usage() {
        System.out.println("""
            commands:
              seed      load accounts/merchants into Postgres and warm Redis
              generate  produce N synthetic transactions to Kafka
              enrich    consume transactions, enrich from Redis (cache-aside on Postgres)
              score     Kafka Streams: velocity state in RocksDB + rules -> decisions
              ledger    consume decisions, batch-insert idempotently into Postgres
              report    build results/RESULTS.md from results/*.json
            """);
    }
}
