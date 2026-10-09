package bench;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Reads the IBM TabFormer credit-card file (card_transaction.v1.csv, 24,386,900 rows) row by row.
 *
 * Columns: User,Card,Year,Month,Day,Time,Amount,Use Chip,Merchant Name,Merchant City,Merchant State,Zip,MCC,Errors?,Is Fraud?
 *
 * Mapping to the pipeline: one account per (User, Card), one merchant per Merchant Name, category from the real MCC,
 * merchant country from Merchant State (US state code or empty for online = US, a country name = that country).
 * The fraud label is read here but never put on Kafka; only scripts/ibm_eval.py uses it, after the run.
 */
final class Ibm {
    /** tx_id = UUID(IBM_MSB, row index): deterministic, so a replayed row always gets the same id. */
    static final long IBM_MSB = 0x1B4D_0000_0000_0001L;

    record Row(long index, String accountId, String merchantId, String category, String merchantCountry,
               long amountCents, long eventMs, String channel, String errors) {
    }

    interface RowFn {
        void accept(Row r) throws Exception;
    }

    private Ibm() {
    }

    static String txId(long index) {
        return new UUID(IBM_MSB, index).toString();
    }

    /** Calls fn for the first limit data rows (limit <= 0 means every row). Returns rows read. */
    static long forEach(String csv, long limit, RowFn fn) throws Exception {
        long n = 0;
        try (BufferedReader in = Files.newBufferedReader(Path.of(csv), StandardCharsets.UTF_8)) {
            String header = in.readLine();
            if (header == null || !header.startsWith("User,Card,Year")) {
                throw new IOException(csv + " does not look like card_transaction.v1.csv (header: " + header + ")");
            }
            String line;
            while ((limit <= 0 || n < limit) && (line = in.readLine()) != null) {
                fn.accept(parse(n, line));
                n++;
            }
        }
        return n;
    }

    static Row parse(long index, String line) {
        List<String> f = split(line);
        String account = "ibm-u" + f.get(0) + "-c" + f.get(1);
        String merchant = "ibm-m" + f.get(8);
        String time = f.get(5);
        long eventMs = LocalDateTime.of(Integer.parseInt(f.get(2)), Integer.parseInt(f.get(3)), Integer.parseInt(f.get(4)),
            Integer.parseInt(time.substring(0, 2)), Integer.parseInt(time.substring(3, 5))).toEpochSecond(ZoneOffset.UTC) * 1000;
        long cents = Math.round(Double.parseDouble(f.get(6).replace("$", "")) * 100);
        String state = f.get(10);
        String country = state.isEmpty() || state.length() == 2 ? "US" : state.toUpperCase();   // online rows have no state: domestic
        String errors = f.get(13);
        if (errors.endsWith(",")) errors = errors.substring(0, errors.length() - 1);
        return new Row(index, account, merchant, category(Integer.parseInt(f.get(12))), country, cents, eventMs, f.get(7), errors);
    }

    /** Merchant Category Code (ISO 18245) to the pipeline's categories. */
    static String category(int mcc) {
        if (mcc >= 3000 && mcc <= 3999) return "TRAVEL";          // airlines, car rental, hotels
        if (mcc == 7995 || mcc == 7800 || mcc == 7801 || mcc == 7802) return "GAMBLING";
        if (mcc >= 5411 && mcc <= 5499 || mcc == 5300 || mcc == 5310 || mcc == 5311) return "GROCERY";
        if (mcc >= 5811 && mcc <= 5814) return "RESTAURANT";
        if (mcc == 5541 || mcc == 5542) return "FUEL";
        if (mcc == 4111 || mcc == 4112 || mcc == 4121 || mcc == 4131 || mcc == 4411 || mcc == 4511
            || mcc == 4722 || mcc == 4784 || mcc == 7011) return "TRAVEL";
        if (mcc == 5045 || mcc == 5732 || mcc == 5733 || mcc == 5734) return "ELECTRONICS";
        if (mcc >= 5600 && mcc <= 5699) return "APPAREL";
        if (mcc == 5912) return "PHARMACY";
        if (mcc == 4814 || mcc == 4899 || mcc == 5815 || mcc == 5816 || mcc == 5968) return "SUBSCRIPTION";
        if (mcc == 4900) return "UTILITIES";
        if (mcc == 4829 || mcc >= 6010 && mcc <= 6012 || mcc == 6051) return "MONEY_TRANSFER";
        return "OTHER";
    }

    /** CSV split with double-quote support (the Errors? column is quoted, e.g. "Bad PIN,"). */
    static List<String> split(String line) {
        List<String> out = new ArrayList<>(15);
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (ch == ',' && !quoted) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString());
        return out;
    }
}
