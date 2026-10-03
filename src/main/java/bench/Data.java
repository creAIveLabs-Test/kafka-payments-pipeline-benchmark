package bench;

import java.util.SplittableRandom;

/** Synthetic reference data. Attributes are derived from the id, so every stage agrees without sharing state. */
final class Data {
    static final String[] TIERS = {"BASIC", "PLUS", "PREMIUM", "CORPORATE"};
    static final String[] CATEGORIES = {"GROCERY", "RESTAURANT", "FUEL", "TRAVEL", "ELECTRONICS",
        "APPAREL", "PHARMACY", "SUBSCRIPTION", "UTILITIES", "GAMBLING"};
    static final String[] COUNTRIES = {"US", "US", "US", "US", "CA", "GB", "IN", "DE", "MX", "BR"};

    private Data() {
    }

    static String accountId(int i) {
        return String.format("acct-%07d", i);
    }

    static String merchantId(int i) {
        return String.format("mer-%06d", i);
    }

    static String tier(int i) {
        int h = Math.floorMod(mix(i), 100);
        return h < 60 ? "BASIC" : h < 85 ? "PLUS" : h < 97 ? "PREMIUM" : "CORPORATE";
    }

    static boolean riskFlag(int i) {
        return Math.floorMod(mix(i * 31 + 7), 100) < 2;
    }

    static String accountCountry(int i) {
        return COUNTRIES[Math.floorMod(mix(i * 17 + 3), COUNTRIES.length)];
    }

    static String category(int m) {
        int h = Math.floorMod(mix(m * 13 + 1), 100);
        return h < 2 ? "GAMBLING" : CATEGORIES[h % (CATEGORIES.length - 1)];
    }

    static String merchantName(int m) {
        return "Merchant " + m;
    }

    static String merchantCountry(int m) {
        return COUNTRIES[Math.floorMod(mix(m * 7 + 5), COUNTRIES.length)];
    }

    /** Power-law pick: low ids are chosen far more often, like real customers and merchants. */
    static int skewed(SplittableRandom rnd, int n, double power) {
        return (int) (n * Math.pow(rnd.nextDouble(), power));
    }

    private static int mix(int x) {
        x ^= x >>> 16;
        x *= 0x7feb352d;
        x ^= x >>> 15;
        x *= 0x846ca68b;
        x ^= x >>> 16;
        return x;
    }
}
