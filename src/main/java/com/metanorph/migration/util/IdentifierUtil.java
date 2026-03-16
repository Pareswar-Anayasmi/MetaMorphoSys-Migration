package com.metanorph.migration.util;

/**
 * Previously used for identifier-based deduplication.
 * Deduplication has been removed; every CSV row always produces fresh records.
 * Class retained to avoid breaking any future references.
 */
public final class IdentifierUtil {

    private IdentifierUtil() {
        throw new UnsupportedOperationException("Utility class");
    }
}
