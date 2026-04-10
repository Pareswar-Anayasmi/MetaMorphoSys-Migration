package com.metanorph.migration.util;

import com.metanorph.migration.config.TableMappingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Resolves CSV header names to the logical column names configured in the YAML table definition.
 */
public final class HeaderResolverUtil {

    private static final Logger log = LoggerFactory.getLogger(HeaderResolverUtil.class);

    private HeaderResolverUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Returns a map of {@code logicalColumnName → matchedCsvHeader} for the given table definition.
     * Matching is case-insensitive.
     *
     * @param csvHeaders      the set of header names present in the uploaded CSV
     * @param tableDefinition the YAML-configured table definition containing expected identifiers
     * @return resolved header mapping; empty map when no columns are configured
     */
    public static Map<String, String> resolve(
            final Iterable<String> csvHeaders,
            final TableMappingConfiguration.TableDefinition tableDefinition) {

        final Map<String, String> mapping = new HashMap<>();
        if (!hasConfiguredColumns(tableDefinition)) {
            return mapping;
        }

        final Map<String, String> normalizedHeaderLookup = buildNormalizedHeaderLookup(csvHeaders);
        final Map<String, List<String>> headerToColumns = new HashMap<>();

        resolveConfiguredColumns(
                tableDefinition.getColumns(), normalizedHeaderLookup, mapping, headerToColumns);

        logDuplicateSourceHeaderUsage(headerToColumns);
        return mapping;
    }

    private static boolean hasConfiguredColumns(final TableMappingConfiguration.TableDefinition tableDefinition) {
        return tableDefinition != null
                && tableDefinition.getColumns() != null
                && !tableDefinition.getColumns().isEmpty();
    }

    private static void resolveConfiguredColumns(
            final Map<String, TableMappingConfiguration.ColumnDefinition> configuredColumns,
            final Map<String, String> normalizedHeaderLookup,
            final Map<String, String> mapping,
            final Map<String, List<String>> headerToColumns) {

        configuredColumns.forEach((columnName, columnDefinition) ->
                resolveColumn(columnName, columnDefinition, normalizedHeaderLookup, mapping, headerToColumns));
    }

    private static void resolveColumn(
            final String columnName,
            final TableMappingConfiguration.ColumnDefinition columnDefinition,
            final Map<String, String> normalizedHeaderLookup,
            final Map<String, String> mapping,
            final Map<String, List<String>> headerToColumns) {

        if (columnDefinition == null) {
            log.debug("No configuration found for logical column '{}'.", columnName);
            return;
        }

        if (columnDefinition.getLiteral() != null && !columnDefinition.getLiteral().isBlank()) {
            log.debug("Logical column '{}' is configured with a literal value.", columnName);
            return;
        }

        if (columnDefinition.getIdentifiers() == null || columnDefinition.getIdentifiers().isEmpty()) {
            log.debug("No identifiers configured for logical column '{}'.", columnName);
            return;
        }

        final String matchedHeader = findMatchedHeader(columnDefinition, normalizedHeaderLookup, columnName);
        if (matchedHeader == null) {
            log.warn("No source header matched identifiers {} for logical column '{}'.", columnDefinition.getIdentifiers(), columnName);
            return;
        }

        final String previous = mapping.putIfAbsent(columnName, matchedHeader);
        if (previous != null && !previous.equals(matchedHeader)) {
            log.warn("Duplicate header mapping for column '{}': '{}' and '{}'. Keeping '{}'.", columnName, previous, matchedHeader, previous);
        }
        headerToColumns.computeIfAbsent(matchedHeader, ignored -> new ArrayList<>()).add(columnName);
    }

    private static String findMatchedHeader(
            final TableMappingConfiguration.ColumnDefinition columnDefinition,
            final Map<String, String> normalizedHeaderLookup,
            final String columnName) {

        return columnDefinition.getIdentifiers().stream()
                .map(identifier -> normalizeIdentifier(identifier, columnName))
                .filter(normalized -> !normalized.isEmpty())
                .map(normalizedHeaderLookup::get)
                .filter(matched -> matched != null && !matched.isBlank())
                .findFirst()
                .orElse(null);
    }

    private static String normalizeIdentifier(final String identifier, final String columnName) {

        final String normalizedIdentifier = normalize(identifier);
        if (normalizedIdentifier.isEmpty()) {
            log.debug("Skipping blank identifier for logical column '{}'.", columnName);
        }
        return normalizedIdentifier;
    }

    private static void logDuplicateSourceHeaderUsage(final Map<String, List<String>> headerToColumns) {

        headerToColumns.forEach((header, columns) -> {
            if (columns.size() > 1) {
                log.warn("Source header '{}' is mapped to multiple logical columns {}.", header, columns);
            }
        });
    }

    private static Map<String, String> buildNormalizedHeaderLookup(final Iterable<String> headers) {

        final Map<String, String> normalizedToHeader = new LinkedHashMap<>();
        for (String header : headers) {
            final String normalizedHeader = normalize(header);
            if (normalizedHeader.isEmpty()) {
                continue;
            }

            final String previous = normalizedToHeader.putIfAbsent(normalizedHeader, header);
            if (previous != null && !previous.equals(header)) {
                log.warn("Duplicate source header normalization '{}': '{}' and '{}'. Keeping '{}'.", normalizedHeader, previous, header, previous);
            }
        }
        return normalizedToHeader;
    }

    private static String normalize(final String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}