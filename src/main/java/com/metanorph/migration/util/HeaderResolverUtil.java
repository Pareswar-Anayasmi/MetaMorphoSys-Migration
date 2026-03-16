package com.metanorph.migration.util;

import com.metanorph.migration.config.TableMappingConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Resolves CSV header names to the logical column names configured in the YAML table definition.
 */
public final class HeaderResolverUtil {

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

        if (tableDefinition.getColumns() == null) {
            return mapping;
        }

        tableDefinition.getColumns().forEach((columnName, columnDefinition) -> {

            if (columnDefinition.getIdentifiers() == null) {
                return;
            }

            columnDefinition.getIdentifiers().forEach(identifier -> {
                for (String header : csvHeaders) {
                    if (header.equalsIgnoreCase(identifier)) {

                        mapping.put(columnName, header);
                    }
                }
            });
        });

        return mapping;
    }
}