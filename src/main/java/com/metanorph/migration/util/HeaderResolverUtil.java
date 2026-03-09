package com.metanorph.migration.util;

import com.metanorph.migration.config.TableMappingConfiguration;

import java.util.HashMap;
import java.util.Map;

public final class HeaderResolverUtil {

    private HeaderResolverUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static Map<String, String> resolve(
            Iterable<String> csvHeaders,
            TableMappingConfiguration.TableDefinition tableDefinition) {

        Map<String, String> mapping = new HashMap<>();

        tableDefinition.getColumns().forEach((columnName, columnDefinition) -> {

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