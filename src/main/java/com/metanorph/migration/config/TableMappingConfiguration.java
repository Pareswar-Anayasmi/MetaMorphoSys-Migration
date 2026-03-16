package com.metanorph.migration.config;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.util.List;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "table-mapping")
public class TableMappingConfiguration {

    private Map<String, TableDefinition> tables;

    @Data
    public static class TableDefinition {

        /** Column name where a generated GUID will be stored for this table's row. */
        private String guidColumn;

        /** Parent table name in the hierarchy. */
        private String parent;

        /**
         * FK column name to inject into this table's row,
         * carrying the parent table's generated GUID.
         */
        private String parentGuidRef;

        /**
         * FK column name to inject into this table's row,
         * carrying the root (top-level) table's generated GUID.
         * Used by benefit to link back to the primary client.
         */
        private String rootGuidRef;

        /**
         * CSV column prefix used to identify columns that belong to this table.
         * E.g. "nominee_" means only CSV headers starting with "nominee_" are read.
         */
        private String sourcePrefix;

        /** Child table names. */
        private List<String> children;

        private Boolean skipIfEmpty = false;

        private Map<String, ColumnDefinition> columns;

        /**
         * Optional physical output table/sheet name.
         * When absent, logical key name is used.
         */
        private String tableName;
    }

    @Data
    public static class ColumnDefinition {

        private List<String> identifiers;
    }
}