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

        private Map<String, ColumnDefinition> columns;
    }

    @Data
    public static class ColumnDefinition {

        private List<String> identifiers;
    }
}