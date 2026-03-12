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

        private List<String> identifier;

        private String parent;    //is table has

        private String parentReference;  //parent table reference

        private List<String> children;   //is table has child

        private Boolean skipIfEmpty = false;

        private Map<String, ColumnDefinition> columns;
    }

    @Data
    public static class ColumnDefinition {

        private List<String> identifiers;
    }
}