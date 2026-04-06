package com.metanorph.migration.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "migration.db")
public class DbSourceProperties {

    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String sourceTable;
    private Integer queryTimeoutSeconds = 60;
    private Integer fetchSize = 500;
}

