package com.metanorph.migration.service.impl;

import com.metanorph.migration.config.TableMappingConfiguration;
import com.metanorph.migration.service.CsvProcessingService;
import com.metanorph.migration.util.CsvReaderUtil;
import com.metanorph.migration.util.ExcelWriterUtil;
import com.metanorph.migration.util.HeaderResolverUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * Service responsible for processing CSV input
 * and generating Excel output.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CsvProcessingServiceImpl implements CsvProcessingService {

    private final TableMappingConfiguration tableMappingConfiguration;

    @Override
    public Workbook processCsv(final Reader reader) {

        log.info("Starting CSV processing");

        final CSVParser csvParser;

        try {
            csvParser = CsvReaderUtil.parse(reader);
        } catch (IOException exception) {
            log.error("Error while parsing CSV file", exception);
            throw new IllegalStateException("Unable to parse CSV input", exception);
        }

        final Map<String, List<Map<String, String>>> tableData = new LinkedHashMap<>();

        /*
         * Initialize table structure
         */
        tableMappingConfiguration.getTables()
                .forEach((tableName, tableDefinition) ->
                        tableData.put(tableName, new ArrayList<>()));

        /*
         * Resolve CSV header mappings
         */
        final Map<String, Map<String, String>> headerMappings = new HashMap<>();

        tableMappingConfiguration.getTables()
                .forEach((tableName, tableDefinition) -> {

                    Map<String, String> resolvedHeaders =
                            HeaderResolverUtil.resolve(
                                    csvParser.getHeaderMap().keySet(),
                                    tableDefinition);

                    headerMappings.put(tableName, resolvedHeaders);
                });

        int mapId = 1;

        /*
         * Process each CSV row
         */
        for (CSVRecord csvRecord : csvParser) {

            for (Map.Entry<String, TableMappingConfiguration.TableDefinition> entry
                    : tableMappingConfiguration.getTables().entrySet()) {

                String tableName = entry.getKey();
                TableMappingConfiguration.TableDefinition tableDefinition = entry.getValue();

                Map<String, String> rowData = new LinkedHashMap<>();

                rowData.put("mapId", String.valueOf(mapId));

                Map<String, String> headerMap = headerMappings.get(tableName);

                if (headerMap == null) {
                    continue;
                }

                tableDefinition.getColumns()
                        .forEach((columnName, columnDefinition) -> {

                            String header = headerMap.get(columnName);

                            String value = "";

                            if (header != null) {
                                value = csvRecord.get(header);
                            }

                            rowData.put(columnName, value);
                        });

                tableData.get(tableName).add(rowData);
            }

            mapId++;
        }

        log.info("CSV processing completed. Generating Excel workbook");

        return ExcelWriterUtil.write(tableData);
    }
}