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
 * Implementation responsible for converting CSV input
 * into an Excel workbook based on YAML table configuration.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CsvProcessingServiceImpl implements CsvProcessingService {

    private final TableMappingConfiguration tableMappingConfiguration;

    @Override
    public Workbook processCsv(final Reader reader) {

        log.info("CSV processing started");

        final CSVParser csvParser = createCsvParser(reader);

        final Map<String, List<Map<String, String>>> tableData = initializeTableStructure();

        final Map<String, Map<String, String>> headerMappings = resolveHeaderMappings(csvParser);

        processRecords(csvParser, tableData, headerMappings);

        log.info("CSV processing completed successfully");

        return ExcelWriterUtil.write(tableData);
    }

    /**
     * Creates CSV parser.
     */
    private CSVParser createCsvParser(final Reader reader) {

        try {
            return CsvReaderUtil.parse(reader);
        } catch (IOException exception) {
            log.error("Unable to parse CSV input", exception);
            throw new IllegalStateException("CSV parsing failed", exception);
        }
    }

    /**
     * Initializes table structure for output Excel sheets.
     */
    private Map<String, List<Map<String, String>>> initializeTableStructure() {

        Map<String, List<Map<String, String>>> tables = new LinkedHashMap<>();

        for (String tableName : tableMappingConfiguration.getTables().keySet()) {
            tables.put(tableName, new ArrayList<>());
        }

        return tables;
    }

    /**
     * Resolves header mappings between CSV headers and YAML identifiers.
     */
    private Map<String, Map<String, String>> resolveHeaderMappings(
            final CSVParser csvParser) {

        Map<String, Map<String, String>> mappings = new HashMap<>();

        for (Map.Entry<String, TableMappingConfiguration.TableDefinition> entry
                : tableMappingConfiguration.getTables().entrySet()) {

            String tableName = entry.getKey();
            TableMappingConfiguration.TableDefinition tableDefinition = entry.getValue();

            Map<String, String> resolvedHeaders =
                    HeaderResolverUtil.resolve(csvParser.getHeaderMap().keySet(), tableDefinition);

            mappings.put(tableName, resolvedHeaders);
        }

        return mappings;
    }

    /**
     * Processes CSV rows and populates table data.
     */
    private void processRecords(
            final CSVParser csvParser,
            final Map<String, List<Map<String, String>>> tableData,
            final Map<String, Map<String, String>> headerMappings) {

        for (CSVRecord csvRecord : csvParser) {

            final String mapId = UUID.randomUUID().toString();

            for (Map.Entry<String, TableMappingConfiguration.TableDefinition> tableEntry
                    : tableMappingConfiguration.getTables().entrySet()) {

                final String tableName = tableEntry.getKey();
                final TableMappingConfiguration.TableDefinition tableDefinition =
                        tableEntry.getValue();

                final Map<String, String> headerMap = headerMappings.get(tableName);

                Map<String, String> rowData = buildRowData(
                        mapId,
                        csvRecord,
                        tableDefinition,
                        headerMap);

                if (containsData(rowData)) {
                    tableData.get(tableName).add(rowData);
                }
            }
        }
    }

    /**
     * Builds row data for a specific table.
     */
    private Map<String, String> buildRowData(
            final String mapId,
            final CSVRecord csvRecord,
            final TableMappingConfiguration.TableDefinition tableDefinition,
            final Map<String, String> headerMap) {

        Map<String, String> rowData = new LinkedHashMap<>();

        rowData.put("mapId", mapId);

        for (String columnName : tableDefinition.getColumns().keySet()) {

            String header = headerMap.get(columnName);

            String value = "";

            if (header != null) {
                value = csvRecord.get(header);
            }

            value = value == null ? "" : value.trim();

            rowData.put(columnName, value);
        }

        return rowData;
    }

    /**
     * Checks if row contains actual data excluding mapId.
     */
    private boolean containsData(final Map<String, String> rowData) {

        for (Map.Entry<String, String> entry : rowData.entrySet()) {

            if (!"mapId".equals(entry.getKey())
                    && entry.getValue() != null
                    && !entry.getValue().isEmpty()) {

                return true;
            }
        }

        return false;
    }
}