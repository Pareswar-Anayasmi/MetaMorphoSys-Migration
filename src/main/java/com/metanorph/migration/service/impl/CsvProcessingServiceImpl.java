package com.metanorph.migration.service.impl;

import com.metanorph.migration.config.TableMappingConfiguration;
import com.metanorph.migration.service.CsvProcessingService;
import com.metanorph.migration.util.CsvReaderUtil;
import com.metanorph.migration.util.ExcelWriterUtil;
import com.metanorph.migration.util.HeaderResolverUtil;
import com.metanorph.migration.util.IdentifierUtil;

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
 * and generating Excel output based on YAML configuration.
 *
 * Supports:
 * - Table hierarchy
 * - Identifier based deduplication
 * - Parent reference propagation
 * - Empty row skipping
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CsvProcessingServiceImpl implements CsvProcessingService {

    private final TableMappingConfiguration tableMappingConfiguration;

    /**
     * Entry method invoked by controller.
     */
    @Override
    public Workbook processCsv(final Reader reader) {

        log.info("Starting CSV processing");

        final CSVParser csvParser = createCsvParser(reader);

        final Map<String, List<Map<String, String>>> tableData = initializeTableStructure();

        final Map<String, Map<String, String>> headerMappings = resolveHeaderMappings(csvParser);

        final Map<String, Set<String>> identifierCache = initializeIdentifierCache();

        processCsvRecords(csvParser, tableData, headerMappings, identifierCache);

        log.info("CSV processing completed successfully");

        return ExcelWriterUtil.write(tableData);
    }

    /**
     * Create CSV parser.
     */
    private CSVParser createCsvParser(final Reader reader) {

        try {
            log.debug("Parsing CSV input");
            return CsvReaderUtil.parse(reader);

        } catch (IOException exception) {

            log.error("Failed to parse CSV input", exception);

            throw new IllegalStateException( "CSV parsing failed. Please verify file format.", exception);
        }
    }

    /**
     * Initialize output table containers.
     */
    private Map<String, List<Map<String, String>>> initializeTableStructure() {

        Map<String, List<Map<String, String>>> tableData = new LinkedHashMap<>();

        if (tableMappingConfiguration.getTables() == null) {

            throw new IllegalStateException( "table-mapping.tables configuration missing in YAML");
        }

        tableMappingConfiguration.getTables()
                .forEach((tableName, definition) -> {

                    log.debug("Initializing structure for table {}", tableName);
                    tableData.put(tableName, new ArrayList<>());
                });

        return tableData;
    }

    /**
     * Resolve CSV headers using configured YAML identifiers.
     */
    private Map<String, Map<String, String>> resolveHeaderMappings(
            final CSVParser csvParser) {

        Map<String, Map<String, String>> mappings = new HashMap<>();

        Set<String> csvHeaders = csvParser.getHeaderMap().keySet();

        tableMappingConfiguration.getTables().forEach((tableName, definition) -> {

                    Map<String, String> resolvedHeaders =HeaderResolverUtil.resolve(csvHeaders, definition);
                    mappings.put(tableName, resolvedHeaders);
                });

        return mappings;
    }

    /**
     * Cache used for identifier based deduplication.
     */
    private Map<String, Set<String>> initializeIdentifierCache() {

        Map<String, Set<String>> cache = new HashMap<>();

        tableMappingConfiguration.getTables()
                .keySet()
                .forEach(table -> cache.put(table, new HashSet<>()));

        return cache;
    }

    /**
     * Iterate CSV rows.
     */
    private void processCsvRecords(
            final CSVParser csvParser,
            final Map<String, List<Map<String, String>>> tableData,
            final Map<String, Map<String, String>> headerMappings,
            final Map<String, Set<String>> identifierCache) {

        int rowNumber = 0;

        for (CSVRecord recordData : csvParser) {

            rowNumber++;

            log.debug("Processing CSV row {}", rowNumber);

            processSingleRecord(recordData, tableData, headerMappings, identifierCache);
        }

        log.info("Total CSV rows processed: {}", rowNumber);
    }

    /**
     * Process a single CSV record.
     */
    private void processSingleRecord(
            final CSVRecord recordData,
            final Map<String, List<Map<String, String>>> tableData,
            final Map<String, Map<String, String>> headerMappings,
            final Map<String, Set<String>> identifierCache) {

        tableMappingConfiguration.getTables()
                .forEach((tableName, tableDefinition) -> {

                    Map<String, String> headerMap = headerMappings.get(tableName);

                    String identifierKey = IdentifierUtil.buildIdentifier(
                            recordData,
                            tableDefinition.getIdentifier(),
                            headerMap);

                    if (identifierKey != null && identifierCache.get(tableName).contains(identifierKey)) {
                        log.debug("Duplicate row skipped for table {}", tableName);
                        return;
                    }

//                    Map<String, String> rowData =
//                            buildRowData(recordData, tableDefinition, headerMap);
                    Map<String, String> rowData = buildRowData(recordData, tableDefinition, headerMap);

                    if (rowData == null) {
                        return;
                    }


                    if (shouldSkipRow(rowData, tableDefinition)) {
                        log.debug("Skipping empty row for table {}", tableName);
                        return;
                    }

                    tableData.get(tableName).add(rowData);

                    if (identifierKey != null) {
                        identifierCache.get(tableName).add(identifierKey);
                    }
                });
    }

    /**
     * Build row data for a specific table.
     * Also inject parent reference column if configured.
     */
//    private Map<String, String> buildRowData(
//            final CSVRecord recordData,
//            final TableMappingConfiguration.TableDefinition tableDefinition,
//            final Map<String, String> headerMap) {
//
//        Map<String, String> rowData = new LinkedHashMap<>();
//
//        // Populate configured columns
//        tableDefinition.getColumns()
//                .forEach((columnName, columnDefinition) -> {
//
//                    String header = headerMap.get(columnName);
//
//                    String value = "";
//
//                    if (header != null) {
//                        value = recordData.get(header);
//                    }
//
//                    rowData.put(columnName, value == null ? "" : value.trim());
//                });
//
//        // Inject parent reference if configured
//        if (tableDefinition.getParentReference() != null) {
//
//            String parentColumn = tableDefinition.getParentReference();
//
//            String header = headerMap.get(parentColumn);
//
//            if (header != null) {
//                rowData.put(parentColumn, recordData.get(header));
//            }
//        }
//
//        return rowData;
//    }

    private Map<String, String> buildRowData(
            final CSVRecord recordData,
            final TableMappingConfiguration.TableDefinition tableDefinition,
            final Map<String, String> headerMap) {

        Map<String, String> rowData = new LinkedHashMap<>();

    /* --------------------------------------------------
       1️⃣ Inject parent reference column first
    -------------------------------------------------- */
//
//        if (tableDefinition.getParentReference() != null) {
//
//            String parentColumn = tableDefinition.getParentReference();
//
//            try {
//
//                String value = recordData.get(parentColumn);
//
//                if (value != null && !value.isBlank()) {
//                    rowData.put(parentColumn, value.trim());
//                }
//
//            } catch (IllegalArgumentException ex) {
//
//                log.warn( "Parent reference column '{}' not found in CSV", parentColumn );
//            }
//        }

        if (tableDefinition.getParentReference() != null) {

            String parentColumn = tableDefinition.getParentReference();

            String parentValue = "";

            try {
                parentValue = recordData.get(parentColumn);
            } catch (Exception ignored) {}

            if (parentValue == null || parentValue.isBlank()) {

                log.debug("Skipping row because parent reference '{}' is missing",
                        parentColumn);

                return null;
            }

            rowData.put(parentColumn, parentValue.trim());
        }

    /* --------------------------------------------------
       2️⃣ Populate normal configured columns
    -------------------------------------------------- */

        tableDefinition.getColumns()
                .forEach((columnName, columnDefinition) -> {

                    String header = headerMap.get(columnName);

                    String value = "";

                    if (header != null) {
                        value = recordData.get(header);
                    }

                    rowData.put(columnName, value == null ? "" : value.trim());
                });

        return rowData;
    }

    /**
     * Skip row if configured and all columns empty.
     */
    private boolean shouldSkipRow(
            final Map<String, String> rowData,
            final TableMappingConfiguration.TableDefinition tableDefinition) {

        if (!Boolean.TRUE.equals(tableDefinition.getSkipIfEmpty())) {
            return false;
        }

        return rowData.values()
                .stream()
                .allMatch(value -> value == null || value.isEmpty());
    }
}