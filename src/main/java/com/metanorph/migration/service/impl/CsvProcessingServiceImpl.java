package com.metanorph.migration.service.impl;

import com.metanorph.migration.config.TableMappingConfiguration;
import com.metanorph.migration.config.TableMappingConfiguration.TableDefinition;
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
 * Processes a flat CSV file and produces a multi-sheet Excel workbook.
 *
 * <p>For every CSV row the following records are always created (no deduplication):
 * <ol>
 *   <li>client       – new GUID generated as {@code client_guid}</li>
 *   <li>address      – {@code client_guid} injected as FK, new GUID as {@code address_guid}</li>
 *   <li>contact      – {@code address_guid} injected as FK</li>
 *   <li>nomineeClient – created only when nominee columns are present; new GUID as {@code nominee_client_guid}</li>
 *   <li>benefit      – {@code nominee_client_guid} + {@code client_guid} injected as FKs</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CsvProcessingServiceImpl implements CsvProcessingService {

    private final TableMappingConfiguration tableMappingConfiguration;

    // ── Entry point ───────────────────────────────────────────────────────────

    @Override
    public Workbook processCsv(final Reader reader) {

        log.info("Starting CSV processing");

        final CSVParser csvParser = createCsvParser(reader);
        final Map<String, List<Map<String, String>>> tableData = initializeTableStructure();
        final Map<String, Map<String, String>> headerMappings = resolveHeaderMappings(csvParser);

        int rowNumber = 0;
        for (CSVRecord csvRecord : csvParser) {
            rowNumber++;
            log.debug("Processing CSV row {}", rowNumber);
            processSingleRecord(csvRecord, tableData, headerMappings);
        }

        log.info("Total CSV rows processed: {}", rowNumber);
        return ExcelWriterUtil.write(tableData);
    }

    // ── Per-row processing ────────────────────────────────────────────────────

    /**
     * Processes one CSV row across all configured tables.
     * A fresh {@code guidContext} is created per row so GUIDs never leak between rows.
     */
    private void processSingleRecord(
            final CSVRecord csvRecord,
            final Map<String, List<Map<String, String>>> tableData,
            final Map<String, Map<String, String>> headerMappings) {

        final Map<String, String> guidContext = new HashMap<>();

        tableMappingConfiguration.getTables().forEach((tableName, tableDef) ->
                processTable(tableName, tableDef, csvRecord, headerMappings, tableData, guidContext));
    }

    /**
     * Builds, optionally assigns a GUID to, and stores one row for a single table.
     */
    private void processTable(
            final String tableName,
            final TableDefinition tableDef,
            final CSVRecord csvRecord,
            final Map<String, Map<String, String>> headerMappings,
            final Map<String, List<Map<String, String>>> tableData,
            final Map<String, String> guidContext) {

        final String currentTableGuid = prepareGuidForTable(tableName, tableDef, guidContext);
        final Map<String, String> headerMap = headerMappings.get(tableName);
        Map<String, String> rowData = buildRowData(csvRecord, tableDef, headerMap, currentTableGuid, guidContext);

        if (rowData == null) {
            log.debug("Row skipped for table '{}' - parent GUID missing", tableName);
            return;
        }

        if (shouldSkipRow(rowData, tableDef)) {
            log.debug("Skipping empty row for table '{}'", tableName);
            discardPreparedGuidIfUnused(tableName, currentTableGuid, guidContext);
            return;
        }

        rowData = assignGuidIfRequired(tableName, tableDef, rowData, guidContext);

        final String outputTableName = resolveOutputTableName(tableName, tableDef);
        tableData.get(outputTableName).add(rowData);
    }

    // ── Row builder ───────────────────────────────────────────────────────────

    /**
     * Assembles a row map for the given table.
     * Returns {@code null} when a required FK GUID is not yet available (parent was skipped).
     */
    private Map<String, String> buildRowData(
            final CSVRecord csvRecord,
            final TableDefinition tableDef,
            final Map<String, String> headerMap,
            final String currentTableGuid,
            final Map<String, String> guidContext) {

        // Validate parent / root GUIDs exist before building the row
        if (!validateParentGuidPresent(tableDef, guidContext)) {
            return null;
        }
        if (!validateRootGuidPresent(tableDef, guidContext)) {
            return null;
        }

        final Map<String, String> rowData = new LinkedHashMap<>();
        populateColumns(csvRecord, tableDef, headerMap, currentTableGuid, rowData, guidContext);
        return rowData;
    }

    /**
     * Validates the parent table's GUID exists in context (parent row was not skipped).
     * Does NOT write anything to rowData – placement is handled inside populateColumns via column definitions.
     */
    private boolean validateParentGuidPresent(
            final TableDefinition tableDef,
            final Map<String, String> guidContext) {

        if (tableDef.getParent() == null || tableDef.getParentGuidRef() == null) {
            return true;
        }

        final String parentGuid = guidContext.get(tableDef.getParent());

        if (parentGuid == null || parentGuid.isBlank()) {
            log.debug("Parent '{}' has no GUID in context – child row will be skipped", tableDef.getParent());
            return false;
        }

        return true;
    }

    /**
     * Validates the root table's GUID exists in context.
     * Does NOT write anything to rowData – placement is handled inside populateColumns via column definitions.
     */
    private boolean validateRootGuidPresent(
            final TableDefinition tableDef,
            final Map<String, String> guidContext) {

        if (tableDef.getRootGuidRef() == null) {
            return true;
        }

        final String rootGuid = guidContext.get(tableDef.getRootTable() != null ? tableDef.getRootTable() : "client");

        if (rootGuid == null || rootGuid.isBlank()) {
            log.debug("Root GUID missing in context – row will be skipped");
            return false;
        }

        return true;
    }

    /**
     * Reads each configured column value from the CSV record and stores it in {@code rowData}.
     * FK columns (parentGuidRef / rootGuidRef) are resolved from {@code guidContext}
     * when a column's identifier matches those sentinel keys.
     */
    private void populateColumns(
            final CSVRecord csvRecord,
            final TableDefinition tableDef,
            final Map<String, String> headerMap,
            final String currentTableGuid,
            final Map<String, String> rowData,
            final Map<String, String> guidContext) {

        if (tableDef.getColumns() == null) {
            return;
        }

        tableDef.getColumns().forEach((columnName, columnDef) -> {

            // 1. FK column whose identifier matches parentGuidRef → pull parent's GUID from context
            if (isParentGuidRefColumn(tableDef, columnDef)) {
                final String parentGuid = guidContext.get(tableDef.getParent());
                rowData.put(columnName, parentGuid != null ? parentGuid : "");
                return;
            }

            // 2. FK column whose identifier matches rootGuidRef → pull root table's GUID from context
            if (isRootGuidRefColumn(tableDef, columnDef)) {
                final String rootTable = tableDef.getRootTable() != null ? tableDef.getRootTable() : "client";
                final String rootGuid = guidContext.get(rootTable);
                rowData.put(columnName, rootGuid != null ? rootGuid : "");
                return;
            }

            // 3. Column whose identifier matches the table's own guidColumn → use generated GUID
            if (isGuidIdentifierColumn(tableDef, columnDef, currentTableGuid)) {
                rowData.put(columnName, currentTableGuid != null ? currentTableGuid : "");
                return;
            }

            // 4. Normal CSV-mapped column
            final String header = headerMap != null ? headerMap.get(columnName) : null;
            if (header != null) {
                final String value = csvRecord.get(header);
                rowData.put(columnName, value == null ? "" : value.trim());
                return;
            }

            // 5. No mapping found – write empty value
            rowData.put(columnName, "");
        });
    }

    /**
     * Returns {@code true} when ALL of the column's identifiers equal the configured {@code parentGuidRef}.
     * Specifically: any identifier that exactly matches tableDef.getParentGuidRef().
     */
    private boolean isParentGuidRefColumn(
            final TableDefinition tableDef,
            final TableMappingConfiguration.ColumnDefinition columnDef) {

        if (tableDef.getParent() == null || tableDef.getParentGuidRef() == null) {
            return false;
        }
        if (columnDef == null || columnDef.getIdentifiers() == null) {
            return false;
        }
        return columnDef.getIdentifiers().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .anyMatch(id -> id.equalsIgnoreCase(tableDef.getParentGuidRef()));
    }

    /**
     * Returns {@code true} when any of the column's identifiers equal the configured {@code rootGuidRef}.
     */
    private boolean isRootGuidRefColumn(
            final TableDefinition tableDef,
            final TableMappingConfiguration.ColumnDefinition columnDef) {

        if (tableDef.getRootGuidRef() == null) {
            return false;
        }
        if (columnDef == null || columnDef.getIdentifiers() == null) {
            return false;
        }
        return columnDef.getIdentifiers().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .anyMatch(id -> id.equalsIgnoreCase(tableDef.getRootGuidRef()));
    }

    private boolean isGuidIdentifierColumn(
            final TableDefinition tableDef,
            final TableMappingConfiguration.ColumnDefinition columnDef,
            final String currentTableGuid) {

        if (currentTableGuid == null || tableDef.getGuidColumn() == null || columnDef == null || columnDef.getIdentifiers() == null) {
            return false;
        }

        return columnDef.getIdentifiers().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .anyMatch(identifier -> identifier.equalsIgnoreCase(tableDef.getGuidColumn()));
    }

    private String prepareGuidForTable(
            final String tableName,
            final TableDefinition tableDef,
            final Map<String, String> guidContext) {

        if (tableDef.getGuidColumn() == null) {
            return null;
        }

        return guidContext.computeIfAbsent(tableName, ignored -> UUID.randomUUID().toString());
    }

    private void discardPreparedGuidIfUnused(
            final String tableName,
            final String preparedGuid,
            final Map<String, String> guidContext) {

        if (preparedGuid == null) {
            return;
        }

        final String current = guidContext.get(tableName);
        if (preparedGuid.equals(current)) {
            guidContext.remove(tableName);
        }
    }

    /**
     * Generates a UUID for tables that declare a {@code guidColumn},
     * prepends it to the row map, and stores it in {@code guidContext}.
     */
    private Map<String, String> assignGuidIfRequired(
            final String tableName,
            final TableDefinition tableDef,
            final Map<String, String> rowData,
            final Map<String, String> guidContext) {

        if (tableDef.getGuidColumn() == null) {
            return rowData;
        }

        final String guid = guidContext.computeIfAbsent(tableName, ignored -> UUID.randomUUID().toString());
        final Map<String, String> ordered = new LinkedHashMap<>();
        ordered.put(tableDef.getGuidColumn(), guid);
        ordered.putAll(rowData);

        log.debug("Generated GUID {} for table '{}'", guid, tableName);
        return ordered;
    }

    // ── Skip-row evaluation ───────────────────────────────────────────────────

    /**
     * Returns {@code true} when {@code skipIfEmpty = true} is configured and
     * all non-FK column values are blank.
     */
    private boolean shouldSkipRow(
            final Map<String, String> rowData,
            final TableDefinition tableDef) {

        if (!Boolean.TRUE.equals(tableDef.getSkipIfEmpty())) {
            return false;
        }

        final Set<String> fkColumns = buildFkColumnSet(tableDef);

        return rowData.entrySet().stream()
                .filter(entry -> !fkColumns.contains(entry.getKey()))
                .allMatch(entry -> entry.getValue() == null || entry.getValue().isEmpty());
    }

    private Set<String> buildFkColumnSet(final TableDefinition tableDef) {
        final Set<String> fkColumns = new HashSet<>();
        if (tableDef.getColumns() == null) {
            return fkColumns;
        }
        tableDef.getColumns().forEach((columnName, columnDef) -> {
            if (isParentGuidRefColumn(tableDef, columnDef) || isRootGuidRefColumn(tableDef, columnDef)) {
                fkColumns.add(columnName);
            }
        });
        return fkColumns;
    }

    // ── Infrastructure helpers ────────────────────────────────────────────────

    private CSVParser createCsvParser(final Reader reader) {
        try {
            return CsvReaderUtil.parse(reader);
        } catch (IOException ex) {
            log.error("Failed to parse CSV input", ex);
            throw new IllegalStateException("CSV parsing failed. Please verify the file format.", ex);
        }
    }

    private Map<String, List<Map<String, String>>> initializeTableStructure() {

        if (tableMappingConfiguration.getTables() == null) {
            throw new IllegalStateException("table-mapping.tables configuration is missing in YAML.");
        }

        final Map<String, List<Map<String, String>>> tableData = new LinkedHashMap<>();

        tableMappingConfiguration.getTables().forEach((logicalName, def) -> {
            final String outputTableName = resolveOutputTableName(logicalName, def);
            tableData.putIfAbsent(outputTableName, new ArrayList<>());
        });

        return tableData;
    }

    private String resolveOutputTableName(final String logicalTableName, final TableDefinition tableDef) {
        if (tableDef.getTableName() == null || tableDef.getTableName().isBlank()) {
            return logicalTableName;
        }
        return tableDef.getTableName().trim();
    }

    private Map<String, Map<String, String>> resolveHeaderMappings(final CSVParser csvParser) {

        final Map<String, Map<String, String>> mappings = new HashMap<>();
        final Set<String> csvHeaders = csvParser.getHeaderMap().keySet();

        tableMappingConfiguration.getTables().forEach((tableName, def) ->
                mappings.put(tableName, HeaderResolverUtil.resolve(csvHeaders, def)));

        return mappings;
    }
}
