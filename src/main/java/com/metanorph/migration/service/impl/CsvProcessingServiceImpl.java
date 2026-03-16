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

        final Map<String, String> headerMap = headerMappings.get(tableName);
        Map<String, String> rowData = buildRowData(csvRecord, tableDef, headerMap, guidContext);

        if (rowData == null) {
            log.debug("Row skipped for table '{}' – parent GUID missing", tableName);
            return;
        }

        if (shouldSkipRow(rowData, tableDef)) {
            log.debug("Skipping empty row for table '{}'", tableName);
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
            final Map<String, String> guidContext) {

        final Map<String, String> rowData = new LinkedHashMap<>();

        if (!injectParentGuidRef(tableDef, guidContext, rowData)) {
            return null;
        }

        if (!injectRootGuidRef(tableDef, guidContext, rowData)) {
            return null;
        }

        populateColumns(csvRecord, tableDef, headerMap, rowData);
        return rowData;
    }

    /**
     * Injects the parent table's GUID as a FK column.
     * Returns {@code false} when the parent GUID is absent (parent row was skipped).
     */
    private boolean injectParentGuidRef(
            final TableDefinition tableDef,
            final Map<String, String> guidContext,
            final Map<String, String> rowData) {

        if (tableDef.getParent() == null || tableDef.getParentGuidRef() == null) {
            return true;
        }

        final String parentGuid = guidContext.get(tableDef.getParent());

        if (parentGuid == null || parentGuid.isBlank()) {
            log.debug("Parent '{}' has no GUID in context – child row will be skipped", tableDef.getParent());
            return false;
        }

        rowData.put(tableDef.getParentGuidRef(), parentGuid);
        return true;
    }

    /**
     * Injects the root client GUID as a FK column (used by the benefit table).
     * Returns {@code false} when the root GUID is absent.
     */
    private boolean injectRootGuidRef(
            final TableDefinition tableDef,
            final Map<String, String> guidContext,
            final Map<String, String> rowData) {

        if (tableDef.getRootGuidRef() == null) {
            return true;
        }

        final String rootGuid = guidContext.get("client");

        if (rootGuid == null || rootGuid.isBlank()) {
            log.debug("Root client GUID missing in context – row will be skipped");
            return false;
        }

        rowData.put(tableDef.getRootGuidRef(), rootGuid);
        return true;
    }

    /**
     * Reads each configured column value from the CSV record and stores it in {@code rowData}.
     */
    private void populateColumns(
            final CSVRecord csvRecord,
            final TableDefinition tableDef,
            final Map<String, String> headerMap,
            final Map<String, String> rowData) {

        if (tableDef.getColumns() == null) {
            return;
        }

        tableDef.getColumns().forEach((columnName, columnDef) -> {
            final String header = headerMap != null ? headerMap.get(columnName) : null;
            final String value  = header != null ? csvRecord.get(header) : "";
            rowData.put(columnName, value == null ? "" : value.trim());
        });
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

        final String guid = UUID.randomUUID().toString();
        final Map<String, String> ordered = new LinkedHashMap<>();
        ordered.put(tableDef.getGuidColumn(), guid);
        ordered.putAll(rowData);
        guidContext.put(tableName, guid);

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
        if (tableDef.getParentGuidRef() != null) fkColumns.add(tableDef.getParentGuidRef());
        if (tableDef.getRootGuidRef()   != null) fkColumns.add(tableDef.getRootGuidRef());
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

