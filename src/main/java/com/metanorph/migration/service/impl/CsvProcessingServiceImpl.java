package com.metanorph.migration.service.impl;

import com.metanorph.migration.config.DbSourceProperties;
import com.metanorph.migration.config.TableMappingConfiguration;
import com.metanorph.migration.config.TableMappingConfiguration.TableDefinition;
import com.metanorph.migration.contstants.ClientConstants;
import com.metanorph.migration.service.CsvProcessingService;
import com.metanorph.migration.util.CsvReaderUtil;
import com.metanorph.migration.util.ExcelInputToCsvUtil;
import com.metanorph.migration.util.ExcelWriterUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.stream.Collectors;

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
    private final DbSourceProperties dbSourceProperties;
     private long clientNumCounter = 1;
     private static final boolean FILTER_ONLY_NON_ADMIT = true;
     private static final String ADMIT_PREFIX = "ADMIT_";
     private final Map<String, String> statusByClaimGuid = new HashMap<>();

    /**
     * Processes uploaded file (CSV/Excel) and converts it into Excel output.
     */
    @Override
    public Workbook processFile(final String fileName, final InputStream inputStream) {
        if (isBlank(fileName)) {
            throw new IllegalArgumentException("File name is required.");
        }
        try {
            if (isExcelFile(fileName)) {
                final String csvContent = ExcelInputToCsvUtil.convertFirstSheetToCsv(inputStream);
                return processCsv(new StringReader(csvContent));
            }
            try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                return processCsv(reader);
            }
        } catch (IOException ex) {
            log.error("Failed to process input file: {}", fileName, ex);
            throw new IllegalStateException("Input processing failed. Please verify the file format.", ex);
        }
    }

    /**
     * Processes uploaded file (CSV/Excel) and converts it into Excel output.
     */
    @Override
    public Workbook processConfiguredTable() {
        final String jdbcUrl = requireNonBlank(dbSourceProperties.getUrl(), "migration.db.url is required.");
        final String sourceTable = validateSqlIdentifier(requireNonBlank(dbSourceProperties.getSourceTable(), "migration.db.source-table is required."));
        final String sql = "SELECT * FROM " + sourceTable;
        try {
            loadDriverIfConfigured();
            try (Connection connection = DriverManager.getConnection(
                    jdbcUrl,
                    emptyIfNull(dbSourceProperties.getUsername()),
                    emptyIfNull(dbSourceProperties.getPassword()));
                 PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setQueryTimeout(safePositive(dbSourceProperties.getQueryTimeoutSeconds(), 60));
                statement.setFetchSize(safePositive(dbSourceProperties.getFetchSize(), 500));
                try (ResultSet resultSet = statement.executeQuery()) {
                    final String csvData = convertResultSetToCsv(resultSet);
                    log.info("Database rows loaded from table '{}' and delegated to CSV pipeline", sourceTable);
                    return processCsv(new StringReader(csvData));
                }
            }
        } catch (SQLException ex) {
            log.error("Database execution failed for table: {}", sourceTable, ex);
            if (isSqlServerDatabaseAccessFailure(ex)) {
                final String databaseName = resolveDatabaseNameFromUrl(jdbcUrl);
                throw new IllegalStateException("Unable to connect to database '" + databaseName + "'. " + "Verify migration.db.url, migration.db.username, and migration.db.password "
                        + "and ensure the login has access to that database.", ex);
            }
            throw new IllegalStateException("Database execution failed. Please verify migration.db configuration.", ex);
        }
    }

    private boolean isExcelFile(final String fileName) {
        final String lowerCaseFileName = fileName.toLowerCase(Locale.ROOT);
        return lowerCaseFileName.endsWith(".xlsx") || lowerCaseFileName.endsWith(".xls");
    }

    /**
     * Reads configured DB table, converts data to CSV, and processes it into Excel.
     */
    public Workbook processCsv(final Reader reader) {
        log.info("Starting CSV processing");
        final Map<String, List<Map<String, String>>> tableData = initializeTableStructure();
        int rowNumber = 0;
        try (CSVParser csvParser = createCsvParser(reader)) {
            for (CSVRecord csvRecord : csvParser) {
                rowNumber++;
                log.debug("Processing CSV row {}", rowNumber);
                processSingleRecord(csvRecord, tableData);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("CSV parsing failed. Please verify the file format.", ex);
        }
        clientNumCounter = 1;
        postProcess(tableData);
        addClaimHistorySettlementSheet(tableData);
        addTaskSheet(tableData);
        log.info("Total CSV rows processed: {}", rowNumber);
        Map<String, List<Map<String, String>>> filteredTableData = new LinkedHashMap<>(tableData);
        List<Map<String, String>> clientRows = filteredTableData.get(ClientConstants.CLIENT_TABLE);
        if (clientRows != null) {
            for (Map<String, String> row : clientRows) {
                row.remove(ClientConstants.CLIENT_REF_GUID);
            }
        }
        Map<String, List<String>> headersBySheet = resolveConfiguredSheetHeaders();
        if (FILTER_ONLY_NON_ADMIT) {
            Map<String, List<String>> admitHeaders = new LinkedHashMap<>();
            headersBySheet.forEach((sheet, headers) -> {
                if (sheet != null && sheet.toUpperCase(Locale.ROOT).startsWith("CLAIM")) {
                    admitHeaders.put(resolveAdmitSheetName(sheet), new ArrayList<>(headers));
                }
            });
            headersBySheet.putAll(admitHeaders);
        }
        List<String> clientHeaders = headersBySheet.get(ClientConstants.CLIENT_TABLE);
        if (clientHeaders != null) {
            clientHeaders.remove(ClientConstants.CLIENT_REF_GUID);
        }
        return ExcelWriterUtil.write(filteredTableData, headersBySheet);
    }

    private void postProcess(Map<String, List<Map<String, String>>> tableData) {
        mapPolicyData(tableData);
        mapClientReferences(tableData);
        normalizeRelationships(tableData);
        normalizeDates(tableData);
    }

    private void mapPolicyData(Map<String, List<Map<String, String>>> tableData) {
        mapMasterPolicyNumToClaimHistoryClient(tableData);
    }

    private void mapClientReferences(Map<String, List<Map<String, String>>> tableData) {
        mapClaimHistoryClientCdToClientGuid(tableData);
        syncAdditionalRowClientRefGuidFromClientCd(tableData);
        mapClaimHistoryClientCdToClientGuid(tableData);
        remapDerivedClientReferencesToClientGuid(tableData);
        replacePaymentClientRefWithClientNum(tableData);
    }

    private void normalizeRelationships(Map<String, List<Map<String, String>>> tableData) {
        normalizeClientRelationshipGuidTo(tableData);
        remapClientRelationshipClientNumToFromClient(tableData);
        removeBlankClientGuidRows(tableData);
    }

    private void normalizeDates(Map<String, List<Map<String, String>>> tableData) {
        normalizeClaimAdditionalFieldPaymentDate(tableData);
        normalizeClaimHistoryDates(tableData);
    }

    private void replacePaymentClientRefWithClientNum(Map<String, List<Map<String, String>>> tableData) {
        List<Map<String, String>> payments = getConfiguredRowsForClaimAndAdmit(tableData, ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE);
        List<Map<String, String>> clients  = getConfiguredRows(tableData, ClientConstants.CLIENT_TABLE);
        if (isEmpty(payments) || isEmpty(clients)) return;
        Map<String, String> clientNumByGuid = toMap(clients, ClientConstants.CLIENT_GUID, ClientConstants.CLIENT_NUM);
        payments.forEach(row -> {
            String ref = row.get(ClientConstants.CLIENT_REF_GUID);
            if (clientNumByGuid.containsKey(ref)) {
                row.put(ClientConstants.CLIENT_REF_GUID, clientNumByGuid.get(ref));
            }
        });
    }

    private void addClaimHistorySettlementSheet(Map<String, List<Map<String, String>>> tableData) {
        processSettlementForSheet(tableData, ClientConstants.CLAIM_HISTORY, ClientConstants.CLAIM_HISTORY_SETTLEMENT);
        //filter
        if (FILTER_ONLY_NON_ADMIT) {
            processSettlementForSheet(tableData,
                    resolveClaimOutputTableName(ClientConstants.CLAIM_HISTORY, true),
                    resolveAdmitSheetName("CLAIM_HISTORY_SETTLEMENT"));
        }
    }

    private void processSettlementForSheet(Map<String, List<Map<String, String>>> tableData, String sourceSheet, String targetSheet) {
        List<Map<String, String>> claimRows = Optional.ofNullable(tableData.get(sourceSheet)).orElse(Collections.emptyList());
        List<Map<String, String>> settlementRows =
                claimRows.stream()
                        .filter(row -> {
                            String amt = row.get(ClientConstants.APPROVED_AMT_CURRENCY);
                            return amt != null && !amt.isBlank() && Double.parseDouble(amt) > 0;
                        })
                        .map(row -> Map.of(
                                "claim_history_settlement_guid", UUID.randomUUID().toString(),
                                "approvedAmtPolCurrency", row.get("approvedAmtPolCurrency"),
                                "claim_history_ref_guid", row.get(ClientConstants.CLAIM_GUID_LINK)
                        ))
                        .map(HashMap::new)
                        .collect(Collectors.toList());
        if (!settlementRows.isEmpty()) {
            tableData.put(targetSheet, settlementRows);
        }
    }

    private void mapMasterPolicyNumToClaimHistoryClient(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> claimHistoryPolicyRows = getConfiguredRows(tableData, ClientConstants.CLAIM_HISTORY_POLICY_TABLE);
        final List<Map<String, String>> claimHistoryClientRows = getConfiguredRows(tableData, ClientConstants.CLAIM_HISTORY_CLIENT_TABLE);
        if (claimHistoryPolicyRows == null || claimHistoryPolicyRows.isEmpty() || claimHistoryClientRows == null || claimHistoryClientRows.isEmpty()) return;
        final Map<String, String> policyNumByClaimHistoryRefGuid = new HashMap<>();
        for (Map<String, String> policyRow : claimHistoryPolicyRows) {
            final String claimHistoryRefGuid = policyRow.get(ClientConstants.CLAIM_HISTORY_REF_GUID_COL);
            final String policyNum = policyRow.get("policyNum");
            if (claimHistoryRefGuid != null && !claimHistoryRefGuid.isBlank() && policyNum != null && !policyNum.isBlank()) {
                policyNumByClaimHistoryRefGuid.putIfAbsent(claimHistoryRefGuid, policyNum);
            }
        }
        for (Map<String, String> clientRow : claimHistoryClientRows) {
            final String claimHistoryRefGuid = clientRow.get(ClientConstants.CLAIM_HISTORY_REF_GUID);
            if (isBlank(claimHistoryRefGuid)) continue;
            final String policyNum = policyNumByClaimHistoryRefGuid.get(claimHistoryRefGuid);
            if (policyNum != null && !policyNum.isBlank()) {
                clientRow.put("masterPolicyNum", policyNum);
            }
        }
    }

    /**
     * For additional (non-Insured) rows in CLAIM_HISTORY_CLIENT, sets clientRefGuid to the CLIENT sheet's client_guid (looked up via the temporary random UUID that
     * was shared at creation time). Also fixes CLAIM_HISTORY_PAYMENT rows whose clientRefGuid still holds that temporary random UUID.
     */
    private void syncAdditionalRowClientRefGuidFromClientCd(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> claimHistoryClientRows = getConfiguredRowsForClaimAndAdmit(tableData, ClientConstants.CLAIM_HISTORY_CLIENT_TABLE);
        if (claimHistoryClientRows.isEmpty()) return;
        final Map<String, String> clientGuidByOldRef = buildClientGuidByOldRef(tableData);
        final Map<String, String> oldToNewClientRefGuid = new HashMap<>();
        for (Map<String, String> row : claimHistoryClientRows) {
            updateAdditionalRowClientRefGuid(row, clientGuidByOldRef, oldToNewClientRefGuid);
        }
        applyOldToNewClientRefGuidToPayments(tableData, oldToNewClientRefGuid);
    }

    private Map<String, String> buildClientGuidByOldRef(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> clientRows = getConfiguredRows(tableData, ClientConstants.CLIENT_TABLE);
        return toMap(clientRows, ClientConstants.CLIENT_REF_GUID, ClientConstants.CLIENT_GUID);
    }

    private void updateAdditionalRowClientRefGuid(final Map<String, String> row, final Map<String, String> clientGuidByOldRef, final Map<String, String> oldToNewClientRefGuid) {
        final String oldClientRefGuid = row.get(ClientConstants.CLIENT_REF_GUID);
        if (isBlank(oldClientRefGuid)) return;
        String newClientRefGuid = clientGuidByOldRef.get(oldClientRefGuid);
        if (isBlank(newClientRefGuid)) return;
        if (!newClientRefGuid.equals(oldClientRefGuid)) {
            oldToNewClientRefGuid.put(oldClientRefGuid, newClientRefGuid);
        }
        row.put(ClientConstants.CLIENT_REF_GUID, newClientRefGuid);
    }

    private void applyOldToNewClientRefGuidToPayments(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> oldToNewClientRefGuid) {
        if (oldToNewClientRefGuid.isEmpty()) return;
        final List<Map<String, String>> paymentRows = getConfiguredRowsForClaimAndAdmit(tableData, ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE);
        for (Map<String, String> paymentRow : paymentRows) {
            final String old = paymentRow.get(ClientConstants.CLIENT_REF_GUID);
            final String updated = oldToNewClientRefGuid.get(old);
            if (updated != null) {
                paymentRow.put(ClientConstants.CLIENT_REF_GUID, updated);
            }
        }
    }

    private void remapDerivedClientReferencesToClientGuid(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> clientRows = getConfiguredRows(tableData, ClientConstants.CLIENT_TABLE);
        if (clientRows == null || clientRows.isEmpty()) return;
        Map<String, String> clientGuidByRef = toMap(clientRows, ClientConstants.CLIENT_REF_GUID, ClientConstants.CLIENT_GUID);
        remapClientReferenceColumn(tableData, ClientConstants.CLIENT_ADDRESS_TABLE, clientGuidByRef);
        remapClientReferenceColumn(tableData, ClientConstants.CLIENT_PHONE_TABLE, clientGuidByRef);
        remapClientReferenceColumn(tableData, ClientConstants.CLIENT_EMAIL_TABLE, clientGuidByRef);
        remapClientReferenceColumn(tableData, ClientConstants.CLIENT_RELATIONSHIP_TABLE, clientGuidByRef);
        remapClientReferenceColumn(tableData, ClientConstants.CLIENT_ADD_FLD_TABLE, clientGuidByRef);
        remapClientReferenceColumn(tableData, ClientConstants.CLAIM_ADDITIONAL_FIELD_TABLE, clientGuidByRef);
        remapClientReferenceColumn(tableData, ClientConstants.PERSON_TABLE, clientGuidByRef);
    }

    private void mapClaimHistoryClientCdToClientGuid(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> clientRows = getConfiguredRows(tableData, ClientConstants.CLIENT_TABLE);
        final List<Map<String, String>> claimHistoryClientRows = getConfiguredRowsForClaimAndAdmit(tableData, ClientConstants.CLAIM_HISTORY_CLIENT_TABLE);
        if (clientRows == null || clientRows.isEmpty() || claimHistoryClientRows.isEmpty()) return;
        final Map<String, String> clientNumByLookupKey = buildClientNumLookup(clientRows);
        applyMappedClientNumToClaimHistoryClients(claimHistoryClientRows, clientNumByLookupKey);
    }

    private Map<String, String> buildClientNumLookup(final List<Map<String, String>> clientRows) {
        final Map<String, String> clientNumByLookupKey = new HashMap<>();
        for (Map<String, String> clientRow : clientRows) {
            final String clientNum = clientRow.get(ClientConstants.CLIENT_NUM);
            if (isBlank(clientNum)) continue;
            putIfNonBlank(clientNumByLookupKey, clientRow.get(ClientConstants.CLIENT_REF_GUID), clientNum);
            putIfNonBlank(clientNumByLookupKey, clientRow.get(ClientConstants.CLIENT_GUID), clientNum);
        }
        return clientNumByLookupKey;
    }

    private void applyMappedClientNumToClaimHistoryClients(final List<Map<String, String>> claimHistoryClientRows, final Map<String, String> clientNumByLookupKey) {
        for (Map<String, String> claimHistoryClientRow : claimHistoryClientRows) {
            final String clientRefGuid = claimHistoryClientRow.get(ClientConstants.CLIENT_REF_GUID);
            if (isBlank(clientRefGuid)) continue;
            final String mappedClientNum = clientNumByLookupKey.get(clientRefGuid);
            if (mappedClientNum != null && !mappedClientNum.isBlank()) {
                claimHistoryClientRow.put(ClientConstants.CLIENT_CD, mappedClientNum);
            }
        }
    }

    private void putIfNonBlank(final Map<String, String> target, final String key, final String value) {
        if (key != null && !key.isBlank() && value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }

    private void remapClientReferenceColumn(final Map<String, List<Map<String, String>>> tableData, final String logicalTableName, final Map<String, String> clientGuidByRef) {
        final List<Map<String, String>> rows = getConfiguredRows(tableData, logicalTableName);
        if (isEmpty(rows)) return;
        for (Map<String, String> row : rows) {
            String currentClientRefGuid = row.get(ClientConstants.CLIENT_GUID_LINK);
            if (isBlank(currentClientRefGuid)) {
                currentClientRefGuid = row.get(ClientConstants.CLIENT_REF_GUID);
            }
            if (isBlank(currentClientRefGuid)) continue;
            final String mappedClientGuid = clientGuidByRef.get(currentClientRefGuid);
            if (mappedClientGuid != null && !mappedClientGuid.isBlank()) {
                row.put(ClientConstants.CLIENT_GUID_LINK, mappedClientGuid);
            } else {
                row.put(ClientConstants.CLIENT_GUID_LINK, currentClientRefGuid);
            }
            row.remove(ClientConstants.CLIENT_REF_GUID);
        }
    }

    /**
     * Processes one CSV row across all configured tables.
     * A fresh {@code guidContext} is created per row so GUIDs never leak between rows.
     */
    private void processSingleRecord(final CSVRecord csvRecord, final Map<String, List<Map<String, String>>> tableData) {
        String status = readCsvValueSafely(csvRecord, ClientConstants.TPCR_CLAIM_DECSN_CD);
        log.info("status {}", status);
        final Map<String, String> guidContext = new HashMap<>();
        guidContext.put("ORIGINAL_STATUS", status);
         //filter
         String filterStatus = readCsvValueSafely(csvRecord, "TPCR_CLAIM_DECSN");
         boolean isAdmit = !(ClientConstants.ADMIT.equalsIgnoreCase(filterStatus.trim()));
         //
        guidContext.put("IS_ADMIT", String.valueOf(isAdmit));
        tableMappingConfiguration.getTables().forEach((tableName, tableDef) -> processTable(tableName, tableDef, csvRecord, tableData, guidContext));
    }

    /**
     * Builds, optionally assigns a GUID to, and stores one row for a single table.
     */
    private void processTable(
            final String tableName, final TableDefinition tableDef,
            final CSVRecord csvRecord,
            final Map<String, List<Map<String, String>>> tableData, final Map<String, String> guidContext) {
        //filter
        final String baseTableName = resolveOutputTableName(tableName, tableDef);
        boolean isAdmit = Boolean.parseBoolean(guidContext.getOrDefault("IS_ADMIT", "false"));
        String outputTableName = getString(baseTableName, isAdmit);
        //
        if (isDerivedRowTable(tableName)) return;
        final String currentTableGuid = prepareGuidForTable(tableName, tableDef, guidContext);
        Map<String, String> rowData = buildRowData(csvRecord, tableDef, currentTableGuid, guidContext);
        if (shouldSkipRow(rowData, tableDef)) {
            log.debug("Skipping empty row for table '{}'", tableName);
            discardPreparedGuidIfUnused(tableName, currentTableGuid, guidContext);
            return;
        }
        rowData = assignGuidIfRequired(tableName, tableDef, rowData, guidContext);
        final boolean isClientTable = isOutputForLogicalTable(outputTableName, ClientConstants.CLIENT_TABLE);
        final boolean isClaimHistoryClientTable = isOutputForLogicalTable(outputTableName, ClientConstants.CLAIM_HISTORY_CLIENT_TABLE);
        final boolean isAdmitFlow = isAdmitOutputTable(outputTableName);
        initializeClientScopedRow(rowData, csvRecord, guidContext, isClientTable, isClaimHistoryClientTable);
        normalizeClaimHistoryCauseOfDeathCd(outputTableName, rowData);
        normalizeClaimHistoryStageAndStatusCd(outputTableName, rowData, csvRecord);
        if (shouldSkipCoverageOrBenefitRow(outputTableName, rowData)) {
            log.debug("Skipping row for table '{}' due to missing required business column", outputTableName);
            discardPreparedGuidIfUnused(tableName, currentTableGuid, guidContext);
            return;
        }
        //filter
        tableData.computeIfAbsent(outputTableName, k -> new ArrayList<>());
         //
         tableData.get(outputTableName).add(rowData);
         if (isClaimHistoryTable(outputTableName)) {
             String claimGuid = rowData.get(ClientConstants.CLAIM_GUID_LINK);
             String status = guidContext.get("ORIGINAL_STATUS");
             if (claimGuid != null && status != null) {
                 statusByClaimGuid.put(claimGuid, status);
             }
         }
        if (isClaimHistoryClientTable) {
            appendClaimHistoryClientDerivedRows(tableData, rowData, csvRecord, isAdmitFlow);
        }
        if (isClientTable || isClaimHistoryClientTable) {
            if (isClientTable) {
                appendClientRelationshipRow(tableData, rowData, csvRecord, guidContext);
            }
            handleAdditionalRows(tableData, rowData, tableDef, outputTableName, csvRecord, guidContext);
        }
    }

    private String getString(String baseTableName, boolean isAdmit) {
        Set<String> claimTables = Set.of(
                ClientConstants.CLAIM_HISTORY,
                "CLAIM_HISTORY_CLIENT",
                "CLAIM_HISTORY_POLICY",
                "CLAIM_ADDITIONAL_FIELD",
                "CLAIM_HISTORY_PAYMENT",
                "CLAIM_HISTORY_COVERAGE",
                "CLAIM_HISTORY_COV_BENEFIT",
                "CLAIM_HISTORY_SETTLEMENT"
        );
        String outputTableName = baseTableName;
        final String normalizedBaseTableName = baseTableName == null ? "" : baseTableName.trim().toUpperCase(Locale.ROOT);
        if (FILTER_ONLY_NON_ADMIT && isAdmit && claimTables.contains(normalizedBaseTableName)) {
            outputTableName = resolveAdmitSheetName(baseTableName);
        }
        return outputTableName;
    }

    private void initializeClientScopedRow(final Map<String, String> rowData, final CSVRecord csvRecord, final Map<String, String> guidContext, final boolean isClientTable, final boolean isClaimHistoryClientTable) {
        if (!isClientTable && !isClaimHistoryClientTable) return;
        if (isClaimHistoryClientTable) {
            rowData.put(ClientConstants.ROLE_CD, ClientConstants.INSURED);
            String clientRefGuid = rowData.get(ClientConstants.CLIENT_REF_GUID);
            if (isBlank(clientRefGuid)) {
                clientRefGuid = UUID.randomUUID().toString();
                rowData.put(ClientConstants.CLIENT_REF_GUID, clientRefGuid);
            }
            guidContext.put(ClientConstants.CLAIM_HISTORY_CLIENT_REF_PRIMARY, clientRefGuid);
            guidContext.put("INSURED_CLIENT_REF_GUID", clientRefGuid);
            rowData.put("relatedToInsuredCd", resolveRelatedToInsuredCd(rowData.get(ClientConstants.ROLE_CD), csvRecord));
            registerRelationshipCd(guidContext, clientRefGuid, rowData.get(ClientConstants.ROLE_CD), csvRecord);
        }

        if (isClientTable) {
            String clientGuid = guidContext.get(ClientConstants.CLAIM_HISTORY_CLIENT_REF_PRIMARY);
            if (isBlank(clientGuid)) {
                clientGuid = UUID.randomUUID().toString();
            }
            final String clientNumGuid = "CL-" + clientNumCounter++;
            rowData.put(ClientConstants.CLIENT_GUID, clientGuid);
            rowData.put(ClientConstants.CLIENT_NUM, clientNumGuid);
        }
    }

    private void appendClaimHistoryClientDerivedRows(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> rowData, final CSVRecord csvRecord, final boolean isAdmitFlow) {
        appendClientAddressRow(tableData, rowData, csvRecord);
        appendClientPhoneRow(tableData, rowData, csvRecord);
        appendClientEmailRow(tableData, rowData, csvRecord);
        appendClientAddFldRows(tableData, rowData, csvRecord);
        appendClaimantAddFldRows(tableData, rowData, csvRecord, isAdmitFlow);
        final String roleCd = rowData.get(ClientConstants.ROLE_CD);
        final String personGuid = appendPersonRow(tableData, rowData, csvRecord);
        appendPersonIdentityRows(tableData, personGuid, roleCd, csvRecord);
        appendClaimHistoryPaymentRow(tableData, rowData, csvRecord, isAdmitFlow);
    }

    private void normalizeClientRelationshipGuidTo(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> relationshipRows = getConfiguredRows(tableData, ClientConstants.CLIENT_RELATIONSHIP_TABLE);
        if (isEmpty(relationshipRows))  return;
        final Map<String, String> firstGuidByClientNumTo = new HashMap<>();
        for (Map<String, String> row : relationshipRows) {
            String clientNumTo = row.get(ClientConstants.CLIENT_NUM_TO_COL); // use exact column name
            String clientGuidTo = row.get(ClientConstants.CLIENT_GUID_TO);
            if (isBlank(clientNumTo)) continue;
            firstGuidByClientNumTo.putIfAbsent(clientNumTo, clientGuidTo);
        }
        for (Map<String, String> row : relationshipRows) {
            String clientNumTo = row.get(ClientConstants.CLIENT_NUM_TO_COL);
            if (isBlank(clientNumTo)) continue;
            String correctGuidTo = firstGuidByClientNumTo.get(clientNumTo);
            if (correctGuidTo != null && !correctGuidTo.isBlank()) {
                row.put("clientGuidTo", correctGuidTo);
            }
        }
    }

    private void remapClientRelationshipClientNumToFromClient(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> relationshipRows = getConfiguredRows(tableData, ClientConstants.CLIENT_RELATIONSHIP_TABLE);
        final List<Map<String, String>> clientRows = getConfiguredRows(tableData, ClientConstants.CLIENT_TABLE);
        if (isEmpty(relationshipRows) || isEmpty(clientRows)) return;
        final Map<String, String> clientNumByGuid = buildClientNumByGuid(clientRows);
        applyClientNumToRelationshipRows(relationshipRows, clientNumByGuid);
    }

    private Map<String, String> buildClientNumByGuid(final List<Map<String, String>> clientRows) {
        return toMap(clientRows, ClientConstants.CLIENT_GUID, ClientConstants.CLIENT_NUM);
    }

    private void applyClientNumToRelationshipRows(final List<Map<String, String>> relationshipRows, final Map<String, String> clientNumByGuid) {
        for (Map<String, String> relationshipRow : relationshipRows) {
            final String lookupGuid = resolveRelationshipLookupGuid(relationshipRow);
            if (isBlank(lookupGuid)) continue;
            final String mappedClientNum = clientNumByGuid.get(lookupGuid);
            if (mappedClientNum != null && !mappedClientNum.isBlank()) {
                relationshipRow.put(ClientConstants.CLIENT_NUM_TO_COL, mappedClientNum);
            }
        }
    }

    private String resolveRelationshipLookupGuid(final Map<String, String> relationshipRow) {
        final String clientGuidTo = relationshipRow.get(ClientConstants.CLIENT_GUID_TO);
        if (clientGuidTo != null && !clientGuidTo.isBlank()) return clientGuidTo;
        return relationshipRow.get(ClientConstants.CLIENT_GUID_LINK);
    }

    private String normalizeDateToDayMonthYear(final String rawValue) {
        if (isBlank(rawValue)) return "";
        String cleaned = rawValue.trim();
        List<DateTimeFormatter> inputFormats = List.of(
                DateTimeFormatter.ofPattern("dd-MM-yyyy"),
                DateTimeFormatter.ofPattern("d-M-yyyy"),
                DateTimeFormatter.ofPattern("d/M/yyyy"),
                DateTimeFormatter.ofPattern(ClientConstants.DATE_FORMAT),
                DateTimeFormatter.ofPattern("M/d/yy"),
                DateTimeFormatter.ofPattern("MM/dd/yy"),
                DateTimeFormatter.ofPattern("M/d/yyyy"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd")
        );
        for (DateTimeFormatter formatter : inputFormats) {
            try {
                LocalDate date = LocalDate.parse(cleaned, formatter);
                return date.format(DateTimeFormatter.ofPattern(ClientConstants.DATE_FORMAT));
            } catch (DateTimeParseException ignored) {
                //
            }
        }
        return cleaned;
    }

    private void normalizeClaimHistoryDates(Map<String, List<Map<String, String>>> tableData) {
        // CLAIM_HISTORY
        List<Map<String, String>> claimRows = getConfiguredRows(tableData, ClientConstants.CLAIM_HISTORY);
        if (claimRows != null) {
            for (Map<String, String> row : claimRows) {
                row.put("eventDt", normalizeDateToDayMonthYear(row.get("eventDt")));
                row.put("intimationDt", normalizeDateToDayMonthYear(row.get("intimationDt")));
                row.put("accidentDt", normalizeDateToDayMonthYear(row.get("accidentDt")));
                row.put("accidentDts", normalizeDateToDayMonthYear(row.get("accidentDts")));
                row.put("adjReasonCd", trimTo255(row.get("adjReasonCd")));
                row.put("adjRemarks", trimTo255(row.get("adjRemarks")));
            }
        }
        // PERSON (ADD THIS BLOCK)
        List<Map<String, String>> personRows = getConfiguredRows(tableData, ClientConstants.PERSON_TABLE);
        if (personRows != null) {
            for (Map<String, String> row : personRows) {
                row.put(ClientConstants.DATE_OF_BIRTH, normalizeDateToDayMonthYear(row.get(ClientConstants.DATE_OF_BIRTH)));
            }
        }
        List<Map<String, String>> policyRows = getConfiguredRows(tableData, ClientConstants.CLAIM_HISTORY_POLICY_TABLE);
        if (policyRows != null) {
            for (Map<String, String> row : policyRows) {
                row.put("commencementDt", normalizeDateToDayMonthYear(row.get("commencementDt")));
            }
        }
    }

    private String trimTo255(String value) {
        if (value == null) return "";
        value = value.trim();
        return value.length() > 255 ? value.substring(0, 255) : value;
    }

    private void normalizeClaimAdditionalFieldPaymentDate(final Map<String, List<Map<String, String>>> tableData) {
        final List<Map<String, String>> claimAddFldRows = getConfiguredRows(tableData, ClientConstants.CLAIM_ADDITIONAL_FIELD_TABLE);
        if (isEmpty(claimAddFldRows)) return;
        for (Map<String, String> row : claimAddFldRows) {
            final String fieldKey = emptyIfNull(row.get(ClientConstants.FIELD_KEY)).trim();
            if (!ClientConstants.PAYMENT_DATE_FIELD.equalsIgnoreCase(fieldKey)) continue;
            final String rawDate = emptyIfNull(row.get(ClientConstants.FIELD_VALUE)).trim();
            final String normalizedDate = normalizeDateToDayMonthYear(rawDate);
            if (!normalizedDate.isBlank()) {
                row.put(ClientConstants.FIELD_VALUE, normalizedDate);
            }
        }
    }

    /**
     * Creates additional rows (Nominee/Claimant/Appointee) based on conditions.
     */
    private void handleAdditionalRows(
            final Map<String, List<Map<String, String>>> tableData,
            final Map<String, String> baseRow,
            final TableDefinition tableDef,
            final String outputTableName,
            final CSVRecord csvRecord,
            final Map<String, String> guidContext) {

        final boolean isClientTable = isOutputForLogicalTable(outputTableName, ClientConstants.CLIENT_TABLE);
        final boolean isClaimHistoryClientTable = isOutputForLogicalTable(outputTableName, ClientConstants.CLAIM_HISTORY_CLIENT_TABLE);
        final boolean isAdmitFlow = isAdmitOutputTable(outputTableName);
        if (!isClientTable && !isClaimHistoryClientTable) return;
        if (!shouldCreateAdditionalRow(baseRow, tableDef.getCreateAdditionalRow(), isClaimHistoryClientTable, csvRecord)) return;
        final List<String> resolvedRoles = resolveAdditionalRoles(csvRecord);
        if (resolvedRoles.isEmpty()) return;
        if (isClaimHistoryClientTable) {
            for (String resolvedRole : resolvedRoles) {
                final Map<String, String> additionalClaimHistoryClientRow = createAdditionalClaimHistoryClientRow(baseRow, resolvedRole, guidContext, csvRecord);
                tableData.get(outputTableName).add(additionalClaimHistoryClientRow);
                appendClientAddressRow(tableData, additionalClaimHistoryClientRow, csvRecord);
                appendClientPhoneRow(tableData, additionalClaimHistoryClientRow, csvRecord);
                appendClientEmailRow(tableData, additionalClaimHistoryClientRow, csvRecord);
                appendClientAddFldRows(tableData, additionalClaimHistoryClientRow, csvRecord);
                appendClaimantAddFldRows(tableData, additionalClaimHistoryClientRow, csvRecord, isAdmitFlow);
                final String personGuid = appendPersonRow(tableData, additionalClaimHistoryClientRow, csvRecord);
                appendPersonIdentityRows(tableData, personGuid, resolvedRole, csvRecord);
                appendClaimHistoryPaymentRow(tableData, additionalClaimHistoryClientRow, csvRecord, isAdmitFlow);
            }
            return;
        }
        final Map<String, String> additionalClientRow = createAdditionalClientRow(guidContext);
        tableData.get(outputTableName).add(additionalClientRow);
        appendClientRelationshipRow(tableData, additionalClientRow, csvRecord, guidContext);
    }

    private boolean shouldCreateAdditionalRow(final Map<String, String> baseRow, final TableDefinition.CreateAdditionalRow config, final boolean isClaimHistoryClientTable, final CSVRecord csvRecord) {
        if (config == null) {
            return isClaimHistoryClientTable;
        }
        if (Boolean.FALSE.equals(config.getEnabled())) return false;
        final TableDefinition.Condition condition = config.getCondition();
        if (condition == null) return true;
        final Map<String, String> conditionValues = buildConditionValueLookup(baseRow, csvRecord);
        return isConditionSatisfied(condition, conditionValues);
    }

    private Map<String, String> buildConditionValueLookup(final Map<String, String> baseRow, final CSVRecord csvRecord) {
        final Map<String, String> conditionValues = new HashMap<>(baseRow);
        csvRecord.toMap().forEach((header, value) -> {
            if (isBlank(header)) return;
            conditionValues.putIfAbsent(header.trim(), value == null ? "" : value.trim());
        });
        return conditionValues;
    }

    private Map<String, String> createAdditionalClaimHistoryClientRow(
            final Map<String, String> baseRow,
            final String resolvedRole,
            final Map<String, String> guidContext,
            final CSVRecord csvRecord) {
        final Map<String, String> row = new LinkedHashMap<>();
        final String claimHistoryClientRefGuid = UUID.randomUUID().toString();
        row.put(ClientConstants.CLIENT_REF_GUID, claimHistoryClientRefGuid);
        row.put(ClientConstants.CLAIM_HISTORY_REF_GUID_COL, baseRow.get(ClientConstants.CLAIM_HISTORY_REF_GUID_COL));
        row.put("clientCd", baseRow.get("clientCd"));
        row.put(ClientConstants.ROLE_CD, resolvedRole);
        row.put("relatedToInsuredCd", resolveRelatedToInsuredCd(resolvedRole, csvRecord));
        guidContext.put(ClientConstants.CLAIM_HISTORY_CLIENT_REF_SECONDARY, claimHistoryClientRefGuid);
        registerRelationshipCd(guidContext, claimHistoryClientRefGuid, resolvedRole, csvRecord);
        return row;
    }

    private Map<String, String> createAdditionalClientRow(final Map<String, String> guidContext) {
        final Map<String, String> row = new LinkedHashMap<>();
        final String clientGuid = UUID.randomUUID().toString();
        final String clientNumGuid = "CL-" + clientNumCounter++;
        row.put(ClientConstants.CLIENT_GUID, clientGuid);
        row.put(ClientConstants.CLIENT_NUM, clientNumGuid);
        row.put(ClientConstants.CLIENT_TYPE_CD, ClientConstants.PERSON_CLIENT_TYPE);
        row.put(ClientConstants.CLIENT_REF_GUID, resolveClientRefGuid(guidContext));
        return row;
    }

    /**
     * Creates PERSON_IDENTITY rows linked to a given personGuid, role-based.
     * - INSURED      : identityTypeCd from TPCR_KYC_DECEASED_SUBMIT, identityNum = blank
     * - NOMINEE/CLAIMANT : multiple pairs; each skipped if both typeCd and num are blank
     * - Other roles  : no rows created
     */
    private void appendPersonIdentityRows(final Map<String, List<Map<String, String>>> tableData, final String personGuid, final String roleCd, final CSVRecord csvRecord) {
        if (isBlank(personGuid)) return;
        if (ClientConstants.INSURED.equalsIgnoreCase(roleCd)) {
            final String typeCd = readCsvValueSafely(csvRecord, "TPCR_KYC_DECEASED_SUBMIT");
            addIdentity(tableData, personGuid, typeCd, "");
            return;
        }
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            addIdentity(tableData, personGuid, readCsvValueSafely(csvRecord, "TPCR_KYC_NOM_SUBMIT"), "");
            addIdentity(tableData, personGuid, readCsvValueSafely(csvRecord, "TPCR_KYCID1"), readCsvValueSafely(csvRecord, "TPCR_KYCID1_NUMBER"));
            addIdentity(tableData, personGuid, readCsvValueSafely(csvRecord, "TPCR_KYCID2"), readCsvValueSafely(csvRecord, "TPCR_KYCID2_NUMBER"));
            addIdentity(tableData, personGuid, readCsvValueSafely(csvRecord, "TPCR_KYC_ID1"), readCsvValueSafely(csvRecord, "TPCR_KYCID1NUMBER"));
            addIdentity(tableData, personGuid, readCsvValueSafely(csvRecord, "TPCR_CKYC"), readCsvValueSafely(csvRecord, "TPCR_CKYC_NUMBER"));
            addIdentity(tableData, personGuid, "PAN", readCsvValueSafely(csvRecord, "TPCR_PANNUMBER"));
            addIdentity(tableData, personGuid, "AADHAR", readCsvValueSafely(csvRecord, "TPCR_AADHAARNUMBER"));
        }
    }

    /**
     * Adds a single PERSON_IDENTITY row. Skips if BOTH identityTypeCd and identityNum are blank.
     */
    private void addIdentity(final Map<String, List<Map<String, String>>> tableData, final String personGuid, final String identityTypeCd, final String identityNum) {
        final String safeTypeCd = identityTypeCd == null ? "" : identityTypeCd.trim();
        final String safeNum    = identityNum    == null ? "" : identityNum.trim();
        if (isBlank(safeTypeCd)) return;
        final Map<String, String> row = new LinkedHashMap<>();
        row.put("personIdentityGuid", UUID.randomUUID().toString());
        row.put(ClientConstants.PERSON_GUID_COL, personGuid);
        row.put("identityTypeCd", safeTypeCd);
        row.put("identityNum", safeNum);
        addRowToConfiguredTable(tableData, ClientConstants.PERSON_IDENTITY_TABLE, row);
    }

    /**
     * Creates CLAIM_HISTORY_PAYMENT row based on role-specific account details.
     */
    private void appendClaimHistoryPaymentRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord, final boolean isAdmitFlow) {
        String roleCd = claimHistoryClientRow.get(ClientConstants.ROLE_CD);
        if (!ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) && !ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) return;
        String claimHistoryRefGuid = claimHistoryClientRow.get(ClientConstants.CLAIM_HISTORY_REF_GUID);
        final String accountNumber = resolveAccountNumber(csvRecord);
        if (accountNumber.isBlank()) return;
        String clientRefGuid = claimHistoryClientRow.get(ClientConstants.CLIENT_REF_GUID);
        final Map<String, String> row = buildConfiguredDerivedRow(
                ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE,
                csvRecord,
                Map.of(
                        "claimHistoryPaymentGuid", UUID.randomUUID().toString(),
                        "clientRefGuid", clientRefGuid,
                        "claim_history_ref_guid", claimHistoryRefGuid,
                        "accountNumber", accountNumber,
                        "bankName", resolveBankName(csvRecord),
                        "branchIdNum1", resolveIfsc(csvRecord),
                        "paymentMethodCd", "BANK_TRANSFER"
                ));
        addRowToConfiguredTable(tableData, ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE, row,
                resolveClaimOutputTableName(ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE, isAdmitFlow));
    }

    private String resolveAccountNumber(CSVRecord csvRecord) {
        String claimantAcc = readCsvValueSafely(csvRecord, "TPCR_CLAIMANTBANKACCOUNT");
        if (!claimantAcc.isBlank()) return claimantAcc;
        return readCsvValueSafely(csvRecord, "TPCR_BENEF_BNK_ACC_NO");
    }

    private String resolveBankName(CSVRecord csvRecord) {
        String claimantBank = readCsvValueSafely(csvRecord, "TPCR_CLAIMANTBANK");
        if (!claimantBank.isBlank()) return claimantBank;
        return readCsvValueSafely(csvRecord, "TPCR_BENEF_BNK_NAME");
    }

    private String resolveIfsc(CSVRecord csvRecord) {
        String claimantIfsc = readCsvValueSafely(csvRecord, "TPCR_CLAIMANTBRANCHIFSC");
        if (!claimantIfsc.isBlank()) return claimantIfsc;
        return readCsvValueSafely(csvRecord, "TPCR_BENEF_IFSC");
    }

    private String resolveRelatedToInsuredCd(final String roleCd, final CSVRecord csvRecord) {
        if ("Insured".equalsIgnoreCase(roleCd) || "Appointee".equalsIgnoreCase(roleCd) ) return ""; // as per requirement
        final String relation = readCsvValueSafely(csvRecord, "TPCR_RELATION");
        if (!relation.isBlank()) {
            final String mapped = mapRelationToRelatedToInsuredCd(relation);
            if (!mapped.isBlank()) return mapped;
        }
        return "";
    }

    private String mapRelationToRelatedToInsuredCd(String relation) {
        if (relation == null) return "";
        Map<String, String> relationMap = tableMappingConfiguration.getMappings().get("relation");
        if (relationMap != null) {
            return relationMap.getOrDefault(relation.trim(), "");
        }
        return "";
    }

    private String normalizeRelationshipCd(final String relationshipCd) {
        final String mapped = mapRelationToRelatedToInsuredCd(relationshipCd);
        if (!mapped.isBlank()) return mapped;
        return relationshipCd;
    }

    private void registerRelationshipCd(final Map<String, String> guidContext, final String clientRefGuid, final String roleCd, final CSVRecord csvRecord) {
        if (isBlank(clientRefGuid)) return;
        guidContext.put(ClientConstants.RELATIONSHIP_CD_PREFIX + clientRefGuid, resolveRelationshipCd(roleCd, csvRecord));
    }

    /**
     * Creates CLIENT_RELATIONSHIP row linking related clients.
     */
    private void appendClientRelationshipRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> clientRow, final CSVRecord csvRecord, final Map<String, String> guidContext) {
        final String clientGuidTo = clientRow.getOrDefault(ClientConstants.CLIENT_GUID, "");
        if (clientGuidTo.isBlank()) return;
        final String clientRefGuid = clientRow.getOrDefault(ClientConstants.CLIENT_REF_GUID, "");
        final String relationshipCd = normalizeRelationshipCd(resolveRelationshipCdForClientRef(clientRefGuid, guidContext, csvRecord));
        final Map<String, String> relationshipRow = buildConfiguredDerivedRow(
                ClientConstants.CLIENT_RELATIONSHIP_TABLE,
                csvRecord,
                Map.of(
                        "clientGuidTo", clientGuidTo,
                        ClientConstants.RELATIONSHIP_CD, relationshipCd,
                        ClientConstants.CLIENT_GUID_LINK, clientRefGuid,
                        "clientRelationshipGuid", UUID.randomUUID().toString()
                ));
        addRowToConfiguredTable(tableData, ClientConstants.CLIENT_RELATIONSHIP_TABLE, relationshipRow);
    }

    private void removeBlankClientGuidRows(final Map<String, List<Map<String, String>>> tableData) {
        List<Map<String, String>> rows = getConfiguredRows(tableData, ClientConstants.CLIENT_RELATIONSHIP_TABLE);
        if (isEmpty(rows)) return;
        rows.removeIf(row -> {
            String clientGuid = row.get(ClientConstants.CLIENT_GUID_LINK);
            return clientGuid == null || clientGuid.isBlank();
        });
    }

    private String resolveRelationshipCdForClientRef(final String clientRefGuid, final Map<String, String> guidContext, final CSVRecord csvRecord) {
        if (clientRefGuid != null && !clientRefGuid.isBlank()) {
            final String cached = guidContext.get(ClientConstants.RELATIONSHIP_CD_PREFIX + clientRefGuid);
            if (cached != null && !cached.isBlank()) return cached;
        }
        return resolveConfiguredRoleBasedValue(ClientConstants.CLIENT_RELATIONSHIP_TABLE, ClientConstants.RELATIONSHIP_CD, csvRecord, preferredRelationshipIdentifiers(null));
    }

    private String resolveRelationshipCd(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.CLIENT_RELATIONSHIP_TABLE, ClientConstants.RELATIONSHIP_CD, csvRecord, preferredRelationshipIdentifiers(roleCd));
    }

    private String resolveClientRefGuid(final Map<String, String> guidContext) {
        final String secondary = guidContext.get(ClientConstants.CLAIM_HISTORY_CLIENT_REF_SECONDARY);
        if (secondary != null && !secondary.isBlank()) return secondary;
        final String primary = guidContext.get(ClientConstants.CLAIM_HISTORY_CLIENT_REF_PRIMARY);
        if (primary != null && !primary.isBlank()) return primary;
        return UUID.randomUUID().toString();
    }

    /**
     * Resolves role type for additional rows from CSV data.
     */
    private List<String> resolveAdditionalRoles(final CSVRecord csvRecord) {
        String relationType = readCsvValueSafely(csvRecord, "TPCR_REL_TYPE");
        if (relationType.isBlank()) {
            return List.of();
        }
        if ("GUARDIAN".equalsIgnoreCase(relationType)) {
            return List.of(ClientConstants.ROLE_APPOINTEE);
        }
        if ("NOMINEE".equalsIgnoreCase(relationType)) {
            return List.of(ClientConstants.ROLE_NOMINEE);
        }
        if ("CLAIMANT".equalsIgnoreCase(relationType)) {
            return List.of(ClientConstants.ROLE_CLAIMANT);
        }
        if ("BOTH".equalsIgnoreCase(relationType)) {
            return List.of(ClientConstants.ROLE_CLAIMANT, ClientConstants.ROLE_APPOINTEE);
        }
        return List.of(relationType);
    }

    private String readCsvValueSafely(final CSVRecord csvRecord, final String header) {
        try {
            final String value = csvRecord.get(header);
            return value == null ? "" : value.trim();
        } catch (Exception ex) {
            return "";
        }
    }

    /**
     * Adds CLIENT_ADDRESS row if address data is present.
     */
    private void appendClientAddressRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord) {
        final String addressLine1 = resolveAddressLine1(claimHistoryClientRow.get(ClientConstants.ROLE_CD), csvRecord);
        if (addressLine1.trim().isEmpty()) return;
        final Map<String, String> clientAddressRow = buildConfiguredDerivedRow(
                ClientConstants.CLIENT_ADDRESS_TABLE,
                csvRecord,
                Map.of(
                        "clientAddressGuid", UUID.randomUUID().toString(),
                        ClientConstants.CLIENT_GUID_LINK, claimHistoryClientRow.getOrDefault(ClientConstants.CLIENT_REF_GUID, ""),
                        "addressLine1", addressLine1
                ));
        addRowToConfiguredTable(tableData, ClientConstants.CLIENT_ADDRESS_TABLE, clientAddressRow);
    }

    private String resolveAddressLine1(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.CLIENT_ADDRESS_TABLE, "addressLine1", csvRecord, preferredAddressLineIdentifiers(roleCd));
    }

    /**
     * Adds CLIENT_PHONE row based on role-specific contact number.
     */
    private void appendClientPhoneRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord) {
        final String roleCd = claimHistoryClientRow.get(ClientConstants.ROLE_CD);
        if (ClientConstants.INSURED.equalsIgnoreCase(roleCd)) {
            final String mobileNumber = readCsvValueSafely(csvRecord, ClientConstants.CSV_COL_MOBILE_NUMBER);
            final String tpdMobileNumber = readCsvValueSafely(csvRecord, "TPD_MOBILENO");
            if (mobileNumber.isBlank() && tpdMobileNumber.isBlank()) {
                log.debug("Skipping CLIENT_PHONE row – both insured contact numbers are blank");
                return;
            }
            if (!mobileNumber.isBlank()) {
                addClientPhoneRow(tableData, claimHistoryClientRow, csvRecord, mobileNumber);
            }
            if (!tpdMobileNumber.isBlank() && !tpdMobileNumber.equalsIgnoreCase(mobileNumber)) {
                addClientPhoneRow(tableData, claimHistoryClientRow, csvRecord, tpdMobileNumber);
            }
            return;
        }
        final String contactNum = resolveContactNum(roleCd, csvRecord);
        if (contactNum.isBlank()) {
            log.debug("Skipping CLIENT_PHONE row – contactNum is blank for roleCd '{}'", roleCd);
            return;
        }
        addClientPhoneRow(tableData, claimHistoryClientRow, csvRecord, contactNum);
    }

    private void addClientPhoneRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord, final String contactNum) {
        final Map<String, String> clientPhoneRow = buildConfiguredDerivedRow(
                ClientConstants.CLIENT_PHONE_TABLE,
                csvRecord,
                Map.of(
                        "clientPhoneGuid", UUID.randomUUID().toString(),
                        ClientConstants.CLIENT_GUID_LINK, claimHistoryClientRow.getOrDefault(ClientConstants.CLIENT_REF_GUID, ""),
                        "contactNum", contactNum
                ));
        addRowToConfiguredTable(tableData, ClientConstants.CLIENT_PHONE_TABLE, clientPhoneRow);
    }

    private String resolveContactNum(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.CLIENT_PHONE_TABLE, "contactNum", csvRecord, preferredContactIdentifiers(roleCd));
    }

    private void appendClientEmailRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord) {
        final String emailId = resolveEmailId(claimHistoryClientRow.get(ClientConstants.ROLE_CD), csvRecord);
        if (emailId.isBlank()) {
            log.debug("Skipping CLIENT_EMAIL row – emailId is blank for roleCd '{}'", claimHistoryClientRow.get(ClientConstants.ROLE_CD));
            return;
        }
        final Map<String, String> clientEmailRow = buildConfiguredDerivedRow(
                ClientConstants.CLIENT_EMAIL_TABLE,
                csvRecord,
                Map.of(
                        "clientEmailGuid", UUID.randomUUID().toString(),
                        ClientConstants.CLIENT_GUID_LINK, claimHistoryClientRow.getOrDefault(ClientConstants.CLIENT_REF_GUID, ""),
                        ClientConstants.TYPE_CD, ClientConstants.PERSONAL_EMAIL_TYPE_CD,
                        "emailId", emailId
                ));
        addRowToConfiguredTable(tableData, ClientConstants.CLIENT_EMAIL_TABLE, clientEmailRow);
    }

    /**
     * Adds CLIENT additional field rows dynamically from CSV.
     */
    private void appendClientAddFldRows(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord) {
        String roleCd = claimHistoryClientRow.get(ClientConstants.ROLE_CD);
        if (!"Insured".equalsIgnoreCase(roleCd)) return;
        final String clientRefGuid = claimHistoryClientRow.getOrDefault(ClientConstants.CLIENT_REF_GUID, "");
        appendConfiguredAdditionalFieldRows(tableData, clientRefGuid, csvRecord);
    }

    /**
     * Adds CLAIM additional field rows linked to claim GUID.
     */
    private void appendClaimantAddFldRows(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord, final boolean isAdmitFlow) {
        final String claimGuid = claimHistoryClientRow.getOrDefault(ClientConstants.CLAIM_HISTORY_REF_GUID, "");
        appendConfiguredAdditionalFieldRows(tableData, ClientConstants.CLAIM_ADDITIONAL_FIELD_TABLE, ClientConstants.CLAIM_GUID_LINK, claimGuid, "claimAddFldGuid", csvRecord,
                resolveClaimOutputTableName(ClientConstants.CLAIM_ADDITIONAL_FIELD_TABLE, isAdmitFlow));
    }

    /**
     * Creates a PERSON row and returns generated personGuid.
     */
    private String appendPersonRow(final Map<String, List<Map<String, String>>> tableData, final Map<String, String> claimHistoryClientRow, final CSVRecord csvRecord) {
        final String roleCd = claimHistoryClientRow.get(ClientConstants.ROLE_CD);
        final String firstName = resolvePersonFirstName(roleCd, csvRecord);
        String personGuid = UUID.randomUUID().toString();
        String rawAge = resolvePersonAge(roleCd, csvRecord);
        String cleanedAge = extractYearsOnly(rawAge);
        final Map<String, String> personRow = buildConfiguredDerivedRow(
                ClientConstants.PERSON_TABLE,
                csvRecord,
                Map.of(
                        ClientConstants.PERSON_GUID_COL, personGuid,
                        ClientConstants.CLIENT_GUID_LINK, claimHistoryClientRow.getOrDefault(ClientConstants.CLIENT_REF_GUID, ""),
                        "firstName", firstName,
                        "genderCd", resolvePersonGender(roleCd, csvRecord),
                        "dateOfBirth", resolvePersonDateOfBirth(roleCd, csvRecord),
                        "age", cleanedAge,
                        "otherName", resolvePersonOtherName(roleCd, csvRecord)
                ));
        addRowToConfiguredTable(tableData, ClientConstants.PERSON_TABLE, personRow);
        return personGuid;
    }

    private String extractYearsOnly(String rawAge) {
        if (isBlank(rawAge)) return "";
        String[] parts = rawAge.trim().split("\\s+");
        if (parts.length > 0) {
            return parts[0];
        }
        return "";
    }

    private String resolvePersonFirstName(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.PERSON_TABLE, "firstName", csvRecord, preferredPersonFirstNameIdentifiers(roleCd));
    }

    private String resolvePersonGender(final String roleCd, final CSVRecord csvRecord) {
        final String raw = resolveConfiguredRoleBasedValue(ClientConstants.PERSON_TABLE, "genderCd", csvRecord, preferredPersonGenderIdentifiers(roleCd));
        return normalizeGenderCd(raw);
    }

    private String normalizeGenderCd(final String genderCd) {
        if (genderCd == null) return "";
        final String trimmed = genderCd.trim();
        if ("M".equalsIgnoreCase(trimmed)) return "MALE";
        if ("F".equalsIgnoreCase(trimmed)) return "FEMALE";
        return trimmed;
    }

    private String resolvePersonDateOfBirth(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.PERSON_TABLE, "dateOfBirth", csvRecord, preferredPersonDateOfBirthIdentifiers(roleCd));
    }

    private String resolvePersonAge(final String roleCd, final CSVRecord csvRecord) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return readCsvValueSafely(csvRecord, "TPCR_NOM_AGE");
        }
        return "";
    }

    private String resolveEmailId(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.CLIENT_EMAIL_TABLE, "emailId", csvRecord, preferredEmailIdentifiers(roleCd));
    }

    private boolean isConditionSatisfied(final TableDefinition.Condition condition, final Map<String, String> values) {
        if (condition == null) return false;
        final List<String> anyNotEmpty = condition.getAnyNotEmpty();
        if (isEmpty(anyNotEmpty)) return false;
        for (String field : anyNotEmpty) {
            if (isBlank(field)) continue;
            final String value = values.get(field.trim());
            if (value != null && !value.isBlank()) return true;
        }
        return false;
    }

    /**
     * Assembles a row map for the given table.
     * Returns {@code null} when a required FK GUID is not yet available (parent was skipped).
     */
    private Map<String, String> buildRowData(final CSVRecord csvRecord, final TableDefinition tableDef, final String currentTableGuid, final Map<String, String> guidContext) {
        if (validateParentGuidPresent(tableDef, guidContext) && validateRootGuidPresent(tableDef, guidContext)) {
            final Map<String, String> rowData = new LinkedHashMap<>();
            populateColumns(csvRecord, tableDef, currentTableGuid, rowData, guidContext);
            return rowData;
        }
        return Collections.emptyMap();
    }

    /**
     * Validates the parent table's GUID exists in context (parent row was not skipped).
     * Does NOT write anything to rowData – placement is handled inside populateColumns via column definitions.
     */
    private boolean validateParentGuidPresent(final TableDefinition tableDef, final Map<String, String> guidContext) {
        if (tableDef.getParent() == null || tableDef.getParentGuidRef() == null) return true;
        final String parentGuid = guidContext.get(tableDef.getParent());
        if (isBlank(parentGuid)) {
            log.debug("Parent '{}' has no GUID in context – child row will be skipped", tableDef.getParent());
            return false;
        }
        return true;
    }

    /**
     * Validates the root table's GUID exists in context.
     * Does NOT write anything to rowData – placement is handled inside populateColumns via column definitions.
     */
    private boolean validateRootGuidPresent(final TableDefinition tableDef, final Map<String, String> guidContext) {
        if (tableDef.getRootGuidRef() == null) return true;
        final String rootGuid = guidContext.get(tableDef.getRootTable() != null ? tableDef.getRootTable() : "client");
        if (isBlank(rootGuid)) {
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
    private void populateColumns(final CSVRecord csvRecord, final TableDefinition tableDef, final String currentTableGuid, final Map<String, String> rowData, final Map<String, String> guidContext) {
        if (tableDef.getColumns() == null) return;
        tableDef.getColumns().forEach((columnName, columnDef) -> rowData.put(columnName, resolveColumnValue(csvRecord, tableDef, currentTableGuid, guidContext, columnDef)));
    }

    private String resolveColumnValue(final CSVRecord csvRecord, final TableDefinition tableDef, final String currentTableGuid, final Map<String, String> guidContext, final TableMappingConfiguration.ColumnDefinition columnDef) {
        final String guidMappedValue = resolveGuidMappedValue(tableDef, currentTableGuid, guidContext, columnDef);
        if (guidMappedValue != null) return guidMappedValue;
        return resolveConfiguredColumnValue(columnDef, csvRecord);
    }

    // ── Skip-row evaluation ───────────────────────────────────────────────────
    /**
     * Returns {@code true} when {@code skipIfEmpty = true} is configured and
     * all non-FK column values are blank.
     */
    private boolean shouldSkipRow(final Map<String, String> rowData, final TableDefinition tableDef) {
        if (!Boolean.TRUE.equals(tableDef.getSkipIfEmpty())) return false;
        final Set<String> fkColumns = buildFkColumnSet(tableDef);
        return rowData.entrySet().stream()
                .filter(entry -> !fkColumns.contains(entry.getKey()))
                .allMatch(entry -> isEmpty(Collections.singletonList(entry.getValue())));
    }

    private Set<String> buildFkColumnSet(final TableDefinition tableDef) {
        final Set<String> fkColumns = new HashSet<>();
        if (tableDef.getColumns() == null) return fkColumns;
        tableDef.getColumns().forEach((columnName, columnDef) -> {
            if (isParentGuidRefColumn(tableDef, columnDef) || isRootGuidRefColumn(tableDef, columnDef)) {
                fkColumns.add(columnName);
            }
        });
        return fkColumns;
    }

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
        if (tableDef == null || tableDef.getTableName() == null || tableDef.getTableName().isBlank()) {
            return logicalTableName;
        }
        return tableDef.getTableName().trim();
    }

    public String convertResultSetToCsv(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        try (StringWriter writer = new StringWriter(); CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            final List<String> headers = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                headers.add(resolveColumnLabel(metaData, i));
            }
            csvPrinter.printRecord(headers);
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount++;
                final List<String> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.add(safeReadAsString(resultSet, i));
                }
                csvPrinter.printRecord(row);
            }
            csvPrinter.flush();
            log.info("Database rows serialized to CSV: {}", rowCount);
            return writer.toString();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to serialize database result to CSV.", ex);
        }
    }

    private String resolveColumnLabel(final ResultSetMetaData metaData, final int columnIndex) throws SQLException {
        final String label = metaData.getColumnLabel(columnIndex);
        if (label != null && !label.isBlank()) return label.trim();
        final String fallback = metaData.getColumnName(columnIndex);
        return fallback == null ? "" : fallback.trim();
    }

    private String safeReadAsString(final ResultSet resultSet, final int columnIndex) throws SQLException {
        final Object value = resultSet.getObject(columnIndex);
        return value == null ? "" : String.valueOf(value);
    }

    private void loadDriverIfConfigured() {
        final String driverClassName = dbSourceProperties.getDriverClassName();
        if (isBlank(driverClassName)) return;
        try {
            Class.forName(driverClassName.trim());
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("Configured JDBC driver class was not found: " + driverClassName, ex);
        }
    }

    private String validateSqlIdentifier(final String identifier) {
        final String trimmed = identifier == null ? "" : identifier.trim();
        if (!ClientConstants.SAFE_SQL_IDENTIFIER.matcher(trimmed).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier configured for migration.db.source-table.");
        }
        return trimmed;
    }

    private int safePositive(final Integer value, final int defaultValue) {
        if (value == null || value <= 0) return defaultValue;
        return value;
    }

    private String requireNonBlank(final String value, final String message) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(message);
        }
        return value.trim();
    }

    private boolean isSqlServerDatabaseAccessFailure(final SQLException ex) {
        if (ex.getErrorCode() == ClientConstants.SQL_SERVER_CANNOT_OPEN_DATABASE_ERROR) return true;
        final String message = ex.getMessage();
        return message != null && message.contains(ClientConstants.SQL_SERVER_ACCESS_DENIED_FRAGMENT);
    }

    private String resolveDatabaseNameFromUrl(final String jdbcUrl) {
        final String marker = "databaseName=";
        final int markerStart = jdbcUrl.indexOf(marker);
        if (markerStart < 0) return "<unknown>";
        final int valueStart = markerStart + marker.length();
        final int valueEnd = jdbcUrl.indexOf(';', valueStart);
        if (valueEnd < 0) return jdbcUrl.substring(valueStart).trim();
        return jdbcUrl.substring(valueStart, valueEnd).trim();
    }

    private String resolveGuidMappedValue(final TableDefinition tableDef, final String currentTableGuid, final Map<String, String> guidContext, final TableMappingConfiguration.ColumnDefinition columnDef) {
        if (isParentGuidRefColumn(tableDef, columnDef)) {
            return emptyIfNull(guidContext.get(tableDef.getParent()));
        }
        if (isRootGuidRefColumn(tableDef, columnDef)) {
            final String rootTable = tableDef.getRootTable() != null ? tableDef.getRootTable() : "client";
            return emptyIfNull(guidContext.get(rootTable));
        }
        if (isGuidIdentifierColumn(tableDef, columnDef, currentTableGuid)) {
            return emptyIfNull(currentTableGuid);
        }
        return null;
    }


    private Map<String, List<String>> resolveConfiguredSheetHeaders() {
        final Map<String, List<String>> configuredHeadersBySheet = new LinkedHashMap<>();
        if (tableMappingConfiguration.getTables() == null) return configuredHeadersBySheet;
        tableMappingConfiguration.getTables().forEach((logicalTableName, definition) -> {
            final List<String> headers = buildConfiguredHeaders(definition);
            configuredHeadersBySheet.put(resolveOutputTableName(logicalTableName, definition), headers);
        });
        return configuredHeadersBySheet;
    }

    private List<String> buildConfiguredHeaders(final TableDefinition definition) {
        final List<String> headers = new ArrayList<>();
        if (definition == null) return headers;
        if (definition.getColumns() != null && definition.getColumns().containsKey(ClientConstants.ID)) {
            headers.add(ClientConstants.ID);
        }
        if (definition.getGuidColumn() != null && !definition.getGuidColumn().isBlank()) {
            final String guidColumn = definition.getGuidColumn().trim();
            if (!headers.contains(guidColumn)) headers.add(guidColumn);
        }
        if (definition.getColumns() != null) headers.addAll(definition.getColumns().keySet());
        return headers;
    }

    private Map<String, String> buildConfiguredDerivedRow(final String logicalTableName, final CSVRecord csvRecord, final Map<String, String> overrides) {
        final Map<String, String> row = createConfiguredRow(logicalTableName);
        final TableDefinition tableDefinition = getTableDefinition(logicalTableName);
        if (tableDefinition != null && tableDefinition.getColumns() != null) {
            tableDefinition.getColumns().forEach((columnName, columnDefinition) -> {
                if (overrides.containsKey(columnName)) {
                    row.put(columnName, emptyIfNull(overrides.get(columnName)));
                    return;
                }
                row.put(columnName, resolveConfiguredColumnValue(columnDefinition, csvRecord));
            });
        }
        overrides.forEach((key, value) -> row.putIfAbsent(key, emptyIfNull(value)));
        return row;
    }

    private Map<String, String> createConfiguredRow(final String logicalTableName) {
        final Map<String, String> row = new LinkedHashMap<>();
        final TableDefinition tableDefinition = getTableDefinition(logicalTableName);
        if (tableDefinition == null || tableDefinition.getColumns() == null) return row;
        tableDefinition.getColumns().forEach((columnName, ignored) -> row.put(columnName, ""));
        return row;
    }

    private TableDefinition getTableDefinition(final String logicalTableName) {
        if (tableMappingConfiguration.getTables() == null) return null;
        return tableMappingConfiguration.getTables().get(logicalTableName);
    }

    private String resolveConfiguredRoleBasedValue(final String logicalTableName, final String columnName, final CSVRecord csvRecord, final List<String> preferredIdentifiers) {
        final TableDefinition tableDefinition = getTableDefinition(logicalTableName);
        if (tableDefinition == null || tableDefinition.getColumns() == null) return "";
        final TableMappingConfiguration.ColumnDefinition columnDefinition = tableDefinition.getColumns().get(columnName);
        return resolveConfiguredColumnValue(columnDefinition, csvRecord, preferredIdentifiers, false);
    }

    private String resolveConfiguredColumnValue(final TableMappingConfiguration.ColumnDefinition columnDefinition, final CSVRecord csvRecord) {
        return resolveConfiguredColumnValue(columnDefinition, csvRecord, null, true);
    }

    private String resolveConfiguredColumnValue(final TableMappingConfiguration.ColumnDefinition columnDefinition, final CSVRecord csvRecord, final List<String> preferredIdentifiers, final boolean fallbackToConfiguredIdentifiers) {
        if (columnDefinition == null) return "";
        final String literal = emptyIfNull(columnDefinition.getLiteral()).trim();
        if (!literal.isBlank()) {
            if (ClientConstants.GETDATE_LITERAL.equalsIgnoreCase(literal)) {
                return LocalDate.now().format(DateTimeFormatter.ofPattern(ClientConstants.DATE_FORMAT));
            }
            return literal;
        }
        final String preferredValue = resolveValueFromIdentifiers(csvRecord, columnDefinition, preferredIdentifiers);
        if (!preferredValue.isBlank()) return preferredValue;
        if (preferredIdentifiers != null && !fallbackToConfiguredIdentifiers) return "";
        return resolveValueFromIdentifiers(csvRecord, columnDefinition, columnDefinition.getIdentifiers());
    }

    private String resolveValueFromIdentifiers(final CSVRecord csvRecord, final TableMappingConfiguration.ColumnDefinition columnDefinition, final List<String> candidateIdentifiers) {
        if (columnDefinition == null || isEmpty(candidateIdentifiers)) return "";
        final Set<String> configuredIdentifiers = new HashSet<>();
        if (columnDefinition.getIdentifiers() != null) {
            columnDefinition.getIdentifiers().stream()
                    .map(this::normalizeValueKey)
                    .filter(value -> !value.isBlank())
                    .forEach(configuredIdentifiers::add);
        }
        return candidateIdentifiers.stream()
                .map(candidateIdentifier -> {
                    final String normalized = normalizeValueKey(candidateIdentifier);
                    if (normalized.isBlank()) return "";
                    if (!configuredIdentifiers.isEmpty() && !configuredIdentifiers.contains(normalized)) return "";
                    return resolveIdentifierValue(csvRecord, candidateIdentifier);
                })
                .filter(value -> !value.isBlank())
                .findFirst()
                .orElse("");
    }

    private String resolveIdentifierValue(final CSVRecord csvRecord, final String identifier) {
        final String trimmedIdentifier = identifier == null ? "" : identifier.trim();
        if (trimmedIdentifier.isBlank()) return "";
        final String sourceValue = readCsvValueByIdentifier(csvRecord, trimmedIdentifier);
        if (!sourceValue.isBlank()) return sourceValue;
        if (isLiteralCandidate(trimmedIdentifier)) return trimmedIdentifier;
        return "";
    }

    private String readCsvValueByIdentifier(final CSVRecord csvRecord, final String identifier) {
        final String normalizedIdentifier = normalizeValueKey(identifier);
        if (normalizedIdentifier.isBlank()) return "";
        for (Map.Entry<String, String> entry : csvRecord.toMap().entrySet()) {
            if (!normalizedIdentifier.equals(normalizeValueKey(entry.getKey()))) continue;
            return entry.getValue() == null ? "" : entry.getValue().trim();
        }
        return "";
    }

    private String normalizeValueKey(final String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private boolean isLiteralCandidate(final String identifier) {
        final String trimmedIdentifier = identifier == null ? "" : identifier.trim();
        if (trimmedIdentifier.isBlank()) return false;
        if (trimmedIdentifier.toUpperCase(Locale.ROOT).startsWith("TPCR_") || trimmedIdentifier.contains("_")) return false;
        return trimmedIdentifier.equalsIgnoreCase("true")
                || trimmedIdentifier.equalsIgnoreCase("false")
                || trimmedIdentifier.contains("/")
                || trimmedIdentifier.equals(trimmedIdentifier.toUpperCase(Locale.ROOT));
    }

    private void appendConfiguredAdditionalFieldRows(
            final Map<String, List<Map<String, String>>> tableData,
            final String linkValue,
            final CSVRecord csvRecord) {
        appendConfiguredAdditionalFieldRows(tableData, ClientConstants.CLIENT_ADD_FLD_TABLE, ClientConstants.CLIENT_GUID_LINK, linkValue, "clientAddFldGuid", csvRecord, "");
    }

    private void appendConfiguredAdditionalFieldRows(
            final Map<String, List<Map<String, String>>> tableData,
            final String logicalTableName,
            final String linkColumnName,
            final String linkValue,
            final String guidColumnName,
            final CSVRecord csvRecord,
            final String explicitOutputTableName) {
        final TableDefinition tableDefinition = getTableDefinition(logicalTableName);
        if (tableDefinition == null || tableDefinition.getColumns() == null) return;
        final TableMappingConfiguration.ColumnDefinition fieldKeyDefinition = tableDefinition.getColumns().get(ClientConstants.FIELD_KEY);
        if (fieldKeyDefinition == null || fieldKeyDefinition.getIdentifiers() == null) return;
        fieldKeyDefinition.getIdentifiers().stream()
                .map(fieldKey -> fieldKey == null ? "" : fieldKey.trim())
                .filter(trimmedFieldKey -> !trimmedFieldKey.isBlank())
                .forEach(trimmedFieldKey -> {
                    final String fieldValue = readCsvValueByIdentifier(csvRecord, trimmedFieldKey);
                    if (fieldValue.isBlank()) return;
                    final Map<String, String> row = buildConfiguredDerivedRow(
                            logicalTableName,
                            csvRecord,
                            Map.of(
                                    linkColumnName, emptyIfNull(linkValue),
                                    guidColumnName, UUID.randomUUID().toString(),
                                    ClientConstants.FIELD_KEY, trimmedFieldKey,
                                    ClientConstants.FIELD_VALUE, fieldValue
                            ));
                    addRowToConfiguredTable(tableData, logicalTableName, row, explicitOutputTableName);
                });
    }

    private List<String> preferredAddressLineIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_NOMINEEADDRESSLINE1");
        }
        if (ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_CLAIMANT_ADD");
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_ADDRESSOFGUARDIAN");
        }
        return List.of(ClientConstants.CSV_COL_ADDRESS_LINE_1);
    }

    private List<String> preferredContactIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_CLAIMANT_MOBNO");
        }
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_NOMINEEMOBILENUMBER");
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_GUARDIANMOBILENUMBER");
        }
        return List.of(ClientConstants.CSV_COL_MOBILE_NUMBER);
    }

    private List<String> preferredEmailIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_NOMINEEEMAILID");
        }
        if (ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_CLAIMANTEMAILID");
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_GUARDIANEMAILID");
        }
        if ("PAYEE".equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_EMAILID");
        }
        return List.of(ClientConstants.CSV_COL_EMAIL);
    }

    private List<String> preferredRelationshipIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_RELATIONOFCLAIMANT");
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_RELATIONSHIPOFGUARDIAN");
        }
        return List.of(ClientConstants.CSV_COL_RELATION);
    }

    private List<String> preferredPersonFirstNameIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_CLAIMANT_NAME, "TPCR_NOMINEEFIRSTNAME", "TPCR_BENEF_NAME");
        }
        if (ClientConstants.INSURED.equalsIgnoreCase(roleCd)) {
            return List.of("TPD_APPLICANTNAME");
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_GUARDIAN_NAME);
        }
        return List.of();
    }

    private List<String> preferredPersonGenderIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_CLAIMANT_GENDER, ClientConstants.CSV_COL_NOMINEE_GENDER, ClientConstants.CSV_COL_GENDER);
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_GENDER);
        }
        return List.of();
    }

    private List<String> preferredPersonDateOfBirthIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_CLAIMANT_DOB, ClientConstants.CSV_COL_NOM_DOB);
        }
        if (ClientConstants.INSURED.equalsIgnoreCase(roleCd)) {
            return List.of("TPD_APPLICANT_DOB", ClientConstants.CSV_COL_DOB_DECEASED);
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_DOB_DECEASED);
        }
        return List.of();
    }

    private String resolvePersonOtherName(final String roleCd, final CSVRecord csvRecord) {
        return resolveConfiguredRoleBasedValue(ClientConstants.PERSON_TABLE, "otherName", csvRecord, preferredPersonOtherNameIdentifiers(roleCd));
    }

    private List<String> preferredPersonOtherNameIdentifiers(final String roleCd) {
        if (ClientConstants.ROLE_NOMINEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_CLAIMANT.equalsIgnoreCase(roleCd)) {
            return List.of("TPCR_CORRECTNOMINEEFIRSTNAME");
        }
        if (ClientConstants.ROLE_APPOINTEE.equalsIgnoreCase(roleCd) || ClientConstants.ROLE_GUARDIAN.equalsIgnoreCase(roleCd)) {
            return List.of(ClientConstants.CSV_COL_GUARDIAN_NAME);
        }
        return List.of();
    }

    private String emptyIfNull(final String value) {
        return value == null ? "" : value;
    }

    private boolean isDerivedRowTable(final String logicalTableName) {
        return ClientConstants.CLIENT_ADDRESS_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.CLIENT_PHONE_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.CLIENT_EMAIL_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.CLIENT_RELATIONSHIP_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.CLIENT_ADD_FLD_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.CLAIM_ADDITIONAL_FIELD_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.PERSON_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE.equalsIgnoreCase(logicalTableName)
                || ClientConstants.PERSON_IDENTITY_TABLE.equalsIgnoreCase(logicalTableName);
    }

    private List<Map<String, String>> getConfiguredRows(final Map<String, List<Map<String, String>>> tableData, final String logicalTableName) {
        final String configuredOutputTableName = resolveConfiguredOutputTableName(logicalTableName);
        if (configuredOutputTableName.isBlank()) {
            return Collections.emptyList();
        }
        return tableData.get(configuredOutputTableName);
    }

    private List<Map<String, String>> getConfiguredRowsForClaimAndAdmit(final Map<String, List<Map<String, String>>> tableData, final String logicalTableName) {
        final List<Map<String, String>> combinedRows = new ArrayList<>();
        final List<Map<String, String>> baseRows = getConfiguredRows(tableData, logicalTableName);
        if (baseRows != null) {
            combinedRows.addAll(baseRows);
        }
        final String baseOutputTableName = resolveConfiguredOutputTableName(logicalTableName);
        final String admitOutputTableName = resolveClaimOutputTableName(logicalTableName, true);
        if (!isBlank(admitOutputTableName) && !admitOutputTableName.equalsIgnoreCase(baseOutputTableName)) {
            final List<Map<String, String>> admitRows = tableData.get(admitOutputTableName);
            if (admitRows != null) {
                combinedRows.addAll(admitRows);
            }
        }
        return combinedRows;
    }

    private void addRowToConfiguredTable(final Map<String, List<Map<String, String>>> tableData, final String logicalTableName, final Map<String, String> row) {
        addRowToConfiguredTable(tableData, logicalTableName, row, "");
    }

    private void addRowToConfiguredTable(final Map<String, List<Map<String, String>>> tableData, final String logicalTableName, final Map<String, String> row,
                                         final String explicitOutputTableName) {
        String outputTableName = explicitOutputTableName;
        if (isBlank(outputTableName)) {
            outputTableName = resolveConfiguredOutputTableName(logicalTableName);
        }
        if (isBlank(outputTableName)) return;
        tableData.computeIfAbsent(outputTableName, ignored -> new ArrayList<>()).add(row);
    }

    private String resolveConfiguredOutputTableName(final String logicalTableName) {
        if (tableMappingConfiguration.getTables() == null) return "";
        final TableDefinition definition = tableMappingConfiguration.getTables().get(logicalTableName);
        if (definition == null) return "";
        return resolveOutputTableName(logicalTableName, definition);
    }

    private boolean isOutputForLogicalTable(final String outputTableName, final String logicalTableName) {
        if (logicalTableName.equalsIgnoreCase(outputTableName)) return true;
        return resolveAdmitSheetName(logicalTableName).equalsIgnoreCase(outputTableName);
    }

     private boolean isAdmitOutputTable(final String outputTableName) {
         if (outputTableName == null) return false;
         if (outputTableName.toUpperCase(Locale.ROOT).startsWith(ADMIT_PREFIX)) return true;
         return outputTableName.equalsIgnoreCase(ClientConstants.ADMIT_CLAIM_HISTORY)
                 || outputTableName.equalsIgnoreCase(ClientConstants.ADMIT_CLAIM_HISTORY_CLIENT)
                 || outputTableName.equalsIgnoreCase(ClientConstants.ADMIT_CLAIM_HISTORY_POLICY)
                 || outputTableName.equalsIgnoreCase(ClientConstants.ADMIT_CLAIM_ADDITIONAL_FIELD)
                 || outputTableName.equalsIgnoreCase(ClientConstants.ADMIT_CLAIM_HISTORY_PAYMENT);
     }

    private String resolveClaimOutputTableName(final String logicalTableName, final boolean isAdmitFlow) {
        final String baseOutputTableName = resolveConfiguredOutputTableName(logicalTableName);
        if (!isAdmitFlow || !FILTER_ONLY_NON_ADMIT || isBlank(baseOutputTableName)) {
            return baseOutputTableName;
        }
        if (!baseOutputTableName.toUpperCase(Locale.ROOT).startsWith("CLAIM")) {
            return baseOutputTableName;
        }
        return resolveAdmitSheetName(baseOutputTableName);
    }

    private String resolveAdmitSheetName(final String baseOutputTableName) {
        if (isBlank(baseOutputTableName)) return "";
        return switch (baseOutputTableName.toUpperCase(Locale.ROOT)) {
            case ClientConstants.CLAIM_HISTORY -> ClientConstants.ADMIT_CLAIM_HISTORY;
            case ClientConstants.CLAIM_HISTORY_CLIENT_TABLE -> ClientConstants.ADMIT_CLAIM_HISTORY_CLIENT;
            case ClientConstants.CLAIM_HISTORY_POLICY_TABLE -> ClientConstants.ADMIT_CLAIM_HISTORY_POLICY;
            case ClientConstants.CLAIM_ADDITIONAL_FIELD_TABLE -> ClientConstants.ADMIT_CLAIM_ADDITIONAL_FIELD;
            case ClientConstants.CLAIM_HISTORY_PAYMENT_TABLE -> ClientConstants.ADMIT_CLAIM_HISTORY_PAYMENT;
            default -> ADMIT_PREFIX + baseOutputTableName;
        };
    }

    private boolean isParentGuidRefColumn(final TableDefinition tableDef, final TableMappingConfiguration.ColumnDefinition columnDef) {
        if (tableDef.getParent() == null || tableDef.getParentGuidRef() == null) return false;
        if (columnDef == null || columnDef.getIdentifiers() == null) return false;
        return columnDef.getIdentifiers().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .anyMatch(id -> id.equalsIgnoreCase(tableDef.getParentGuidRef()));
    }

    private boolean isRootGuidRefColumn(final TableDefinition tableDef, final TableMappingConfiguration.ColumnDefinition columnDef) {
        if (tableDef.getRootGuidRef() == null) return false;
        if (columnDef == null || columnDef.getIdentifiers() == null) return false;
        return columnDef.getIdentifiers().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .anyMatch(id -> id.equalsIgnoreCase(tableDef.getRootGuidRef()));
    }

    private boolean isGuidIdentifierColumn(final TableDefinition tableDef, final TableMappingConfiguration.ColumnDefinition columnDef, final String currentTableGuid) {
        if (currentTableGuid == null || tableDef.getGuidColumn() == null || columnDef == null || columnDef.getIdentifiers() == null) return false;
        return columnDef.getIdentifiers().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .anyMatch(identifier -> identifier.equalsIgnoreCase(tableDef.getGuidColumn()));
    }

    /**
     * Prepares or retrieves GUID for a table in current processing context.
     */
    private String prepareGuidForTable(final String tableName, final TableDefinition tableDef, final Map<String, String> guidContext) {
        if (tableDef.getGuidColumn() == null) return null;
        return guidContext.computeIfAbsent(tableName, ignored -> UUID.randomUUID().toString());
    }

    private void discardPreparedGuidIfUnused( final String tableName, final String preparedGuid, final Map<String, String> guidContext) {
        if (preparedGuid == null) return;
        final String current = guidContext.get(tableName);
        if (preparedGuid.equals(current)) {
            guidContext.remove(tableName);
        }
    }

    /**
     * Assigns GUID to row if required by table configuration.
     */
    private Map<String, String> assignGuidIfRequired(final String tableName, final TableDefinition tableDef, final Map<String, String> rowData, final Map<String, String> guidContext) {
        if (tableDef.getGuidColumn() == null) return rowData;
        final String guid = guidContext.computeIfAbsent(tableName, ignored -> UUID.randomUUID().toString());
        final Map<String, String> ordered = new LinkedHashMap<>();
        ordered.put(tableDef.getGuidColumn(), guid);
        ordered.putAll(rowData);
        log.debug("Generated GUID {} for table '{}'", guid, tableName);
        return ordered;
    }

    /**
     * Skips coverage/benefit rows if required business fields are missing.
     */
    private boolean shouldSkipCoverageOrBenefitRow(final String outputTableName, final Map<String, String> rowData) {
        if (ClientConstants.CLAIM_HISTORY_COVERAGE_TABLE.equalsIgnoreCase(outputTableName)) {
            return emptyIfNull(rowData.get(ClientConstants.PLAN_CD)).isBlank();
        }
        if (ClientConstants.CLAIM_HISTORY_COV_BENEFIT_TABLE.equalsIgnoreCase(outputTableName)) {
            return emptyIfNull(rowData.get(ClientConstants.BENEFIT_CD)).isBlank();
        }
        return false;
    }

    private void normalizeClaimHistoryCauseOfDeathCd(final String outputTableName, final Map<String, String> rowData) {
        if (!ClientConstants.CLAIM_HISTORY.equalsIgnoreCase(outputTableName)) return;
        final String currentValue = emptyIfNull(rowData.get(ClientConstants.CLAIM_HISTORY_CAUSE_OF_DEATH_CD)).trim();
        if (ClientConstants.NATURAL.equalsIgnoreCase(currentValue)) {
            rowData.put(ClientConstants.CLAIM_HISTORY_CAUSE_OF_DEATH_CD, ClientConstants.NATURAL);
        } else if ("ACCIDENTAL".equalsIgnoreCase(currentValue)) {
            rowData.put(ClientConstants.CLAIM_HISTORY_CAUSE_OF_DEATH_CD, "UNNATURAL");
        }
        final String eventSubTypeValue = emptyIfNull(rowData.get(ClientConstants.CLAIM_HISTORY_EVENT_SUB_TYPE_CD)).trim();
        if ("NATURAL".equalsIgnoreCase(eventSubTypeValue)) {
            rowData.put(ClientConstants.CLAIM_HISTORY_EVENT_SUB_TYPE_CD, "NATURAL");
        } else if ("ACCIDENTAL".equalsIgnoreCase(eventSubTypeValue)) {
            rowData.put(ClientConstants.CLAIM_HISTORY_EVENT_SUB_TYPE_CD, "UNNATURAL");
        }
    }

     private void normalizeClaimHistoryStageAndStatusCd(final String outputTableName, final Map<String, String> rowData, final CSVRecord csvRecord) {
         if (!isClaimHistoryTable(outputTableName)) return;
         final String decision = readCsvValueSafely(csvRecord, "TPCR_CLAIM_DECSN");
         if ("admit".equalsIgnoreCase(decision)) {
             rowData.put("stageCd", ClientConstants.PAID_STATUS);
             rowData.put(ClientConstants.STATUS_CD, ClientConstants.PAID_STATUS);
         } else {
             rowData.put("stageCd", ClientConstants.PENDING_MANUAL_ADJ);
             rowData.put("statusCd", ClientConstants.DE_COMPLETE_STATUS);
         }
     }

     private boolean isClaimHistoryTable(final String outputTableName) {
         return ClientConstants.CLAIM_HISTORY.equalsIgnoreCase(outputTableName)
                 || ClientConstants.ADMIT_CLAIM_HISTORY.equalsIgnoreCase(outputTableName);
     }

    private boolean isBlank(String val) {
        return val == null || val.isBlank();
    }

    private boolean isEmpty(List<?> list) {
        return list == null || list.isEmpty();
    }

    private Map<String, String> toMap(List<Map<String, String>> rows, String key, String value) {
        if (rows == null) return Map.of();
        return rows.stream()
                .filter(r -> !isBlank(r.get(key)) && !isBlank(r.get(value)))
                .collect(Collectors.toMap(
                        r -> r.get(key),
                        r -> r.get(value),
                        (a, b) -> a
                ));
    }

     private void addTaskSheet(Map<String, List<Map<String, String>>> tableData) {
         List<Map<String, String>> claimRows = new ArrayList<>();
         claimRows.addAll(tableData.getOrDefault(ClientConstants.CLAIM_HISTORY, Collections.emptyList()));
         claimRows.addAll(tableData.getOrDefault(ClientConstants.ADMIT_CLAIM_HISTORY, Collections.emptyList()));
         
         List<Map<String, String>> taskRows = claimRows.stream()
                 .filter(row -> {
                     String status = statusByClaimGuid.get(row.get("claimGuid"));
                     return !(ClientConstants.ADMIT.equalsIgnoreCase(status) || "REJ".equalsIgnoreCase(status));
                 })
                 .map(row -> buildConfiguredDerivedRow(ClientConstants.TASK_TABLE, null,
                         Map.of(
                                 "taskGuid", UUID.randomUUID().toString(),
                                 "claimhistoryrefguid", row.get("claimGuid")
                         )
                 ))
                 .toList();
         if (!isEmpty(taskRows)) {
             tableData.put(ClientConstants.TASK_TABLE, taskRows);
         }
     }

}


