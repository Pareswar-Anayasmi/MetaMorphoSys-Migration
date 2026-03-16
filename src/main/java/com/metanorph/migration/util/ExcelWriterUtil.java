package com.metanorph.migration.util;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public final class ExcelWriterUtil {

    private ExcelWriterUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    public static Workbook write(Map<String, List<Map<String, String>>> tableData) {

        Workbook workbook = new XSSFWorkbook();

        tableData.forEach((sheetName, rows) -> {

            Sheet sheet = workbook.createSheet(sheetName);

            if (rows.isEmpty()) {
                return;
            }

            List<String> headers = resolveHeaders(rows);

            Row headerRow = sheet.createRow(0);
            for (int columnIndex = 0; columnIndex < headers.size(); columnIndex++) {
                headerRow.createCell(columnIndex).setCellValue(headers.get(columnIndex));
            }

            int rowIndex = 1;
            for (Map<String, String> rowData : rows) {
                Row row = sheet.createRow(rowIndex++);

                for (int columnIndex = 0; columnIndex < headers.size(); columnIndex++) {
                    String header = headers.get(columnIndex);
                    String value = rowData.getOrDefault(header, "");
                    row.createCell(columnIndex).setCellValue(value);
                }
            }
        });

        return workbook;
    }

    private static List<String> resolveHeaders(List<Map<String, String>> rows) {
        Map<String, Boolean> seen = new LinkedHashMap<>();

        for (Map<String, String> rowData : rows) {
            for (String key : rowData.keySet()) {
                seen.putIfAbsent(key, Boolean.TRUE);
            }
        }

        return new ArrayList<>(seen.keySet());
    }
}
