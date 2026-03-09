package com.metanorph.migration.util;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.util.List;
import java.util.Map;

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

            Row headerRow = sheet.createRow(0);

            int columnIndex = 0;

            for (String column : rows.get(0).keySet()) {

                headerRow.createCell(columnIndex++)
                        .setCellValue(column);
            }

            int rowIndex = 1;

            for (Map<String, String> rowData : rows) {

                Row row = sheet.createRow(rowIndex++);

                int cellIndex = 0;

                for (String value : rowData.values()) {

                    row.createCell(cellIndex++)
                            .setCellValue(value);
                }
            }
        });

        return workbook;
    }
}
