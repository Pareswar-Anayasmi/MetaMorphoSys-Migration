package com.metanorph.migration.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public final class ExcelInputToCsvUtil {

    private ExcelInputToCsvUtil() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static String convertFirstSheetToCsv(final InputStream inputStream) throws IOException {
        try (Workbook workbook = WorkbookFactory.create(inputStream);
             StringWriter out = new StringWriter();
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT)) {

            final Sheet sheet = workbook.getNumberOfSheets() > 0 ? workbook.getSheetAt(0) : null;
            if (sheet == null) {
                return "";
            }

            final DataFormatter formatter = new DataFormatter();
            final FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

            final Row headerRow = sheet.getRow(sheet.getFirstRowNum());
            if (headerRow == null) {
                return "";
            }

            final int lastCellIndexExclusive = headerRow.getLastCellNum();
            if (lastCellIndexExclusive <= 0) {
                return "";
            }

            for (int columnIndex = 0; columnIndex < lastCellIndexExclusive; columnIndex++) {
                printer.print(readCell(headerRow, columnIndex, formatter, evaluator));
            }
            printer.println();

            for (int rowIndex = sheet.getFirstRowNum() + 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                final Row row = sheet.getRow(rowIndex);
                for (int columnIndex = 0; columnIndex < lastCellIndexExclusive; columnIndex++) {
                    printer.print(readCell(row, columnIndex, formatter, evaluator));
                }
                printer.println();
            }

            printer.flush();
            return out.toString();
        }
    }

    private static String readCell(
            final Row row,
            final int columnIndex,
            final DataFormatter formatter,
            final FormulaEvaluator evaluator) {

        if (row == null) {
            return "";
        }

        final Cell cell = row.getCell(columnIndex, MissingCellPolicy.RETURN_BLANK_AS_NULL);
        if (cell == null) {
            return "";
        }

        return formatter.formatCellValue(cell, evaluator).trim();
    }
}

