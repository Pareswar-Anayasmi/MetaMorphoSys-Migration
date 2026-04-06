package com.metanorph.migration.service;

import org.apache.poi.ss.usermodel.Workbook;
import java.io.InputStream;

public interface CsvProcessingService {

    Workbook processFile(String fileName, InputStream inputStream);

    Workbook processConfiguredTable();
}
