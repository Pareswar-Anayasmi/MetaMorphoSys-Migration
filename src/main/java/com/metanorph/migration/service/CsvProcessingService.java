package com.metanorph.migration.service;

import org.apache.poi.ss.usermodel.Workbook;
import java.io.Reader;

public interface CsvProcessingService {

    Workbook processCsv(Reader reader) ;
}
