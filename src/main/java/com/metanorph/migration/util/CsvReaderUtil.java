package com.metanorph.migration.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import java.io.IOException;
import java.io.Reader;

public class CsvReaderUtil {

    private CsvReaderUtil() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static CSVParser parse(final Reader reader) throws IOException {

        return CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .build()
                .parse(reader);
    }
}
