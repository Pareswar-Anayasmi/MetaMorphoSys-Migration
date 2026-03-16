package com.metanorph.migration.controller;

import com.metanorph.migration.service.CsvProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * REST controller that accepts a CSV file upload and returns a processed Excel workbook.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/migration")
@CrossOrigin("*")
public class CsvUploadController {

    private static final MediaType EXCEL_MEDIA_TYPE =
            MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    private final CsvProcessingService csvProcessingService;

    /**
     * Accepts a multipart CSV file, processes it, and streams back an Excel (.xlsx) file.
     *
     * @param file the uploaded CSV file
     * @return a downloadable Excel file as a streaming response
     * @throws IOException if reading the upload or writing the workbook fails
     */
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<InputStreamResource> uploadCsv(
            @RequestParam("file") final MultipartFile file) throws IOException {

        log.info("CSV upload request received. File name: {}", file.getOriginalFilename());

        final byte[] excelBytes = buildExcelBytes(file);

        log.info("Excel file generated successfully");

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=migration_output.xlsx")
                .contentType(EXCEL_MEDIA_TYPE)
                .body(new InputStreamResource(new ByteArrayInputStream(excelBytes)));
    }

    /**
     * Reads the CSV, delegates processing, and serialises the resulting workbook to a byte array.
     * The workbook is closed in a try-with-resources block to prevent resource leaks.
     */
    private byte[] buildExcelBytes(final MultipartFile file) throws IOException {

        try (final InputStreamReader csvReader   = new InputStreamReader(file.getInputStream());
             final Workbook           workbook   = csvProcessingService.processCsv(csvReader);
             final ByteArrayOutputStream buffer  = new ByteArrayOutputStream()) {

            workbook.write(buffer);
            return buffer.toByteArray();
        }
    }
}
