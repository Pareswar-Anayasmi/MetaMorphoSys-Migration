package com.metanorph.migration.controller;

import com.metanorph.migration.service.CsvProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/migration")
@CrossOrigin("*")
public class CsvUploadController {

    private final CsvProcessingService csvProcessingService;

    @PostMapping( value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE )
    public ResponseEntity<InputStreamResource> uploadCsv( @RequestParam("file") MultipartFile file) throws IOException {

        log.info("CSV upload request received. File name: {}", file.getOriginalFilename());

        Workbook workbook = csvProcessingService.processCsv( new InputStreamReader(file.getInputStream()));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        workbook.write(outputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());

        log.info("Excel file generated successfully");
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=migration_output.xlsx")
                .contentType(MediaType.parseMediaType(
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(new InputStreamResource(inputStream));
    }
}
