package com.zeotap.dataingestion.controller;

import com.opencsv.exceptions.CsvValidationException;
import com.zeotap.dataingestion.model.FlatFileConfig;
import com.zeotap.dataingestion.model.TableInfo;
import com.zeotap.dataingestion.service.FlatFileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class FileController {

    private final FlatFileService flatFileService;
    
    @Value("${app.upload.dir}")
    private String uploadDir;
    
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> uploadFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam("delimiter") String delimiter,
            @RequestParam("hasHeader") boolean hasHeader) {
        
        Map<String, Object> response = new HashMap<>();
        
        try {
            FlatFileConfig config = new FlatFileConfig();
            config.setDelimiter(delimiter);
            config.setHasHeader(hasHeader);
            
            TableInfo tableInfo = flatFileService.processUploadedFile(file, config);
            
            response.put("success", true);
            response.put("message", "File uploaded successfully");
            response.put("filePath", config.getFilePath());
            response.put("schema", tableInfo);
            
            return ResponseEntity.ok(response);
        } catch (IOException | CsvValidationException e) {
            log.error("Error processing uploaded file", e);
            response.put("success", false);
            response.put("message", "Error processing file: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    @GetMapping("/download/{fileName:.+}")
    public ResponseEntity<Resource> downloadFile(@PathVariable String fileName) {
        try {
            Path filePath = Paths.get(uploadDir).resolve(fileName);
            Resource resource = new UrlResource(filePath.toUri());
            
            if (resource.exists() && resource.isReadable()) {
                return ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
                        .body(resource);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (MalformedURLException e) {
            log.error("Error downloading file", e);
            return ResponseEntity.badRequest().build();
        }
    }
} 