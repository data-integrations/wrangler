package com.example.clickhouse.controller;

import com.example.clickhouse.service.DataIngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/ingestion")
public class DataIngestionController {

    @Autowired
    private DataIngestionService dataIngestionService;

    @GetMapping
    public String showIngestionPage() {
        return "ingestion";
    }

    @PostMapping("/clickhouse-to-file")
    public String clickhouseToFile(@RequestParam String tableName,
                                 @RequestParam String columns,
                                 @RequestParam String fileFormat,
                                 Model model) {
        try {
            List<String> columnList = Arrays.asList(columns.split(","));
            int recordCount = dataIngestionService.exportFromClickHouse(tableName, columnList, fileFormat);
            model.addAttribute("message", "Successfully exported " + recordCount + " records");
        } catch (Exception e) {
            model.addAttribute("error", "Error during export: " + e.getMessage());
        }
        return "ingestion";
    }

    @PostMapping("/clickhouse-to-file/join")
    public String clickhouseToFileWithJoin(@RequestParam String tableNames,
                                         @RequestParam String columns,
                                         @RequestParam String joinConditions,
                                         @RequestParam String fileFormat,
                                         Model model) {
        try {
            List<String> tableNameList = Arrays.asList(tableNames.split(","));
            List<String> columnList = Arrays.asList(columns.split(","));
            List<String> joinConditionList = Arrays.asList(joinConditions.split(";"));
            int recordCount = dataIngestionService.exportFromClickHouseWithJoin(
                tableNameList, columnList, joinConditionList, fileFormat);
            model.addAttribute("message", "Successfully exported " + recordCount + " records");
        } catch (Exception e) {
            model.addAttribute("error", "Error during export: " + e.getMessage());
        }
        return "ingestion";
    }

    @GetMapping("/preview")
    @ResponseBody
    public ResponseEntity<?> previewData(@RequestParam String tableName,
                                       @RequestParam String columns,
                                       @RequestParam(defaultValue = "100") int limit) {
        try {
            List<String> columnList = Arrays.asList(columns.split(","));
            List<Map<String, Object>> data = dataIngestionService.previewData(
                tableName, columnList, limit);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Error during preview: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/preview/join")
    @ResponseBody
    public ResponseEntity<?> previewDataWithJoin(@RequestParam String tableNames,
                                               @RequestParam String columns,
                                               @RequestParam String joinConditions,
                                               @RequestParam(defaultValue = "100") int limit) {
        try {
            List<String> tableNameList = Arrays.asList(tableNames.split(","));
            List<String> columnList = Arrays.asList(columns.split(","));
            List<String> joinConditionList = Arrays.asList(joinConditions.split(";"));
            List<Map<String, Object>> data = dataIngestionService.previewDataWithJoin(
                tableNameList, columnList, joinConditionList, limit);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Error during preview: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/file-to-clickhouse")
    public String fileToClickhouse(@RequestParam MultipartFile file,
                                 @RequestParam String tableName,
                                 @RequestParam String columns,
                                 Model model) {
        try {
            List<String> columnList = Arrays.asList(columns.split(","));
            int recordCount = dataIngestionService.importToClickHouse(file, tableName, columnList);
            model.addAttribute("message", "Successfully imported " + recordCount + " records");
        } catch (Exception e) {
            model.addAttribute("error", "Error during import: " + e.getMessage());
        }
        return "ingestion";
    }
} 