package com.example.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class TestTableController {
    private final TestTableRepository repository;

    public TestTableController(TestTableRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/test_table")
    public List<TestTable> getTestTable() {
        return repository.findAll();
    }
}
