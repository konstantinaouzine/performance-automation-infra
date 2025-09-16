package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.MathContext;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class LoadController {
    private static final Logger log = LoggerFactory.getLogger(LoadController.class);
    private final SecureRandom random = new SecureRandom();

    /*
     * Endpoint to generate CPU and memory load for POC performance testing.
     * Parameters:
     *   cpuMillis   - approximate time (ms) to keep CPU busy (default 500)
     *   memMB       - approximate memory to allocate in megabytes (default 50)
     *   complexity  - math loop complexity multiplier (default 1000)
     */
    @GetMapping("/load")
    public String generateLoad(@RequestParam(name = "cpuMillis", defaultValue = "500") long cpuMillis,
                               @RequestParam(name = "memMB", defaultValue = "50") int memMB,
                               @RequestParam(name = "complexity", defaultValue = "1000") int complexity) {
        long start = System.currentTimeMillis();

        // Memory pressure
        List<byte[]> blocks = new ArrayList<>();
        int bytes = memMB * 1024 * 1024;
        int chunk = 1 * 1024 * 1024; // 1MB chunks
        int allocated = 0;
        while (allocated < bytes) {
            blocks.add(new byte[Math.min(chunk, bytes - allocated)]);
            allocated += chunk;
        }

        // CPU intensive math loop
        MathContext mc = new MathContext(50);
        BigDecimal value = BigDecimal.valueOf(random.nextDouble() + 1.0);
        while (System.currentTimeMillis() - start < cpuMillis) {
            for (int i = 0; i < complexity; i++) {
                double r = ThreadLocalRandom.current().nextDouble() + 1.000001d;
                value = value.multiply(new BigDecimal(r, mc), mc)
                        .sqrt(mc)
                        .add(new BigDecimal(Math.log(r + 1.0), mc), mc)
                        .remainder(new BigDecimal(r + 2.0, mc), mc);
            }
        }

        // Prevent GC optimizing away
        int hash = value.hashCode() ^ blocks.size();
        long elapsed = System.currentTimeMillis() - start;
        log.info("Generated load: cpuMillis={} memMB={} complexity={} elapsed={}ms hash={}", cpuMillis, memMB, complexity, elapsed, hash);
        return "OK cpuMillis=" + cpuMillis + " memMB=" + memMB + " complexity=" + complexity + " elapsed=" + elapsed + "ms";
    }
}
