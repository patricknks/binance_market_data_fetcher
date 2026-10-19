package com.trading.service;

import com.trading.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

@Service
public class ResourceMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ResourceMonitor.class);
    private final AppConfig appConfig;
    private final MemoryMXBean memoryMXBean;

    public ResourceMonitor(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    public void sampleResourcesAndMaybeExit() {
        if (appConfig.getMemoryLimitMb() <= 0) {
            return;
        }

        try {
            long heapMax = memoryMXBean.getHeapMemoryUsage().getMax();
            long heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed();
            long heapUsedMb = heapUsed / (1024 * 1024);
            long maxMb = heapMax / (1024 * 1024);

            logger.debug("Resource sample: Heap Memory Used={} MB / {} MB", heapUsedMb, maxMb);

            if (heapUsedMb > appConfig.getMemoryLimitMb()) {
                logger.error("Memory threshold exceeded: {:.1f} MB > {} MB. Exiting for supervisor restart.", 
                        (double) heapUsedMb, appConfig.getMemoryLimitMb());
                System.exit(99);
            }
        } catch (Exception e) {
            logger.warn("Resource sampling failed: {}", e.getMessage());
        }
    }

    public void forceGarbageCollection() {
        try {
            System.gc();
            logger.debug("Forced GC collection executed");
        } catch (Exception e) {
            logger.warn("GC collection failed: {}", e.getMessage());
        }
    }
}
