package com.trading;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@EnableScheduling
public class DataFetcherApplication {

    private static final Logger logger = LoggerFactory.getLogger(DataFetcherApplication.class);

    public static void main(String[] args) {
        logger.info("########################## START APPLICATION ##########################");
        // Determine folder where the JAR is located and set app.home so logging/config are relative to it
        try {
            java.net.URI uri = DataFetcherApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            java.io.File jarFile = new java.io.File(uri);
            String jarDir = jarFile.isDirectory() ? jarFile.getAbsolutePath() : jarFile.getParentFile().getAbsolutePath();
            System.setProperty("app.home", jarDir);
            logger.info("Set app.home={} (based on JAR location)", jarDir);
        } catch (Exception e) {
            // fallback to working directory
            String cwd = System.getProperty("user.dir");
            System.setProperty("app.home", cwd);
            logger.warn("Failed to determine JAR location, using working dir as app.home={}", cwd);
        }

        SpringApplication.run(DataFetcherApplication.class, args);
    }
}
