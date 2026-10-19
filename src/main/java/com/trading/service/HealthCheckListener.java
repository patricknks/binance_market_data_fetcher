package com.trading.service;

import com.trading.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class HealthCheckListener {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckListener.class);
    private final AppConfig appConfig;
    public static final AtomicLong lastMainLoopHeartbeat = new AtomicLong(System.currentTimeMillis());

    public HealthCheckListener(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startHealthCheckListener() {
        Thread listenerThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(appConfig.getHealthcheck().getPort())) {
                logger.info("[HealthCheck] Listening on port {}", appConfig.getHealthcheck().getPort());

                while (true) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        long now = System.currentTimeMillis();
                        byte[] response;

                        if (now - lastMainLoopHeartbeat.get() > 10000) {
                            response = "FAIL\n".getBytes();
                        } else {
                            response = "OK\n".getBytes();
                        }

                        clientSocket.getOutputStream().write(response);
                        clientSocket.getOutputStream().flush();
                        clientSocket.close();
                    } catch (IOException e) {
                        logger.error("[HealthCheck] Exception: {}", e.getMessage());
                    }
                }
            } catch (IOException e) {
                logger.error("[HealthCheck] Failed to bind: {}", e.getMessage());
            }
        });

        listenerThread.setDaemon(true);
        listenerThread.setName("HealthCheckListener");
        listenerThread.start();
    }

    public static void updateHeartbeat() {
        lastMainLoopHeartbeat.set(System.currentTimeMillis());
    }
}
