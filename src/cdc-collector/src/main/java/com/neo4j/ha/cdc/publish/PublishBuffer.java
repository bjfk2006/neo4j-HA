package com.neo4j.ha.cdc.publish;

import com.neo4j.ha.common.model.ChangeEvent;
import com.neo4j.ha.common.serialization.EventSerializer;
import com.neo4j.ha.common.serialization.EventDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;

public class PublishBuffer {

    private static final Logger log = LoggerFactory.getLogger(PublishBuffer.class);
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");
    private static final long MAX_FILE_BYTES = 10 * 1024 * 1024; // 10MB per file

    private final ConcurrentLinkedQueue<ChangeEvent> memoryBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicLong bufferSize = new AtomicLong(0);
    private final EventSerializer serializer;
    private final EventDeserializer deserializer;
    private final Path bufferDir;
    private final int maxFiles;

    public PublishBuffer(EventSerializer serializer, String bufferDir, int maxFiles) {
        this.serializer = serializer;
        this.deserializer = new EventDeserializer();
        this.bufferDir = Path.of(bufferDir);
        this.maxFiles = maxFiles;
        try {
            Files.createDirectories(this.bufferDir);
        } catch (IOException e) {
            log.warn("Failed to create buffer directory: {}", bufferDir, e);
        }
    }

    public void add(List<ChangeEvent> events) {
        memoryBuffer.addAll(events);
        bufferSize.addAndGet(events.size());
        log.warn("Buffered {} events (total: {}), Redis may be unavailable", events.size(), bufferSize.get());

        if (bufferSize.get() > 10000) {
            flushToFile();
        }
    }

    public boolean hasBuffered() {
        return !memoryBuffer.isEmpty() || hasBufferedOnDisk();
    }

    public boolean hasBufferedOnDisk() {
        File[] files = bufferDir.toFile().listFiles((dir, name) -> name.endsWith(".jsonl"));
        return files != null && files.length > 0;
    }

    public long size() {
        return bufferSize.get();
    }

    public List<ChangeEvent> drain(int maxCount) {
        List<ChangeEvent> batch = new ArrayList<>();

        // First drain from disk (oldest first) to preserve ordering
        if (memoryBuffer.isEmpty() && hasBufferedOnDisk()) {
            batch.addAll(replayFromDisk(maxCount));
        }

        // Then drain from memory
        int remaining = maxCount - batch.size();
        for (int i = 0; i < remaining; i++) {
            ChangeEvent event = memoryBuffer.poll();
            if (event == null) break;
            batch.add(event);
        }

        bufferSize.addAndGet(-batch.size());
        return batch;
    }

    /**
     * Reads events from the oldest buffer file on disk. Deletes the file after reading.
     */
    private List<ChangeEvent> replayFromDisk(int maxCount) {
        File[] files = bufferDir.toFile().listFiles((dir, name) -> name.endsWith(".jsonl"));
        if (files == null || files.length == 0) return List.of();

        Arrays.sort(files);
        List<ChangeEvent> events = new ArrayList<>();

        File oldest = files[0];
        try (BufferedReader reader = Files.newBufferedReader(oldest.toPath())) {
            String line;
            while ((line = reader.readLine()) != null && events.size() < maxCount) {
                if (line.isBlank()) continue;
                try {
                    ChangeEvent event = deserializer.fromJson(line);
                    events.add(event);
                } catch (Exception e) {
                    log.warn("Skipping corrupt buffer line in {}: {}", oldest.getName(), e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("Failed to read buffer file: {}", oldest.getName(), e);
            return events;
        }

        try {
            Files.delete(oldest.toPath());
            log.info("Replayed {} events from disk buffer {}", events.size(), oldest.getName());
        } catch (IOException e) {
            log.warn("Failed to delete buffer file after replay: {}", oldest.getName(), e);
        }

        return events;
    }

    private void flushToFile() {
        enforceMaxFiles();

        String filename = "buffer-" + LocalDateTime.now().format(TS_FMT) + ".jsonl";
        Path filePath = bufferDir.resolve(filename);
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            ChangeEvent event;
            int count = 0;
            long bytesWritten = 0;
            while ((event = memoryBuffer.poll()) != null && count < 10000) {
                String json = serializer.toJson(event);
                if (bytesWritten + json.length() > MAX_FILE_BYTES) {
                    memoryBuffer.add(event);
                    break;
                }
                writer.write(json);
                writer.newLine();
                bytesWritten += json.length() + 1;
                bufferSize.decrementAndGet();
                count++;
            }
            log.info("Flushed {} events to {}", count, filePath);
        } catch (IOException e) {
            log.error("Failed to flush buffer to file", e);
        }
    }

    private void enforceMaxFiles() {
        File[] files = bufferDir.toFile().listFiles((dir, name) -> name.endsWith(".jsonl"));
        if (files == null) return;
        if (files.length >= maxFiles) {
            Arrays.sort(files);
            int toRemove = files.length - maxFiles + 1;
            for (int i = 0; i < toRemove; i++) {
                long lost = countLines(files[i]);
                try {
                    Files.delete(files[i].toPath());
                    bufferSize.addAndGet(-lost);
                    log.warn("Buffer maxFiles reached, discarded oldest file {} ({} events lost)",
                        files[i].getName(), lost);
                } catch (IOException e) {
                    log.error("Failed to delete overflow buffer file: {}", files[i].getName(), e);
                }
            }
        }
    }

    private static long countLines(File file) {
        try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
            long count = 0;
            while (reader.readLine() != null) count++;
            return count;
        } catch (IOException e) {
            return 0;
        }
    }
}
