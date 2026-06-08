package io.stargate.sgv2.jsonapi.util;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LoggerTestWrapper implements AutoCloseable {

    private final java.util.logging.Logger targetLogger;
    private final java.util.logging.Level previoiusLevel;
    private final java.util.logging.Handler memoryHandler;
    public final List<LogRecord> records = new ArrayList<>();

    public LoggerTestWrapper(Class<?> clazz){
        this(clazz, Level.FINEST);
    }
    public LoggerTestWrapper(Class<?> clazz, java.util.logging.Level newLevel) {

        this.targetLogger = java.util.logging.Logger.getLogger(clazz.getName());
        this.previoiusLevel = targetLogger.getLevel();
        targetLogger.setLevel(newLevel);

        this.memoryHandler = new java.util.logging.Handler() {
            public void publish(java.util.logging.LogRecord r) { records.add(r); }
            public void flush() {}
            public void close() {}
        };
        this.memoryHandler.setLevel(newLevel);
        targetLogger.addHandler(memoryHandler);
    }

    public List<LogRecord> logRecords() {
        return records;
    }

    public List<String> logMessages(){
        return records.stream().map(LogRecord::getMessage).toList();
    }

    @Override
    public void close() {
        targetLogger.setLevel(previoiusLevel);
        targetLogger.removeHandler(memoryHandler);
    }
}
