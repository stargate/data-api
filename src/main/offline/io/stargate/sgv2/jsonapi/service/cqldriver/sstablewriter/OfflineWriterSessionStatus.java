package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

public record OfflineWriterSessionStatus(
    String sessionId,
    String keyspace,
    String tableName,
    String ssTableOutputDirectory,
    int fileWriterBufferSizeInMB,
    long dataDirectorySizeInBytes,
    int insertsSucceeded,
    int insertsFailed) {}
