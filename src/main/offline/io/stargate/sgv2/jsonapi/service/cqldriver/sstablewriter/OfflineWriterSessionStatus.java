package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

/**
 * The status of the offline writer session. Apart from the user provided configurations, this has additional information such as the data directory size, the number of inserts that succeeded and failed, etc.
 */
public record OfflineWriterSessionStatus(
    String sessionId,
    String keyspace,
    String tableName,
    String ssTableOutputDirectory,
    int fileWriterBufferSizeInMB,
    long dataDirectorySizeInBytes,
    int insertsSucceeded,
    int insertsFailed) {}
