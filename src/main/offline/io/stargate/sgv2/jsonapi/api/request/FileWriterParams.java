package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import java.util.List;

/**
 * The parameters that are required to write the data to SSTable files used primarily by {@link
 * FileWriterSession}
 */
public record FileWriterParams(
    /* The name of the keyspace */
    String keyspaceName,
    /* The name of the table */
    String tableName,
    /* The directory where the SSTable files will be written */
    String ssTableOutputDirectory,
    /* The size of the buffer used to write the SSTable files */
    int fileWriterBufferSizeInMB,
    /* The CQL to create the table */
    String createTableCQL,
    /* The CQL to insert the data into the table */
    String insertStatementCQL,
    /* The CQL to create the indexes */
    List<String> indexCQLs,
    /* The flag to enable vector */
    Boolean vectorEnabled) {
  public FileWriterParams {
    if (keyspaceName == null || keyspaceName.isBlank()) {
      throw new IllegalArgumentException("Invalid FileWriterParams, keyspaceName is null or empty");
    }
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("Invalid FileWriterParams, tableName is null or empty");
    }
    if (ssTableOutputDirectory == null || ssTableOutputDirectory.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, ssTableOutputDirectory is null or empty");
    }
    if (fileWriterBufferSizeInMB <= 0) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, fileWriterBufferSizeInMB is less than or equal to 0");
    }
    if (createTableCQL == null || createTableCQL.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, createTableCQL is null or empty");
    }
    if (insertStatementCQL == null || insertStatementCQL.isBlank()) {
      throw new IllegalArgumentException(
          "Invalid FileWriterParams, insertStatementCQL is null or empty");
    }
    if (vectorEnabled == null) {
      throw new IllegalArgumentException("Invalid FileWriterParams, vectorEnabled is null");
    }
  }
}
