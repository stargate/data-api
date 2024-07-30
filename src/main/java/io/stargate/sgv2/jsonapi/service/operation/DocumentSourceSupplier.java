package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * POC of what pushing the projection down looks like.
 *
 * <p>The idea is to encapsulate both what columns we pull from the table & how we then convert a
 * row we read into a document into this one interface to a read operation can hand it all off.
 */
public interface DocumentSourceSupplier {

  /**
   * Called by an operation when it wants to get a {@link DocumentSource} implementation that when
   * later called, will be able to convert the provided {@link Row} into a document to return to the
   * user.
   *
   * <p>Note: Implementations should not immediately create a JSON document, it should return an
   * object that defers creating the document until asked. Deferring the document creation allows
   * the operation to be more efficient by only creating the document if it is needed.
   *
   * <p>Implementations should use the {@link
   * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} to map the
   * columns in the row to the fields in the document.
   *
   * @param row
   * @return
   */
  DocumentSource documentSource(Row row);
}
