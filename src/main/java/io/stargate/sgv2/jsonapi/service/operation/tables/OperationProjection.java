package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSource;


/**
 * POC of what pushing the projection down looks like.
 *
 * The idea is to encapsulate both what columns we pull from the table & how we then convert a row
 * we read into a document into this one interface to a read operation can hand it all off.
 *
 * See {@link AllJSONProjection} for a POC that is how the initial Tables POC works
 */
public interface OperationProjection {

  /**
   * Called by an operation when it wants the projection to add the columns it will select from the
   * database to the {@link Select} from the Query builder.
   *
   * Implementations should add the columns they need by name from their internal state. The projection
   * should already have been valided as valid to run against the table, all the columns in the projection
   * should exist in the table.
   *
   * TODO: the select param should be a Select type, is only a SelectFrom because that is where the
   * builder has json(), will change to select when we stop doing that. See AllJSONProjection
   * @param select
   * @return
   */
  Select forSelect(SelectFrom select);

  /**
   * Called by an opertion when it wants to get a {@link DocumentSource} implementation that when later called,
   * will be able to convert the provided {@link Row} into a document to return to the user.
   *
   * Note: Implementations should not immediately create a JSON document, it should return an object that
   * defers creating the document until asked. Defering the document creation allows the operation to
   * be more efficient by only creating the document if it is needed.
   *
   * Implementations should use the {@link io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} to
   * map the columns in the row to the fields in the document.
   *
   * @param row
   * @return
   */
  DocumentSource toDocument(Row row);

}
