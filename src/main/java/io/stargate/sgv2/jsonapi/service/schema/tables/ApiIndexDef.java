package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Map;

/**
 * The internal API definition of an Index.
 *
 * <p>Is an interface so the unsupported indexss can be represented as easily.
 */
public interface ApiIndexDef extends PrettyPrintable {

  /** The name of the index in the database. */
  CqlIdentifier indexName();

  /**
   * The target column the index is on.
   *
   * <p>Code should not assume a column can only have one index.
   */
  CqlIdentifier targetColumn();

  /** The type of index from the API perspective. */
  ApiIndexType indexType();

  /** How to describe this index in the public HTTP API. */
  IndexDesc<?> indexDesc();

  /**
   * Raw CQL indexing options from {@link
   * com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata#getOptions()}.
   */
  Map<String, String> indexOptions();

  /**
   * If the index is unsupported by the API, unsupported indexes need to be listed from <code>
   * listIndexes</code> but cannot be created.
   *
   * @return
   */
  default boolean isUnsupported() {
    return false;
  }

  @Override
  default PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    // Does not include the indexDesc because it is built on demand
    return prettyToStringBuilder
        .append("indexName", cqlIdentifierToMessageString(indexName()))
        .append("targetColumn", cqlIdentifierToMessageString(targetColumn()))
        .append("indexType", indexType())
        .append("indexOptions", indexOptions())
        .append("isUnsupported", isUnsupported());
  }
}
