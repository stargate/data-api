package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Map;

/**
 * The API definition of an index, is an interface so the unsupported indexes can be represented as
 * well.
 */
public interface ApiIndexDef extends PrettyPrintable {

  CqlIdentifier indexName();

  CqlIdentifier targetColumn();

  ApiIndexType indexType();

  IndexDesc<?> indexDesc();

  // For map/set/list collection columns, index creation needs to specify the function.
  // And indexFunctions are not appended as options.
  // For ApiIndexType that is not COLLECTION, indexFunction should be null.
  ApiIndexFunction indexFunction();

  Map<String, String> indexOptions();

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
        .append("indexFunction", indexFunction())
        .append("indexOptions", indexOptions())
        .append("isUnsupported", isUnsupported());
  }
}
