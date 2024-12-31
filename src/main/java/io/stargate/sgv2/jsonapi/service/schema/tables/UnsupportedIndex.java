package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.ApiIndexSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.UnsupportedIndexDefinitionDesc;
import java.util.Map;
import java.util.Objects;

/** An index that is defined in the database but not supported by the API. */
public class UnsupportedIndex implements ApiIndexDef {

  private final IndexMetadata indexMetadata;

  UnsupportedIndex(IndexMetadata indexMetadata) {
    this.indexMetadata = Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");
  }

  @Override
  public CqlIdentifier indexName() {
    return indexMetadata.getName();
  }

  @Override
  public CqlIdentifier targetColumn() {
    // we may not have been able to extract this from the index, e.g. this has an unsupported
    // function or something else
    throw new UnsupportedOperationException("UnsupportedIndex does not have a target column");
  }

  @Override
  public ApiIndexType indexType() {
    throw new UnsupportedOperationException("UnsupportedIndex does not have indexType");
  }

  @Override
  public Map<String, String> indexOptions() {
    return indexMetadata.getOptions();
  }

  @Override
  public boolean isUnsupported() {
    return true;
  }

  @Override
  public IndexDesc<UnsupportedIndexDefinitionDesc> indexDesc() {

    var definition =
        new UnsupportedIndexDefinitionDesc(
            ApiIndexSupportDesc.noSupport(indexMetadata.describe(true)));

    return new IndexDesc<>() {
      @Override
      public String name() {
        return cqlIdentifierToJsonKey(indexMetadata.getName());
      }

      @Override
      public ApiIndexType indexType() {
        return null;
      }

      @Override
      public UnsupportedIndexDefinitionDesc definition() {
        return definition;
      }
    };
  }
}
