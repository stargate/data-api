package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescBindingPoint;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.TextIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromCql;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromIndexDesc;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** An index of type {@link ApiIndexType#TEXT} on text column */
public class ApiTextIndex extends ApiSupportedIndex {
  public static final IndexFactoryFromIndexDesc<ApiTextIndex, TextIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();
  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  private final JsonNode analyzer;

  private ApiTextIndex(
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> options,
      JsonNode analyzer) {
    super(ApiIndexType.TEXT, indexName, targetColumn, options, null);

    this.analyzer = Objects.requireNonNull(analyzer, "analyzer must not be null");
  }

  @Override
  public IndexDesc<TextIndexDefinitionDesc> getSchemaDescription(
      SchemaDescBindingPoint bindingPoint) {
    // Index is always has same representation

    var definitionOptions = new TextIndexDefinitionDesc.TextIndexDescOptions(analyzer);
    var definition =
        new TextIndexDefinitionDesc(cqlIdentifierToJsonKey(targetColumn), definitionOptions);

    return new IndexDesc<>() {
      @Override
      public String name() {
        return cqlIdentifierToJsonKey(indexName);
      }

      @Override
      public String indexType() {
        return indexType.apiName();
      }

      @Override
      public TextIndexDefinitionDesc definition() {
        return definition;
      }
    };
  }

  /**
   * Factory to create a new {@link ApiTextIndex} using {@link RegularIndexDefinitionDesc} from the
   * user request.
   */
  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiTextIndex, TextIndexDefinitionDesc> {
    @Override
    public ApiTextIndex create(
        TableSchemaObject tableSchemaObject, String indexName, TextIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now, we are relying on the validation of the request deserializer that these values are
      // specified userNameToIdentifier will throw an exception if the values are not specified
      var indexIdentifier = userNameToIdentifier(indexName, "indexName");
      var targetIdentifier = userNameToIdentifier(indexDesc.column(), "targetColumn");

      var apiColumnDef = checkIndexColumnExists(tableSchemaObject, targetIdentifier);

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails - the resolver needs to set up a custom error mapper

      // Text indexes can only be on text columns
      if (apiColumnDef.type().typeName() != ApiTypeName.TEXT) {
        throw SchemaException.Code.UNSUPPORTED_TEXT_INDEX_FOR_DATA_TYPES.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                  map.put("unsupportedColumns", errFmt(apiColumnDef));
                }));
      }

      Map<String, String> indexOptions = new HashMap<>();

      // The analyzer is optional, if not specified default settings will be used
      // (named analyzer "standard" in CQL).
      // But further, if it is a StringNode, needs to be as a "raw" String, not a JSON String;
      // but if ObjectNode, it must be encoded as JSON object.

      JsonNode analyzerDef = indexDesc.options() == null ? null : indexDesc.options().analyzer();
      if (analyzerDef == null) {
        analyzerDef =
            JsonNodeFactory.instance.textNode(
                TableDescDefaults.CreateTextIndexOptionsDefaults.DEFAULT_NAMED_ANALYZER);
      } else {
        // validate that the analyzer is either a String or an Object
        if (!analyzerDef.isTextual() && !analyzerDef.isObject()) {
          final String unsupportedType = JsonUtil.nodeTypeAsString(analyzerDef);
          throw SchemaException.Code.UNSUPPORTED_JSON_TYPE_FOR_TEXT_INDEX.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put("unsupportedType", unsupportedType);
                  }));
        }
      }
      indexOptions.put(
          TableDescConstants.TextIndexCQLOptions.OPTION_ANALYZER,
          analyzerDef.isTextual() ? analyzerDef.textValue() : analyzerDef.toString());

      return new ApiTextIndex(indexIdentifier, targetIdentifier, indexOptions, analyzerDef);
    }
  }

  /**
   * Factory to create a new {@link ApiTextIndex} using the {@link IndexMetadata} from the driver.
   */
  private static class CqlTypeFactory extends IndexFactoryFromCql {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {

      // this is a sanity check, the base will have worked this, but we should verify it here
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);
      if (apiIndexType != ApiIndexType.TEXT) {
        throw new IllegalStateException(
            "ApiTextIndex factory only supports %s indexes, apiIndexType: %s"
                .formatted(ApiIndexType.TEXT, apiIndexType));
      }

      // also, we  must not have an index function
      if (indexTarget.indexFunction() != null) {
        throw new IllegalStateException(
            "ApiTextIndex factory must not have index function, indexMetadata.name: "
                + indexMetadata.getName());
      }

      String analyzerDefFromCql =
          indexMetadata.getOptions().get(TableDescConstants.TextIndexCQLOptions.OPTION_ANALYZER);

      // Heuristics: 3 choices:
      // 1. JSON Object (as a String) -- JSON decode
      // 2. String (as a String) -- use as-is
      // 3. null or empty -- failure case (should not happen, but handle explicitly)

      JsonNode analyzerDef;

      if (analyzerDefFromCql == null || analyzerDefFromCql.isBlank()) {
        // should never happen, but just in case
        throw new IllegalStateException(
            "ApiTextIndex definition broken (indexMetadata.name: "
                + indexMetadata.getName()
                + "), missing '"
                + TableDescConstants.TextIndexCQLOptions.OPTION_ANALYZER
                + "' JSON; options = "
                + indexMetadata.getOptions());
      } else if (analyzerDefFromCql.trim().startsWith("{")) {
        try {
          analyzerDef = OBJECT_MAPPER.readTree(analyzerDefFromCql);
        } catch (IOException e) {
          throw new IllegalStateException(
              "ApiTextIndex definition broken (indexMetadata.name: "
                  + indexMetadata.getName()
                  + "), invalid JSON -- "
                  + analyzerDefFromCql
                  + " -- error: "
                  + e.getMessage());
        }
      } else {
        // just a string, use as is
        analyzerDef = OBJECT_MAPPER.getNodeFactory().textNode(analyzerDefFromCql);
      }

      return new ApiTextIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          analyzerDef);
    }
  }
}
