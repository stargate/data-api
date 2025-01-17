package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.quarkus.logging.Log;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.GeneralIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.defaults.BooleanProperty;
import io.stargate.sgv2.jsonapi.util.defaults.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ApiGeneralIndex serves two types in general:<br>
 * Primitive datatype columns. <br>
 * Only text and ascii can be analyzed, will be appended as indexOptions.<br>
 * Map/Set/List collection datatype columns. <br>
 */
public class ApiGeneralIndex extends ApiSupportedIndex {

  public static final IndexFactoryFromIndexDesc<ApiGeneralIndex, GeneralIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();

  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  private interface Options {
    BooleanProperty.Stringable ASCII =
        Properties.ofStringable("ascii", TableDescDefaults.GeneralIndexDescDefaults.ASCII);

    BooleanProperty.Stringable CASE_SENSITIVE =
        Properties.ofStringable(
            "case_sensitive", TableDescDefaults.GeneralIndexDescDefaults.CASE_SENSITIVE);

    BooleanProperty.Stringable NORMALIZE =
        Properties.ofStringable("normalize", TableDescDefaults.GeneralIndexDescDefaults.NORMALIZE);
  }

  private ApiGeneralIndex(
      CqlIdentifier indexName,
      ApiIndexType indexType,
      CqlIdentifier targetColumn,
      ApiIndexFunction indexFunction,
      Map<String, String> indexOptions) {
    super(indexType, indexName, targetColumn, indexFunction, indexOptions);
  }

  public boolean isAscii() {
    return Options.ASCII.getWithDefaultStringable(indexOptions);
  }

  public boolean isCaseSensitive() {
    return Options.CASE_SENSITIVE.getWithDefaultStringable(indexOptions);
  }

  public boolean isNormalize() {
    return Options.NORMALIZE.getWithDefaultStringable(indexOptions);
  }

  /**
   * Factor to create a new {@link ApiGeneralIndex} using {@link GeneralIndexDefinitionDesc} from
   * the user request.
   */
  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiGeneralIndex, GeneralIndexDefinitionDesc> {

    @Override
    public ApiGeneralIndex create(
        TableSchemaObject tableSchemaObject,
        String indexName,
        GeneralIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now we are relying on the validation of the request deserializer that these values are
      // specified
      // userNameToIdentifier will throw an exception if the values are not specified
      var indexIdentifier = userNameToIdentifier(indexName, "indexName");
      var targetIdentifier = userNameToIdentifier(indexDesc.column(), "targetColumn");

      var apiColumnDef = checkIndexColumnExists(tableSchemaObject, targetIdentifier);

      // create an ApiGeneralIndex for the target primitive datatype
      if (apiColumnDef.type().isPrimitive()) {
        return createApiIndexForPrimitive(
            tableSchemaObject, apiColumnDef, indexIdentifier, targetIdentifier, indexDesc);
      }

      // create an ApiGeneralIndex for the target map/set/list datatype
      if (apiColumnDef.type().isContainer()) {
        try {
          return createApiIndexForCollection(
              tableSchemaObject, apiColumnDef, indexIdentifier, targetIdentifier, indexDesc);
        } catch (UnknownCqlIndexFunctionException e) {
          // This won't happen, since the index function has already been validated.
        }
      }

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails
      throw SchemaException.Code.UNSUPPORTED_INDEXING_FOR_DATA_TYPES.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns", errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                map.put("supportedTypes", errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()));
                map.put("unsupportedColumns", errFmt(apiColumnDef));
              }));
    }

    /** The helper method to create ApiIndex for primitive dataTypes. */
    private ApiGeneralIndex createApiIndexForPrimitive(
        TableSchemaObject tableSchemaObject,
        ApiColumnDef apiColumnDef,
        CqlIdentifier indexIdentifier,
        CqlIdentifier columnIdentifier,
        GeneralIndexDefinitionDesc indexDesc) {
      Map<String, String> indexOptions = new HashMap<>();
      var optionsDesc = indexDesc.options();

      validateAnalyzableDatatypes(tableSchemaObject, apiColumnDef, optionsDesc, indexDesc);

      populateIndexOptionsMap(indexOptions, optionsDesc);

      return new ApiGeneralIndex(
          indexIdentifier, ApiIndexType.REGULAR, columnIdentifier, null, indexOptions);
    }

    /**
     * The helper method to create ApiIndex for map/set/list collection dataTypes. <br>
     *
     * <pre>
     * Rules to validate the indexFunction
     * 1. indexFunction in indexDesc should not be null, user must provide
     * 2. Since Data API does not support frozen map/set/list table creation, FULL index will also not be supported.
     * 3. Currently does not support create index for frozen map/set/list columns
     * 4. Only text and ascii datatypes can be analyzed, including text and ascii on map/set/list.
     * 5. KEYS and ENTRIES index functions can only be used for map, not for set/list
     * </pre>
     */
    private ApiGeneralIndex createApiIndexForCollection(
        TableSchemaObject tableSchemaObject,
        ApiColumnDef apiColumnDef,
        CqlIdentifier indexIdentifier,
        CqlIdentifier columnIdentifier,
        GeneralIndexDefinitionDesc indexDesc)
        throws UnknownCqlIndexFunctionException {
      Map<String, String> indexOptions = new HashMap<>();
      var optionsDesc = indexDesc.options();

      // Rule 1
      if (indexDesc.indexFunction() == null) {
        throw SchemaException.Code.MISSING_INDEX_FUNCTION_FOR_COLLECTION_COLUMN.get();
      }

      // Rule 2
      // This rule has been enforced to keys/values/entries in GeneralIndexDefinitionDesc

      // Rule 3
      if (apiColumnDef.type() instanceof CollectionApiDataType collectionApiDataType
          && collectionApiDataType.isFrozen) {
        // above check is not necessary, just to keep a safe cast
        throw SchemaException.Code.UNSUPPORTED_INDEXING_FOR_FROZEN_COLUMN.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                  map.put("indexFunction", indexDesc.indexFunction());
                  map.put("targetColumn", errFmt(columnIdentifier));
                }));
      }

      // Rule 4
      validateAnalyzableDatatypes(tableSchemaObject, apiColumnDef, optionsDesc, indexDesc);
      populateIndexOptionsMap(indexOptions, optionsDesc);

      // Rule 5
      if (indexDesc.indexFunction().equals(ApiIndexFunction.KEYS.cqlFunction)
          || indexDesc.indexFunction().equals(ApiIndexFunction.ENTRIES.cqlFunction)) {
        if (apiColumnDef.type().typeName() != ApiTypeName.MAP) {
          throw SchemaException.Code.CANNOT_APPLY_INDEX_FUNCTION_KEYS_ENTRIES_TO_NON_MAP_COLUMN.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put(
                        "allColumns",
                        errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                    map.put("indexFunction", indexDesc.indexFunction());
                    map.put("targetColumn", errFmt(columnIdentifier));
                  }));
        }
      }

      return new ApiGeneralIndex(
          indexIdentifier,
          ApiIndexType.COLLECTION,
          columnIdentifier,
          ApiIndexFunction.fromCql(indexDesc.indexFunction()),
          indexOptions);
    }

    /**
     * Populate the indexOptions map by pulling values from optionsDesc. indexOptions will be used
     * to append the options in createIndex cql statement.
     */
    private void populateIndexOptionsMap(
        Map<String, String> indexOptions,
        GeneralIndexDefinitionDesc.GeneralIndexDescOptions optionsDesc) {
      Options.ASCII.putOrDefaultStringable(
          indexOptions, optionsDesc == null ? null : optionsDesc.ascii());
      Options.CASE_SENSITIVE.putOrDefaultStringable(
          indexOptions, optionsDesc == null ? null : optionsDesc.caseSensitive());
      Options.NORMALIZE.putOrDefaultStringable(
          indexOptions, optionsDesc == null ? null : optionsDesc.normalize());
    }

    /**
     * Only text and ascii datatypes can be analyzed. <br>
     * Only text and ascii in the collection datatype can be analyzed. It works for values(list),
     * values(set), keys(map), values(map). Note, not for entries(map). <br>
     * The method will error out as UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES if rules are violated.
     */
    private void validateAnalyzableDatatypes(
        TableSchemaObject tableSchemaObject,
        ApiColumnDef apiColumnDef,
        GeneralIndexDefinitionDesc.GeneralIndexDescOptions optionsDesc,
        GeneralIndexDefinitionDesc indexDesc) {

      if (optionsDesc == null) {
        // no need to analyze if user does not specify any GeneralIndexDescOptions
        return;
      }

      // Primitive type
      ApiTypeName analyzedColumnType = apiColumnDef.type().typeName();

      // Map collection type
      if (apiColumnDef.type() instanceof ApiMapType mapType) {
        if (indexDesc.indexFunction().equals(ApiIndexFunction.KEYS.cqlFunction)) {
          analyzedColumnType = mapType.getKeyType().typeName();
        } else if (indexDesc.indexFunction().equals(ApiIndexFunction.VALUES.cqlFunction)) {
          analyzedColumnType = mapType.getValueType().typeName();
        } else {
          throw SchemaException.Code.CANNOT_ANALYZE_ENTRIES_ON_MAP_COLUMNS.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put(
                        "allColumns",
                        errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                    map.put("indexFunction", indexDesc.indexFunction());
                    map.put("targetColumn", errFmt(apiColumnDef.name()));
                  }));
        }
      }

      // Set/List collection type
      if (apiColumnDef.type() instanceof ApiSetType apiSetType) {
        analyzedColumnType = apiSetType.valueType.typeName();
      }
      if (apiColumnDef.type() instanceof ApiListType apiListType) {
        analyzedColumnType = apiListType.valueType.typeName();
      }

      if (analyzedColumnType != ApiTypeName.TEXT && analyzedColumnType != ApiTypeName.ASCII) {
        var anyPresent =
            Options.ASCII.isPresent(optionsDesc.ascii())
                || Options.NORMALIZE.isPresent(optionsDesc.normalize());
        // CASE_SENSITIVE is not analyze option, default to true

        if (anyPresent) {
          throw SchemaException.Code.UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put(
                        "allColumns",
                        errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                    map.put("unsupportedColumns", errFmt(apiColumnDef));
                  }));
        }
      }
    }
  }

  /**
   * Factory to create a new {@link ApiGeneralIndex} using {@link IndexMetadata} from the driver.
   */
  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {

      // this is a sanity check, the base will have worked this, but we should check it here
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);
      if (apiIndexType != ApiIndexType.REGULAR && apiIndexType != ApiIndexType.COLLECTION) {
        throw new IllegalStateException(
            "ApiGeneralIndex factory only supports %s,%s indexes, apiIndexType: %s"
                .formatted(ApiIndexType.REGULAR, ApiIndexType.COLLECTION, apiIndexType));
      }

      // also, we should not have an index function
      if (indexTarget.indexFunction() != null) {
        throw new IllegalStateException(
            "ApiRegularIndex factory does not support index functions, indexTarget.indexFunction: "
                + indexTarget.indexFunction());
      }
      // TODO, index target problem
      return new ApiGeneralIndex(
          indexMetadata.getName(),
          apiIndexType,
          indexTarget.targetColumn(),
          indexTarget.indexFunction(),
          indexMetadata.getOptions());
    }
  }

  @Override
  public IndexDesc<GeneralIndexDefinitionDesc> indexDesc() {

    // Only the text indexes has the properties, we rely on the factories to create the options
    // map with the correct values, so we use getIfPresent to skip the defaults which and read null
    // if it is not in the map
    var definitionOptions =
        new GeneralIndexDefinitionDesc.GeneralIndexDescOptions(
            Options.ASCII.getIfPresentStringable(indexOptions),
            Options.CASE_SENSITIVE.getIfPresentStringable(indexOptions),
            Options.NORMALIZE.getIfPresentStringable(indexOptions));

    var definition =
        new GeneralIndexDefinitionDesc(
            cqlIdentifierToJsonKey(targetColumn),
            indexFunction != null ? indexFunction.name() : null,
            definitionOptions);
    return new IndexDesc<GeneralIndexDefinitionDesc>() {
      @Override
      public String name() {
        return cqlIdentifierToJsonKey(indexName);
      }

      @Override
      public String indexType() {
        return indexType.indexTypeName();
      }

      @Override
      public GeneralIndexDefinitionDesc definition() {
        return definition;
      }
    };
  }
}
