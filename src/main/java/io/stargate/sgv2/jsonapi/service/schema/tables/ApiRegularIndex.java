package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;
import static io.stargate.sgv2.jsonapi.util.CqlOptionUtils.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapComponentDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromCql;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.IndexFactoryFromIndexDesc;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** An index of type {@link ApiIndexType#REGULAR} on a scalar or collection column */
public class ApiRegularIndex extends ApiSupportedIndex {

  public static final IndexFactoryFromIndexDesc<ApiRegularIndex, RegularIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();

  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  /** Names of the CQL index options */
  public interface CQLOptions {
    String ASCII = "ascii";
    String CASE_SENSITIVE = "case_sensitive";
    String NORMALIZE = "normalize";
  }

  private ApiRegularIndex(
      CqlIdentifier indexName,
      CqlIdentifier targetColumn,
      Map<String, String> indexOptions,
      ApiIndexFunction indexFunction) {
    super(ApiIndexType.REGULAR, indexName, targetColumn, indexOptions, indexFunction);
  }

  @Override
  public IndexDesc<RegularIndexDefinitionDesc> getSchemaDescription(
      SchemaDescSource schemaDescSource) {
    // Index is always has same representation

    // Only the text indexes has the properties, we rely on the factories to create the options
    // map in indexOptions with the correct values, so just use get() and return a null if not
    // found.
    // Then rely on the RegularIndexDescOptions to exclude nulls in its serialisation
    var definitionOptions =
        new RegularIndexDefinitionDesc.RegularIndexDescOptions(
            getBooleanIfPresent(indexOptions, CQLOptions.ASCII),
            getBooleanIfPresent(indexOptions, CQLOptions.CASE_SENSITIVE),
            getBooleanIfPresent(indexOptions, CQLOptions.NORMALIZE));

    var definition =
        new RegularIndexDefinitionDesc(
            new RegularIndexDefinitionDesc.RegularIndexColumn(
                cqlIdentifierToJsonKey(targetColumn),
                indexFunction == null ? null : indexFunction.toApiMapComponent()),
            definitionOptions);

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
      public RegularIndexDefinitionDesc definition() {
        return definition;
      }
    };
  }

  /**
   * Factory to create a new {@link ApiRegularIndex} using {@link RegularIndexDefinitionDesc} from
   * the user request.
   */
  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiRegularIndex, RegularIndexDefinitionDesc> {

    /**
     * /** ApiRegularIndex could be used for indexes on primitive or collection(map/set/list)
     * datatypes.
     *
     * @param tableSchemaObject The schema object of the table where the index will be created. Must
     *     not be null.
     * @param indexName The name of the index to be created. Must not be null.
     * @param indexDesc The description of the index to be created, including column and options.
     *     Must not be null.
     * @return A new instance of {@link ApiRegularIndex}.
     */
    @Override
    public ApiRegularIndex create(
        TableSchemaObject tableSchemaObject,
        String indexName,
        RegularIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now, we are relying on the validation of the request deserializer that these values are
      // specified userNameToIdentifier will throw an exception if the values are not specified
      var indexIdentifier = userNameToIdentifier(indexName, "indexName");
      var targetIdentifier = userNameToIdentifier(indexDesc.column().columnName(), "targetColumn");
      var mapComponentDesc = indexDesc.column().mapComponent();

      var apiColumnDef = checkIndexColumnExists(tableSchemaObject, targetIdentifier);

      // create ApiRegularIndex for the target primitive column
      if (apiColumnDef.type().isPrimitive()) {
        return createApiIndexForPrimitive(
            tableSchemaObject, apiColumnDef, indexIdentifier, targetIdentifier, indexDesc);
      }

      // create ApiRegularIndex for the target map/set/list column
      if (apiColumnDef.type().isContainer()
          && apiColumnDef.type().typeName() != ApiTypeName.VECTOR) {
        return createApiIndexForCollection(
            tableSchemaObject,
            apiColumnDef,
            indexIdentifier,
            targetIdentifier,
            mapComponentDesc,
            indexDesc);
      }

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails.

      // This won't happen for now, since above checking cover primitive and collection types.
      // However, any primitive type that is not allow to create index should fall into this error.
      // Ticket. https://github.com/stargate/data-api/issues/1872
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
    private ApiRegularIndex createApiIndexForPrimitive(
        TableSchemaObject tableSchemaObject,
        ApiColumnDef apiColumnDef,
        CqlIdentifier indexIdentifier,
        CqlIdentifier targetIdentifier,
        RegularIndexDefinitionDesc indexDesc) {
      var optionsDesc = indexDesc.options();

      // resolve the analyzer options
      var indexOptions =
          resolveAnalyzerProperty(tableSchemaObject, apiColumnDef, optionsDesc, null);

      // indexFunction is null for primitive dataTypes
      return new ApiRegularIndex(indexIdentifier, targetIdentifier, indexOptions, null);
    }

    /**
     * The helper method to create ApiIndex for map/set/list collection dataTypes.
     *
     * <p>Index Function for set and list will be default to values(set/list). Index Function for
     * map will be default to entries(map). User can specify the index function for map column by
     * following:
     *
     * <ul>
     *   <li>Index on map keys: <code>{"column": {"mapColumn" : "$keys"}}</code>
     *   <li>Index on map values: <code>{"column": {"mapColumn" : "$values"}}</code>
     *   <li>Index on map entries(default): <code>{"column": "mapColumn"}</code>
     * </ul>
     *
     * <p>Rules for collection indexes:
     *
     * <ul>
     *   <li>NO support create index for frozen map/set/list columns
     *   <li>Only text and ascii datatypes can be analyzed, including text and ascii on
     *       map/set/list.
     *   <li>text analyse can only be keys and values for a map , not entries.
     * </ul>
     */
    private ApiRegularIndex createApiIndexForCollection(
        TableSchemaObject tableSchemaObject,
        ApiColumnDef apiColumnDef,
        CqlIdentifier indexIdentifier,
        CqlIdentifier targetIdentifier,
        MapComponentDesc mapComponentDesc,
        RegularIndexDefinitionDesc indexDesc) {
      var optionsDesc = indexDesc.options();

      // do NOT support create index for frozen map/set/list columns
      if (apiColumnDef.type() instanceof CollectionApiDataType<?> collectionApiDataType
          && collectionApiDataType.isFrozen()) {
        throw SchemaException.Code.UNSUPPORTED_INDEXING_FOR_FROZEN_COLUMN.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                  map.put("targetColumn", errFmt(targetIdentifier));
                }));
      }

      // validate user specified index function
      // Default index function for set and list is values
      var indexFunction =
          apiColumnDef.type().typeName() == ApiTypeName.MAP
              ? ApiIndexFunction.fromMapComponentDesc(mapComponentDesc)
              : ApiIndexFunction.VALUES;

      // resolve the analyzer options
      var indexOptions =
          resolveAnalyzerProperty(tableSchemaObject, apiColumnDef, optionsDesc, indexFunction);

      return new ApiRegularIndex(indexIdentifier, targetIdentifier, indexOptions, indexFunction);
    }

    /**
     * Method to validate user specified analyzer property in the index options, if the property is
     * valid, populate the indexOptions map.
     *
     * <p>Rules for analyzer options:
     *
     * <ul>
     *   <li>Text and ascii primitive datatypes can have the analyzer options specified.
     *   <li>List/Set that has text and ascii primitive value can have the analyzer options
     *       specified.
     *   <li>Map index on keys, and keys are text and ascii primitive datatypes can have the
     *       analyzer options specified.
     *   <li>Map index on values, and values are text and ascii primitive datatypes can have the
     *       analyzer options specified.
     *   <li>Map index on entries is not supported for analyzer options.
     * </ul>
     *
     * The method will error out as UNSUPPORTED_TEXT_ANALYSIS_FOR_DATA_TYPES if rules are violated.
     * The method will return the indexOptions map if the rules are followed.
     */
    private Map<String, String> resolveAnalyzerProperty(
        TableSchemaObject tableSchemaObject,
        ApiColumnDef apiColumnDef,
        RegularIndexDefinitionDesc.RegularIndexDescOptions optionsDesc,
        ApiIndexFunction indexFunction) {

      // Nothing to validate if user does not specify the options.
      if (optionsDesc == null) {
        // Text and ascii fields will default options, this is to align with the previous behavior.
        if (apiColumnDef.type().typeName() == ApiTypeName.TEXT
            || apiColumnDef.type().typeName() == ApiTypeName.ASCII) {
          return populateIndexOptions(null);
        }
        return new HashMap<>();
      }

      ApiTypeName targetTypeName =
          switch (apiColumnDef.type()) {
            case ApiMapType apiMapType ->
                switch (indexFunction) {
                  case KEYS -> apiMapType.getKeyType().typeName();
                  case VALUES -> apiMapType.getValueType().typeName();
                  case ENTRIES ->
                      throw SchemaException.Code.CANNOT_ANALYZE_ENTRIES_ON_MAP_COLUMNS.get(
                          errVars(
                              tableSchemaObject,
                              map -> {
                                map.put(
                                    "allColumns",
                                    errFmtApiColumnDef(
                                        tableSchemaObject.apiTableDef().allColumns()));
                                map.put("targetColumn", errFmt(apiColumnDef.name()));
                                map.put("analyzedOptions", optionsDesc.toString());
                              }));
                  case null ->
                      throw new IllegalStateException(
                          "Unexpected indexFunction for ApiMapType: " + indexFunction);
                };
            case ApiSetType apiSetType -> apiSetType.valueType.typeName();
            case ApiListType apiListType -> apiListType.valueType.typeName();
              // Primitive type
            default -> apiColumnDef.type().typeName();
          };

      if (targetTypeName != ApiTypeName.TEXT && targetTypeName != ApiTypeName.ASCII) {
        // Only text and ascii fields can have the text analysis options specified
        var anyPresent =
            optionsDesc.ascii() != null
                || optionsDesc.caseSensitive() != null
                || optionsDesc.normalize() != null;

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
        // If the target column is not text or ascii, and the user has not specified any options.
        return new HashMap<>();
      } else {
        return populateIndexOptions(optionsDesc);
      }
    }

    /**
     * Method to populate the index options map.
     *
     * <p>Note, this method should be called after the options are being validated.
     */
    private Map<String, String> populateIndexOptions(
        RegularIndexDefinitionDesc.RegularIndexDescOptions optionsDesc) {
      Map<String, String> indexOptions = new HashMap<>();
      var ascii =
          ApiOptionUtils.getOrDefault(
              optionsDesc,
              RegularIndexDefinitionDesc.RegularIndexDescOptions::ascii,
              TableDescDefaults.RegularIndexDescDefaults.ASCII);
      put(indexOptions, CQLOptions.ASCII, ascii);
      var case_sensitive =
          ApiOptionUtils.getOrDefault(
              optionsDesc,
              RegularIndexDefinitionDesc.RegularIndexDescOptions::caseSensitive,
              TableDescDefaults.RegularIndexDescDefaults.CASE_SENSITIVE);
      put(indexOptions, CQLOptions.CASE_SENSITIVE, case_sensitive);

      var normalize =
          ApiOptionUtils.getOrDefault(
              optionsDesc,
              RegularIndexDefinitionDesc.RegularIndexDescOptions::normalize,
              TableDescDefaults.RegularIndexDescDefaults.NORMALIZE);
      put(indexOptions, CQLOptions.NORMALIZE, normalize);
      return indexOptions;
    }
  }

  /**
   * Factory to create a new {@link ApiRegularIndex} using {@link IndexMetadata} from the driver.
   */
  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
        throws UnsupportedCqlIndexException {

      // this is a sanity check, the base will have worked this, but we should check it here
      var apiIndexType = ApiIndexType.fromCql(apiColumnDef, indexTarget, indexMetadata);
      if (apiIndexType != ApiIndexType.REGULAR) {
        throw new IllegalStateException(
            "ApiRegularIndex factory only supports %s indexes, apiIndexType: %s"
                .formatted(ApiIndexType.REGULAR, apiIndexType));
      }

      return new ApiRegularIndex(
          indexMetadata.getName(),
          indexTarget.targetColumn(),
          indexMetadata.getOptions(),
          indexTarget.indexFunction());
    }
  }
}
