package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.defaults.BooleanProperty;
import io.stargate.sgv2.jsonapi.util.defaults.Properties;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** An index on a numeric or text column that is not using text analysis . */
public class ApiRegularIndex extends ApiSupportedIndex {

  public static final IndexFactoryFromIndexDesc<ApiRegularIndex, RegularIndexDefinitionDesc>
      FROM_DESC_FACTORY = new UserDescFactory();

  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  private interface Options {
    BooleanProperty.Stringable ASCII =
        Properties.ofStringable("ascii", TableDescDefaults.RegularIndexDescDefaults.ASCII);

    BooleanProperty.Stringable CASE_SENSITIVE =
        Properties.ofStringable(
            "case_sensitive", TableDescDefaults.RegularIndexDescDefaults.CASE_SENSITIVE);

    BooleanProperty.Stringable NORMALIZE =
        Properties.ofStringable("normalize", TableDescDefaults.RegularIndexDescDefaults.NORMALIZE);
  }

  private ApiRegularIndex(
      CqlIdentifier indexName, CqlIdentifier targetColumn, Map<String, String> indexOptions) {
    super(ApiIndexType.REGULAR, indexName, targetColumn, indexOptions);
  }

  @Override
  public IndexDesc<RegularIndexDefinitionDesc> indexDesc() {

    // Only the text indexes has the properties, we rely on the factories to create the options
    // map with the correct values, so we use getIfPresent to skip the defaults which and read null
    // if it
    // is not in the map
    var definitionOptions =
        new RegularIndexDefinitionDesc.RegularIndexDescOptions(
            Options.ASCII.getIfPresentStringable(indexOptions),
            Options.CASE_SENSITIVE.getIfPresentStringable(indexOptions),
            Options.NORMALIZE.getIfPresentStringable(indexOptions));

    var definition =
        new RegularIndexDefinitionDesc(cqlIdentifierToJsonKey(targetColumn), definitionOptions);
    return new IndexDesc<RegularIndexDefinitionDesc>() {
      @Override
      public String name() {
        return cqlIdentifierToJsonKey(indexName);
      }

      @Override
      public ApiIndexType indexType() {
        return ApiIndexType.REGULAR;
      }

      @Override
      public RegularIndexDefinitionDesc definition() {
        return definition;
      }
    };
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
   * Factor to create a new {@link ApiRegularIndex} using {@link RegularIndexDefinitionDesc} from
   * the user request.
   */
  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiRegularIndex, RegularIndexDefinitionDesc> {

    @Override
    public ApiRegularIndex create(
        TableSchemaObject tableSchemaObject,
        String indexName,
        RegularIndexDefinitionDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now we are relying on the validation of the request deserializer that these values are
      // specified
      // userNameToIdentifier will throw an exception if the values are not specified
      var indexIdentifier = userNameToIdentifier(indexName, "indexName");
      var targetIdentifier = userNameToIdentifier(indexDesc.column(), "targetColumn");

      var apiColumnDef = checkIndexColumnExists(tableSchemaObject, targetIdentifier);

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails
      // regular indexes can only be on primitive. Adding indexes on maps, sets, lists will come
      // later.
      if (!apiColumnDef.type().isPrimitive()) {
        throw SchemaException.Code.UNSUPPORTED_INDEXING_FOR_DATA_TYPES.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                  map.put("supportedTypes", errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()));
                  map.put("unsupportedColumns", errFmt(apiColumnDef));
                }));
      }

      Map<String, String> indexOptions = new HashMap<>();
      var optionsDesc = indexDesc.options();

      if (apiColumnDef.type().typeName() != ApiTypeName.TEXT
          && apiColumnDef.type().typeName() != ApiTypeName.ASCII) {
        // Only text and ascii fields can have the text analysis options specified
        if (optionsDesc != null) {
          var anyPresent =
              Options.ASCII.isPresent(optionsDesc.ascii())
                  || Options.CASE_SENSITIVE.isPresent(optionsDesc.caseSensitive())
                  || Options.NORMALIZE.isPresent(optionsDesc.normalize());

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
        // nothing to update in the cqlOptions for these indexes
      } else {
        // text and ascii fields can have the text analysis options specified
        Options.ASCII.putOrDefaultStringable(
            indexOptions, optionsDesc == null ? null : optionsDesc.ascii());
        Options.CASE_SENSITIVE.putOrDefaultStringable(
            indexOptions, optionsDesc == null ? null : optionsDesc.caseSensitive());
        Options.NORMALIZE.putOrDefaultStringable(
            indexOptions, optionsDesc == null ? null : optionsDesc.normalize());
      }

      return new ApiRegularIndex(indexIdentifier, targetIdentifier, indexOptions);
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

      // also, we should not have an index function
      if (indexTarget.indexFunction() != null) {
        throw new IllegalStateException(
            "ApiRegularIndex factory does not support index functions, indexTarget.indexFunction: "
                + indexTarget.indexFunction());
      }

      return new ApiRegularIndex(
          indexMetadata.getName(), indexTarget.targetColumn(), indexMetadata.getOptions());
    }
  }
}
