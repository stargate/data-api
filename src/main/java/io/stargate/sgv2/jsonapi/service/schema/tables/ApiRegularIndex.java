package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;
import static io.stargate.sgv2.jsonapi.util.CqlOptionUtils.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
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
      CqlIdentifier indexName, CqlIdentifier targetColumn, Map<String, String> indexOptions) {
    super(ApiIndexType.REGULAR, indexName, targetColumn, indexOptions);
  }

  @Override
  public IndexDesc<RegularIndexDefinitionDesc> indexDesc() {

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
        new RegularIndexDefinitionDesc(cqlIdentifierToJsonKey(targetColumn), definitionOptions);

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
      var targetIdentifier = userNameToIdentifier(indexDesc.column(), "targetColumn");

      var apiColumnDef = checkIndexColumnExists(tableSchemaObject, targetIdentifier);

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails - the resolver needs to set up a custom error mapper
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
        }
        // nothing to update in the cqlOptions for these indexes
      } else {
        // text and ascii fields can have the text analysis options specified
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

      // for now, we do not support collection indexes, will do for GA - aaron nov 11
      // and when we do the collection indexes will be in the createIndex command so will be regular
      // indexes.
      if (apiColumnDef.type().isContainer()) {
        throw new UnsupportedCqlIndexException(
            "Collection indexes not fully supported.", indexMetadata);
      }

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
