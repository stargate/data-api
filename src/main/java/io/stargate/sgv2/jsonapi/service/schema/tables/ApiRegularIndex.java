package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.RegularIndexDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.defaults.BooleanProperty;
import io.stargate.sgv2.jsonapi.util.defaults.PropertyDefaults;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** An index on a numeric or text column that is not using text analysis . */
public class ApiRegularIndex implements ApiIndexDef {

  public static final IndexFactoryFromIndexDesc<ApiRegularIndex, RegularIndexDesc>
      FROM_DESC_FACTORY = new UserDescFactory();
  public static final IndexFactoryFromCql FROM_CQL_FACTORY = new CqlTypeFactory();

  private static final BooleanProperty.Stringable ASCII =
      PropertyDefaults.ofStringable("ascii", TableDescDefaults.RegularIndexDescDefaults.ASCII);
  private static final BooleanProperty.Stringable CASE_SENSITIVE =
      PropertyDefaults.ofStringable(
          "case_sensitive", TableDescDefaults.RegularIndexDescDefaults.CASE_SENSITIVE);
  private static final BooleanProperty.Stringable NORMALIZE =
      PropertyDefaults.ofStringable(
          "normalize", TableDescDefaults.RegularIndexDescDefaults.NORMALIZE);

  protected final CqlIdentifier indexName;
  protected final CqlIdentifier targetColumn;
  protected final Map<String, String> options;

  ApiRegularIndex(
      CqlIdentifier indexName, CqlIdentifier targetColumn, Map<String, String> options) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.targetColumn = Objects.requireNonNull(targetColumn, "targetColumn must not be null");
    this.options =
        Collections.unmodifiableMap(Objects.requireNonNull(options, "options must not be null"));
  }

  @Override
  public CqlIdentifier indexName() {
    return indexName;
  }

  @Override
  public CqlIdentifier targetColumn() {
    return targetColumn;
  }

  @Override
  public ApiIndexType indexType() {
    return ApiIndexType.REGULAR;
  }

  @Override
  public boolean isUnsupported() {
    return false;
  }

  @Override
  public IndexDesc indexDesc() {
    return null;
  }

  public boolean isAscii() {
    return ASCII.getWithDefaultStringable(options);
  }

  public boolean isCaseSensitive() {
    return CASE_SENSITIVE.getWithDefaultStringable(options);
  }

  public boolean isNormalize() {
    return NORMALIZE.getWithDefaultStringable(options);
  }

  private static class UserDescFactory
      extends IndexFactoryFromIndexDesc<ApiRegularIndex, RegularIndexDesc> {

    @Override
    public ApiRegularIndex create(
        TableSchemaObject tableSchemaObject, String indexName, RegularIndexDesc indexDesc) {

      Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null");
      Objects.requireNonNull(indexDesc, "indexDesc must not be null");

      // for now we are relying on the validation of the request deserializer that these values are
      // specified
      // userNameToIdentifier will throw an exception if the values are not specified
      var indexIdentifier = userNameToIdentifier(indexName, "indexName");
      var targetIdentifier = userNameToIdentifier(indexDesc.column(), "targetColumn");

      var apiColumnDef = tableSchemaObject.apiTableDef().allColumns().get(targetIdentifier);
      if (apiColumnDef == null) {
        throw SchemaException.Code.UNKNOWN_INDEX_COLUMN.get(
            errVars(
                tableSchemaObject,
                map -> {
                  map.put(
                      "allColumns",
                      errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                  map.put("unknownColumns", errFmt(targetIdentifier));
                }));
      }

      // we could check if there is an existing index but that is a race condition, we will need to
      // catch it if it fails
      // regular indexes can only be on primitive. Adding indexes on maps, sets, lists will come
      // later.
      if (!apiColumnDef.type().isPrimitive()) {
        throw SchemaException.Code.DATA_TYPE_NOT_SUPPORTED_BY_INDEXING.get(
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

      Map<String, String> cqlOptions = new HashMap<>();
      var optionsDesc = indexDesc.options();

      if (apiColumnDef.type().typeName() != ApiTypeName.TEXT
          && apiColumnDef.type().typeName() != ApiTypeName.ASCII) {
        // Only text and ascii fields can have the text analysis options specified
        if (optionsDesc != null) {
          var anyPresent =
              ASCII.isPresent(optionsDesc.ascii())
                  || CASE_SENSITIVE.isPresent(optionsDesc.caseSensitive())
                  || NORMALIZE.isPresent(optionsDesc.normalize());

          if (anyPresent) {
            throw SchemaException.Code.TEXT_ANALYSIS_NOT_SUPPORTED_BY_DATA_TYPE.get(
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
        ASCII.putOrDefaultStringable(cqlOptions, optionsDesc.ascii());
        CASE_SENSITIVE.putOrDefaultStringable(cqlOptions, optionsDesc.caseSensitive());
        NORMALIZE.putOrDefaultStringable(cqlOptions, optionsDesc.normalize());
      }

      return new ApiRegularIndex(indexIdentifier, targetIdentifier, cqlOptions);
    }
  }

  private static class CqlTypeFactory extends IndexFactoryFromCql {

    @Override
    protected ApiIndexDef create(
        ApiColumnDef apiColumnDef, IndexTarget indexTarget, IndexMetadata indexMetadata)
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

  //
  //  private CreateVectorIndexCommand createVectorIndexDefinition() {
  //    final String sourceModel =
  //        Optional.ofNullable(
  //                optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.SOURCE_MODEL_OPTION))
  //            .map(String::toLowerCase)
  //            .orElse(null);
  //
  //    final SimilarityFunction similarityFunction =
  //        Optional.ofNullable(
  //                optionsFromDriver.get(
  //                    TableIndexConstants.IndexOptionKeys.SIMILARITY_FUNCTION_OPTION))
  //            .map((func) -> SimilarityFunction.fromString(func.toLowerCase()))
  //            .orElse(null);
  //
  //    CreateVectorIndexCommand.Definition.Options vectorOptions = null;
  //    if (similarityFunction != null || sourceModel != null) {
  //      vectorOptions =
  //          new CreateVectorIndexCommand.Definition.Options(similarityFunction, sourceModel);
  //    }
  //
  //    CreateVectorIndexCommand.Definition definition =
  //        new CreateVectorIndexCommand.Definition(
  //            CqlIdentifierUtil.externalRepresentation(targetColumn), vectorOptions);
  //    return new CreateVectorIndexCommand(
  //        CqlIdentifierUtil.externalRepresentation(indexName), definition, null);
  //  }
  //
  //  private CreateIndexCommand createRegularIndexDefinition() {
  //    final Boolean caseSensitive =
  //        Optional.ofNullable(
  //
  // optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.CASE_SENSITIVE_OPTION))
  //            .map(Boolean::valueOf)
  //            .orElse(TableIndexConstants.IndexOptionDefault.CASE_SENSITIVE_OPTION_DEFAULT);
  //    final Boolean normalize =
  //        Optional.ofNullable(
  //                optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.NORMALIZE_OPTION))
  //            .map(Boolean::valueOf)
  //            .orElse(TableIndexConstants.IndexOptionDefault.NORMALIZE_OPTION_DEFAULT);
  //
  //    final Boolean ascii =
  //
  // Optional.ofNullable(optionsFromDriver.get(TableIndexConstants.IndexOptionKeys.ASCII_OPTION))
  //            .map(Boolean::valueOf)
  //            .orElse(TableIndexConstants.IndexOptionDefault.ASCII_OPTION_DEFAULT);
  //
  //    CreateIndexCommand.Definition.Options indexOptions =
  //        new CreateIndexCommand.Definition.Options(caseSensitive, normalize, ascii);
  //    CreateIndexCommand.Definition definition =
  //        new CreateIndexCommand.Definition(
  //            CqlIdentifierUtil.externalRepresentation(targetColumn), indexOptions);
  //    return new CreateIndexCommand(
  //        CqlIdentifierUtil.externalRepresentation(indexName), definition, null);
  //  }

}
