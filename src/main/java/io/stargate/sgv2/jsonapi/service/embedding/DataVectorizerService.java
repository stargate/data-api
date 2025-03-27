package io.stargate.sgv2.jsonapi.service.embedding;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.MeteredEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.*;

/** Service to vectorize the data to embedding vector. */
@ApplicationScoped
public class DataVectorizerService {

  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public DataVectorizerService(
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  /**
   * This will vectorize the sort clause, update clause and the document with `$vectorize` field
   *
   * @param dataApiRequestInfo
   * @param commandContext
   * @param command
   * @return
   */
  public <T extends SchemaObject> Uni<Command> vectorize(
      CommandContext<T> commandContext, Command command) {

    final DataVectorizer dataVectorizer = constructDataVectorizer(commandContext);

    if (commandContext.schemaObject() instanceof TableSchemaObject) {
      // For Tables, this is now handled by the EmbeddingTask and composite tasks
      return Uni.createFrom().item(command);
    }

    return vectorizeSortClause(dataVectorizer, commandContext, command)
        .onItem()
        .transformToUni(flag -> vectorizeDocument(dataVectorizer, commandContext, command))
        .onItem()
        .transform(flag -> command);
  }

  public <T extends SchemaObject> DataVectorizer constructDataVectorizer(
      CommandContext<T> commandContext) {
    EmbeddingProvider embeddingProvider =
        Optional.ofNullable(commandContext.embeddingProvider())
            .map(
                provider ->
                    new MeteredEmbeddingProvider(
                        meterRegistry,
                        jsonApiMetricsConfig,
                        commandContext.requestContext(),
                        provider,
                        commandContext.commandName()))
            .orElse(null);
    return new DataVectorizer(
        embeddingProvider,
        objectMapper.getNodeFactory(),
        commandContext.requestContext().getEmbeddingCredentials(),
        commandContext.schemaObject());
  }

  private <T extends SchemaObject> Uni<Boolean> vectorizeSortClause(
      DataVectorizer dataVectorizer, CommandContext<T> commandContext, Command command) {

    if (command instanceof Sortable sortable) {
      return dataVectorizer.vectorize(sortable.sortClause());
    }
    return Uni.createFrom().item(true);
  }

  private <T extends SchemaObject> Uni<Boolean> vectorizeDocument(
      DataVectorizer dataVectorizer, CommandContext<T> commandContext, Command command) {
    if (command instanceof InsertOneCommand insertOneCommand) {
      return dataVectorizer.vectorize(List.of(insertOneCommand.document()));
    } else if (command instanceof InsertManyCommand insertManyCommand) {
      return dataVectorizer.vectorize(insertManyCommand.documents());
    }
    return Uni.createFrom().item(true);
  }

  /**
   * Vectorize any table command. This is a new code path to avoid changing the collection code
   * until we need to should be updated later to be a single code path - aaron nov 5th 2024
   */
  private Uni<Command> vectorizeTableCommand(
      DataVectorizer dataVectorizer,
      CommandContext<TableSchemaObject> commandContext,
      Command command) {

    var apiTableDef = commandContext.schemaObject().apiTableDef();
    var vectorizeDefs = apiTableDef.allColumns().getVectorizeDefs();
    if (vectorizeDefs.isEmpty()) {
      return Uni.createFrom().item(command);
    }

    // Update is handled later because we need to check if the update actually changed a value
    // check the UpdateOperationResult class
    List<DataVectorizer.VectorizeTask> tasks =
        switch (command) {
          case InsertManyCommand imc ->
              tasksForVectorizeColumns(
                  commandContext.schemaObject(),
                  imc.documents(),
                  DocumentException.Code.UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION);
          case InsertOneCommand ioc ->
              tasksForVectorizeColumns(
                  commandContext.schemaObject(),
                  List.of(ioc.document()),
                  DocumentException.Code.UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION);
            // Notice table update vectorize happens before UpdateCommand execution, since we can't
            // do readThenUpdate for table.
            // Collection update vectorize happens after the DB read.
          case UpdateOneCommand uoc ->
              taskforUpdate(commandContext.schemaObject(), uoc.updateClause());
          case Sortable sortable -> tasksForSort(sortable, commandContext);

          default -> List.of();
        };

    if (tasks.isEmpty()) {
      // Nothing to do, or this is not the sort of command that we can vectorize
      return Uni.createFrom().item(command);
    }

    var embeddingRequestType =
        switch (command) {
          case ModifyCommand mc -> EmbeddingProvider.EmbeddingRequestType.INDEX;
          case Sortable s -> EmbeddingProvider.EmbeddingRequestType.SEARCH;
          default ->
              throw new IllegalArgumentException(
                  "Could not determine the embeddingRequestType for command type "
                      + command.getClass());
        };

    return dataVectorizer.vectorizeTasks(command, tasks, embeddingRequestType);
  }

  /** Build the list of vectorize tasks when inserting one or more documents */
  private <T extends TableSchemaObject, E extends RequestException>
      List<DataVectorizer.VectorizeTask> tasksForVectorizeColumns(
          T tableSchemaObject, List<JsonNode> documents, ErrorCode<E> noVectorizeDefinitionCode) {

    var apiTableDef = tableSchemaObject.apiTableDef();
    var vectorColumnDefs = apiTableDef.allColumns().filterVectorColumnsToList();

    if (vectorColumnDefs.isEmpty()) {
      return List.of();
    }

    // get all the fields in the documents that are vector columns, and the value of the node is a
    // string, so we can check if they have a vectorize def.
    // key is the parent of the vector field, not the vector field itself
    Map<ObjectNode, List<ApiColumnDef>> candidateVectorizeFieldsPerDoc = new HashMap<>();
    for (var vectorColumnDef : vectorColumnDefs) {
      for (var document : documents) {
        var vectorField = document.path(vectorColumnDef.jsonKey());

        // if the field does not exist then jackson will return a missing node, double-checking with
        // text to be sure
        if (!vectorField.isMissingNode() && vectorField.isTextual()) {
          // document is the parent, we do not go deeper than the top level
          if (!document.isObject()) {
            throw new IllegalArgumentException("Document node must be an ObjectNode");
          }
          candidateVectorizeFieldsPerDoc
              .computeIfAbsent((ObjectNode) document, key -> new ArrayList<>())
              .add(vectorColumnDef);
        }
      }
    }

    // Now check that the columns actually have vectorize enabled.
    Set<String> nonVectorizeFieldNames = new HashSet<>();
    Set<ApiColumnDef> supportedVectorizeFields = new HashSet<>();
    for (var columnDefs : candidateVectorizeFieldsPerDoc.values()) {
      for (var columnDef : columnDefs) {
        if (((ApiVectorType) columnDef.type()).getVectorizeDefinition() == null) {
          nonVectorizeFieldNames.add(columnDef.jsonKey());
        } else {
          supportedVectorizeFields.add(columnDef);
        }
      }
    }

    if (!nonVectorizeFieldNames.isEmpty()) {
      throw noVectorizeDefinitionCode.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("validVectorizeColumns", errFmtApiColumnDef(supportedVectorizeFields));
                map.put("invalidVectorizeColumns", errFmtJoin(nonVectorizeFieldNames));
              }));
    }

    var vectorizeTasks = new ArrayList<DataVectorizer.VectorizeTask>();
    for (var entry : candidateVectorizeFieldsPerDoc.entrySet()) {
      var parentNodeDoc = entry.getKey();
      var vectorColumnDefsPerDoc = entry.getValue();
      for (var vectorColumnDef : vectorColumnDefsPerDoc) {
        // if the user sent a blank string for the vectorize field, we just turn that into a null
        // without vectorizing
        // but that only applies if the column has a vectorize definition so do it here not above
        var vectorizeText = parentNodeDoc.path(vectorColumnDef.jsonKey()).textValue();
        if (vectorizeText.isBlank()) {
          parentNodeDoc.putNull(vectorColumnDef.jsonKey());
        } else {
          vectorizeTasks.add(new DataVectorizer.VectorizeTask(parentNodeDoc, vectorColumnDef));
        }
      }
    }
    return vectorizeTasks;
  }

  /** Build the list of vectorize tasks for a sort clause */
  private List<DataVectorizer.VectorizeTask> tasksForSort(
      Sortable command, CommandContext<TableSchemaObject> commandContext) {

    var sortClause = command.sortClause();
    // because this is coming off the command may be null or empty
    if (sortClause == null || sortClause.isEmpty()) {
      return List.of();
    }

    var vectorizeSorts = command.sortClause().tableVectorizeSorts();
    if (vectorizeSorts.isEmpty()) {
      return List.of();
    }

    var tableSchemaObject = commandContext.schemaObject();

    if (vectorizeSorts.size() > 1) {
      //      "sort": {
      //        "vector_col_with_vectorize_def": "ChatGPT integrated sneakers that talk to you",
      //        "vector_col_without_vectorize_de": "ChatGPT integrated sneakers that talk to you"
      //      },
      //      if we do not check here, we will get the first vectorize sort, vectorize it, and sort
      // clause will end up with
      //      thinking second one is not a vector sort, and say you can not combine vector sort and
      // non-vector sort
      throw SortException.Code.CANNOT_SORT_ON_MULTIPLE_VECTORIZE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "sortVectorizeColumns",
                    errFmtJoin(vectorizeSorts.stream().map(SortExpression::path).toList()));
              }));
    }

    var vectorizeSortExpression = vectorizeSorts.getFirst();
    var apiTableDef = tableSchemaObject.apiTableDef();
    var vectorColumnDef =
        apiTableDef.allColumns().get(vectorizeSortExpression.pathAsCqlIdentifier());

    // if there is no target vector column in table, just leave it for sort to fail
    if (vectorColumnDef == null) {
      return List.of();
    }

    if (vectorColumnDef.type().typeName() != ApiTypeName.VECTOR) {
      throw SortException.Code.CANNOT_VECTORIZE_SORT_NON_VECTOR_COLUMN.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("nonVectorColumn", errFmtApiColumnDef(List.of(vectorColumnDef)));
              }));
    }

    var vectorTypeDef = (ApiVectorType) vectorColumnDef.type();
    if (vectorTypeDef.getVectorizeDefinition() == null) {
      throw SortException.Code.CANNOT_VECTORIZE_SORT_WHEN_MISSING_VECTORIZE_DEFINITION.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("noVectorizeDefinition", errFmtApiColumnDef(List.of(vectorColumnDef)));
              }));
    }

    return List.of(
        new DataVectorizer.SortVectorizeTask(sortClause, vectorizeSortExpression, vectorColumnDef));
  }

  /**
   * Vectorize the '$vectorize' fields in the update clause
   *
   * @param updateClause - Update clause to be vectorized
   */
  private <T extends TableSchemaObject> List<DataVectorizer.VectorizeTask> taskforUpdate(
      T tableSchemaObject, UpdateClause updateClause) {
    if (updateClause == null) {
      return List.of();
    }

    //    "$set": {
    //      "vectorCol1" : "eat apple",
    //      "vectorCol2" : "eat orange"
    //    }
    var setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
    // no need to vectorize $unset, cause vector would be updated to null
    if (setNode == null) {
      return List.of();
    }

    // can reuse the tasksForInsert
    return tasksForVectorizeColumns(
        tableSchemaObject,
        List.of(setNode),
        UpdateException.Code.UNSUPPORTED_VECTORIZE_WHEN_MISSING_VECTORIZE_DEFINITION);
  }
}
