package io.stargate.sgv2.jsonapi.service.embedding;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.MeteredEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableVectorizeSortClauseResolver;
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
      DataApiRequestInfo dataApiRequestInfo, CommandContext<T> commandContext, Command command) {

    final DataVectorizer dataVectorizer =
        constructDataVectorizer(dataApiRequestInfo, commandContext);

    // TODO, This is the hack to make table vectorize failure goes into command process flow
    try {
      if (commandContext.schemaObject() instanceof TableSchemaObject) {
        return vectorizeTableCommand(dataVectorizer, commandContext.asTableContext(), command);
      }
    } catch (APIException e) {
      return Uni.createFrom().failure(e);
    }

    return vectorizeSortClause(dataVectorizer, commandContext, command)
        .onItem()
        .transformToUni(flag -> vectorizeDocument(dataVectorizer, commandContext, command))
        .onItem()
        .transform(flag -> command);
  }

  public <T extends SchemaObject> DataVectorizer constructDataVectorizer(
      DataApiRequestInfo dataApiRequestInfo, CommandContext<T> commandContext) {
    EmbeddingProvider embeddingProvider =
        Optional.ofNullable(commandContext.embeddingProvider())
            .map(
                provider ->
                    new MeteredEmbeddingProvider(
                        meterRegistry,
                        jsonApiMetricsConfig,
                        dataApiRequestInfo,
                        provider,
                        commandContext.commandName()))
            .orElse(null);
    return new DataVectorizer(
        embeddingProvider,
        objectMapper.getNodeFactory(),
        dataApiRequestInfo.getEmbeddingCredentials(),
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
              tasksForInsert(commandContext.schemaObject(), imc.documents());
          case InsertOneCommand ioc ->
              tasksForInsert(commandContext.schemaObject(), List.of(ioc.document()));
            //          case Sortable sortable -> tasksForSort(apiTableDef, sortable.sortClause());
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
  private <T extends TableSchemaObject> List<DataVectorizer.VectorizeTask> tasksForInsert(
      T tableSchemaObject, List<JsonNode> documents) {

    var apiTableDef = tableSchemaObject.apiTableDef();
    var vectorColumnDefs = apiTableDef.allColumns().filterByTypeToList(ApiTypeName.VECTOR);

    if (vectorColumnDefs.isEmpty()) {
      return List.of();
    }

    // get all the fields in the documents that are vector columns, and the value of the node is a
    // string, so we can check if they have a vectorize def.
    // key is the parent of the vector field, not the vector field itself
    Map<ObjectNode, ApiColumnDef> candidateVectorizeField = new HashMap<>();
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
          candidateVectorizeField.put((ObjectNode) document, vectorColumnDef);
        }
      }
    }

    // Now check that the columns actually have vectorize enabled.
    Set<String> nonVectorizeFieldNames = new HashSet<>();
    List<ApiColumnDef> supportedVectorizeFields = new ArrayList<>();
    for (var columnDef : candidateVectorizeField.values()) {
      if (((ApiVectorType) columnDef.type()).getVectorizeDefinition() == null) {
        nonVectorizeFieldNames.add(columnDef.jsonKey());
      } else {
        supportedVectorizeFields.add(columnDef);
      }
    }

    if (!nonVectorizeFieldNames.isEmpty()) {
      throw DocumentException.Code.INVALID_VECTORIZE_ON_COLUMN_WITHOUT_VECTORIZE_DEFINITION.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("validVectorizeColumns", errFmtApiColumnDef(supportedVectorizeFields));
                map.put("invalidVectorizeColumns", errFmtJoin(nonVectorizeFieldNames));
              }));
    }

    var vectorizeTasks =
        new ArrayList<DataVectorizer.VectorizeTask>(candidateVectorizeField.size());
    for (var entry : candidateVectorizeField.entrySet()) {
      var parentNode = entry.getKey();
      var vectorColumnDef = entry.getValue();

      // if the user sent a blank string for the vectorize field, we just turn that into a null
      // without vectorizing
      // but that only applies if the column has a vectorize definition so do it here not above
      var vectorizeText = parentNode.path(vectorColumnDef.jsonKey()).textValue();
      if (vectorizeText.isBlank()) {
        parentNode.putNull(vectorColumnDef.jsonKey());
      } else {
        vectorizeTasks.add(new DataVectorizer.VectorizeTask(parentNode, vectorColumnDef));
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

    // TODO, how to refactor and add this resolver to class scope
    var tableVectorizeSortClauseResolver = new TableVectorizeSortClauseResolver();
    var vectorizeSortExpression =
        tableVectorizeSortClauseResolver.resolve(commandContext, command).orElse(null);

    if (vectorizeSortExpression == null) {
      return List.of();
    }
    var vectorColumnDef =
        commandContext
            .schemaObject()
            .apiTableDef()
            .allColumns()
            .get(vectorizeSortExpression.pathAsCqlIdentifier());

    return List.of(
        new DataVectorizer.SortVectorizeTask(sortClause, vectorizeSortExpression, vectorColumnDef));
  }
}
