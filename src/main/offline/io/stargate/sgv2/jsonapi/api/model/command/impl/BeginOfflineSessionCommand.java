package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.CreateCollectionCommandResolver;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Representation of the offline begin-writer API {@link Command}. */
@Schema(description = "Command that initializes the offline writer.")
@JsonTypeName("beginOfflineSession")
public class BeginOfflineSessionCommand implements CollectionCommand {
  @Schema(
      description = "The namespace to write to.",
      type = SchemaType.STRING,
      implementation = String.class)
  private final String namespace;

  @Schema(
      name = "createCollection",
      description = "Create collection command.",
      type = SchemaType.OBJECT,
      implementation = CreateCollectionCommand.class)
  private final CreateCollectionCommand createCollection;

  @Schema(
      description = "The SSTable output directory.",
      type = SchemaType.STRING,
      implementation = String.class)
  private final String ssTableOutputDirectory;

  @Schema(
      description = "The file writer buffer size.",
      type = SchemaType.INTEGER,
      implementation = Integer.class)
  private final int fileWriterBufferSizeInMB;

  @JsonIgnore private final CollectionSchemaObject collectionSettings;
  @JsonIgnore private final String sessionId;
  @JsonIgnore private final FileWriterParams fileWriterParams;

  /**
   * fileWriterBufferSize Constructs a new {@link BeginOfflineSessionCommand}.
   *
   * @param namespace the namespace to write to
   * @param createCollection the create collection command
   * @param ssTableOutputDirectory the SSTable output directory
   */
  public BeginOfflineSessionCommand(
      String namespace,
      CreateCollectionCommand createCollection,
      String ssTableOutputDirectory,
      int fileWriterBufferSizeInMB) {
    this.namespace = namespace;
    this.createCollection = createCollection;
    this.ssTableOutputDirectory = ssTableOutputDirectory;
    this.sessionId = UUID.randomUUID().toString();
    this.fileWriterBufferSizeInMB = fileWriterBufferSizeInMB;
    this.collectionSettings = buildCollectionSettings();
    // AARON : passing the collectionSettings after refactor because I am wrorried about the order
    // the state is built
    this.fileWriterParams = buildFileWriterParams(collectionSettings);

    // TODO: move a lot of this logic into the command resovler, the command is meant to be a basic
    // POJO of state of the
    // command, it should not have all this behaviour. e.g. things we need to mark as @JSONIGNORE
  }

  private CollectionSchemaObject buildCollectionSettings() {
    boolean isVectorEnabled =
        this.createCollection.options() != null && this.createCollection.options().vector() != null;
    int vectorSize =
        (isVectorEnabled && this.createCollection.options().vector().dimension() != null)
            ? this.createCollection.options().vector().dimension()
            : 0;
    CollectionSchemaObject.SimilarityFunction similarityFunction =
        isVectorEnabled
            ? CollectionSchemaObject.SimilarityFunction.fromString(
                this.createCollection.options().vector().metric())
            : null;
    VectorConfig.VectorizeConfig vectorizeConfig =
        isVectorEnabled
            ? toCollectionSettingsVectorizeConfig(
                this.createCollection.options().vector().vectorizeConfig())
            : null;
    VectorConfig vectorConfig =
        isVectorEnabled
            ? new VectorConfig(isVectorEnabled, vectorSize, similarityFunction, null)
            : null;
    return new CollectionSchemaObject(
        namespace,
        createCollection.name(),
        toCollectionSettingsIdConfig(
            this.createCollection.options() != null
                ? this.createCollection.options().idConfig()
                : null),
        vectorConfig,
        toCollectionSettingsIndexing(
            this.createCollection.options() != null
                ? this.createCollection.options().indexing()
                : null));
  }

  private CollectionSchemaObject.IndexingConfig toCollectionSettingsIndexing(
      CreateCollectionCommand.Options.IndexingConfig indexing) {
    if (indexing == null) {
      return null;
    }
    return new CollectionSchemaObject.IndexingConfig(
        indexing.allow() != null ? new HashSet<>(indexing.allow()) : null,
        indexing.deny() != null ? new HashSet<>(indexing.deny()) : null);
  }

  private CollectionSchemaObject.IdConfig toCollectionSettingsIdConfig(
      CreateCollectionCommand.Options.IdConfig idConfig) {
    if (idConfig == null) return null;
    // TODO-SL: check if idConfig.idType() is null and handle accordingly
    return new CollectionSchemaObject.IdConfig(
        CollectionSchemaObject.IdType.fromString(idConfig.idType()));
  }

  private VectorConfig.VectorizeConfig toCollectionSettingsVectorizeConfig(
      CreateCollectionCommand.Options.VectorSearchConfig.VectorizeConfig vectorize) {
    if (vectorize == null) {
      return null;
    }
    String provider = vectorize.provider();
    String model = vectorize.modelName();
    return new VectorConfig.VectorizeConfig(
        provider, model, vectorize.authentication(), vectorize.parameters());
  }

  private FileWriterParams buildFileWriterParams(CollectionSchemaObject collectionObject) {

    CreateCollectionCommand.Options createCollectionOptions = this.createCollection.options();
    CreateCollectionCommand.Options.IndexingConfig indexingConfig =
        createCollectionOptions != null ? createCollectionOptions.indexing() : null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig =
        createCollectionOptions != null ? createCollectionOptions.vector() : null;
    CreateCollectionCommand.Options.IdConfig idConfig =
        createCollectionOptions != null ? createCollectionOptions.idConfig() : null;
    boolean hasIndexing = indexingConfig != null;
    boolean hasVector = vectorSearchConfig != null;

    var tableCommandContext =
        CommandContext.forSchemaObject(collectionObject, null, this.createCollection.name(), null);

    var keyspaceCommandContext =
        CommandContext.forSchemaObject(
            KeyspaceSchemaObject.fromSchemaObject(collectionObject),
            null,
            this.createCollection.name(),
            null);

    String comment =
        CreateCollectionCommandResolver.generateComment(
            new ObjectMapper(),
            hasIndexing,
            hasVector,
            this.getClass().getSimpleName(),
            indexingConfig,
            vectorSearchConfig,
            idConfig);
    CreateCollectionOperation createCollectionOperation =
        hasVector
            ? CreateCollectionOperation.withVectorSearch(
                keyspaceCommandContext,
                null,
                new ObjectMapper(),
                null,
                this.createCollection.name(),
                this.createCollection.options().vector().dimension() != null
                    ? this.createCollection.options().vector().dimension()
                    : 0,
                this.createCollection.options().vector().metric(),
                comment,
                0,
                false,
                false)
            : CreateCollectionOperation.withoutVectorSearch(
                keyspaceCommandContext,
                null,
                new ObjectMapper(),
                null,
                this.createCollection.name(),
                comment,
                0,
                false,
                false);
    String createTableCQL =
        createCollectionOperation
            .getCreateTable(this.namespace, this.createCollection.name())
            .getQuery();
    List<String> indexCQLs =
        createCollectionOperation
            .getIndexStatements(this.namespace, this.createCollection.name(), false)
            .stream()
            .map(SimpleStatement::getQuery)
            .toList();
    InsertOperation insertOperation =
        InsertOperation.create(tableCommandContext, List.of(), true, true, false);
    String insertStatementCQL = insertOperation.buildInsertQuery(hasVector);
    return new FileWriterParams(
        this.namespace,
        this.createCollection.name(),
        this.ssTableOutputDirectory,
        fileWriterBufferSizeInMB,
        createTableCQL,
        insertStatementCQL,
        indexCQLs,
        hasVector);
  }

  public CollectionSchemaObject getCollectionSchemaObject() {
    return this.collectionSettings;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public FileWriterParams getFileWriterParams() {
    return this.fileWriterParams;
  }
}
