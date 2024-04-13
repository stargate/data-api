package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.CreateCollectionCommandResolver;
import java.util.List;
import java.util.UUID;
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

  @JsonIgnore private final CollectionSettings collectionSettings;
  @JsonIgnore private final EmbeddingProvider embeddingProvider;
  @JsonIgnore private final String sessionId;
  @JsonIgnore private final FileWriterParams fileWriterParams;
  @JsonIgnore private String comment;

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
      EmbeddingProvider embeddingProvider,
      int fileWriterBufferSizeInMB) {
    this.namespace = namespace;
    this.createCollection = createCollection;
    this.ssTableOutputDirectory = ssTableOutputDirectory;
    this.embeddingProvider = embeddingProvider;
    this.sessionId = UUID.randomUUID().toString();
    this.fileWriterBufferSizeInMB = fileWriterBufferSizeInMB;
    this.fileWriterParams = buildFileWriterParams();
    this.collectionSettings = buildCollectionSettings();
  }

  private CollectionSettings buildCollectionSettings() {
    boolean isVectorEnabled =
        this.createCollection.options() != null && this.createCollection.options().vector() != null;
    int vectorSize =
        (isVectorEnabled && this.createCollection.options().vector().dimension() != null)
            ? this.createCollection.options().vector().dimension()
            : 0;
    CollectionSettings.SimilarityFunction similarityFunction =
        isVectorEnabled
            ? CollectionSettings.SimilarityFunction.fromString(
                this.createCollection.options().vector().metric())
            : null;

    return CollectionSettings.getCollectionSettings(
        this.createCollection.name(),
        isVectorEnabled,
        vectorSize,
        similarityFunction,
        this.comment,
        new ObjectMapper());
  }

  private FileWriterParams buildFileWriterParams() {
    CreateCollectionCommand.Options createCollectionOptions = this.createCollection.options();
    CreateCollectionCommand.Options.IndexingConfig indexingConfig =
        createCollectionOptions != null ? createCollectionOptions.indexing() : null;
    CreateCollectionCommand.Options.VectorSearchConfig vectorSearchConfig =
        createCollectionOptions != null ? createCollectionOptions.vector() : null;
    CreateCollectionCommand.Options.IdConfig idConfig =
        createCollectionOptions != null ? createCollectionOptions.idConfig() : null;
    boolean hasIndexing = indexingConfig != null;
    boolean hasVector = vectorSearchConfig != null;
    this.comment =
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
                new CommandContext(this.namespace, this.createCollection.name()),
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
                new CommandContext(this.namespace, this.createCollection.name()),
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
            .getIndexStatements(this.namespace, this.createCollection.name())
            .stream()
            .map(SimpleStatement::getQuery)
            .toList();
    InsertOperation insertOperation =
        new InsertOperation(
            new CommandContext(this.namespace, this.createCollection.name()),
            List.of(),
            true,
            true);
    String insertStatementCQL = insertOperation.buildInsertQuery(hasVector);
    return new FileWriterParams(
        this.namespace,
        this.getCreateCollectionCommand().name(),
        this.getSsTableOutputDirectory(),
        fileWriterBufferSizeInMB,
        createTableCQL,
        insertStatementCQL,
        indexCQLs,
        hasVector);
  }

  public String getNamespace() {
    return namespace;
  }

  public CreateCollectionCommand getCreateCollectionCommand() {
    return createCollection;
  }

  public String getSsTableOutputDirectory() {
    return ssTableOutputDirectory;
  }

  public CollectionSettings getCollectionSettings() {
    return this.collectionSettings;
  }

  public EmbeddingProvider getEmbeddingProvider() {
    return this.embeddingProvider;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public FileWriterParams getFileWriterParams() {
    return this.fileWriterParams;
  }
}
