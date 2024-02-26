package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import java.util.UUID;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Representation of the offline begin-writer API {@link Command}. */
@Schema(description = "Command that initializes the offline writer.")
@JsonTypeName("offlineBeginWriter")
public class OfflineBeginWriterCommand implements Command {
  @Schema(
      description = "The namespace to write to.",
      type = SchemaType.STRING,
      implementation = String.class)
  private final String namespace;

  @Schema(
      description = "The namespace to write to.",
      type = SchemaType.OBJECT,
      implementation = CreateCollectionCommand.class)
  private final CreateCollectionCommand createCollectionCommand;

  @Schema(
      description = "The SSTable output directory.",
      type = SchemaType.STRING,
      implementation = String.class)
  private final String ssTableOutputDirectory;

  @JsonIgnore private final CollectionSettings collectionSettings;
  @JsonIgnore private final EmbeddingService embeddingService;
  @JsonIgnore private final String sessionId;
  @JsonIgnore private final FileWriterParams fileWriterParams;

  /**
   * Constructs a new {@link OfflineBeginWriterCommand}.
   *
   * @param namespace the namespace to write to
   * @param createCollectionCommand the create collection command
   * @param ssTableOutputDirectory the SSTable output directory
   */
  public OfflineBeginWriterCommand(
      String namespace,
      CreateCollectionCommand createCollectionCommand,
      String ssTableOutputDirectory) {
    this.namespace = namespace;
    this.createCollectionCommand = createCollectionCommand;
    this.ssTableOutputDirectory = ssTableOutputDirectory;
    this.collectionSettings = buildCollectionSettings();
    this.embeddingService = null; // TODO
    this.sessionId = UUID.randomUUID().toString();
    this.fileWriterParams = buildFileWriterParams();
  }

  private CollectionSettings buildCollectionSettings() {
    boolean isVectorEnabled =
        this.createCollectionCommand.options() != null
            && this.createCollectionCommand.options().vector() != null;
    int vectorSize =
        isVectorEnabled ? this.createCollectionCommand.options().vector().dimension() : 0;
    CollectionSettings.SimilarityFunction similarityFunction =
        isVectorEnabled
            ? CollectionSettings.SimilarityFunction.fromString(
                this.createCollectionCommand.options().vector().metric())
            : null;

    return CollectionSettings.getCollectionSettings(
        this.createCollectionCommand.name(),
        isVectorEnabled,
        vectorSize,
        similarityFunction,
        null, // TODO
        new ObjectMapper()); // TODO
  }

  private FileWriterParams buildFileWriterParams() {
    String createTableCQL =
        CreateCollectionOperation.forCQL(
                this.collectionSettings.vectorEnabled(),
                this.collectionSettings.vectorSize(),
                null) // TODO-SL fix comments field
            .getCreateTable(this.namespace, this.createCollectionCommand.name())
            .getQuery();
    String insertStatementCQL =
        InsertOperation.forCQL(
                new CommandContext(this.namespace, this.createCollectionCommand.name()))
            .buildInsertQuery(
                this.collectionSettings.vectorEnabled()); // TODO-SL add conditionalInsert to method
    return new FileWriterParams(
        this.namespace,
        this.getCreateCollectionCommand().name(),
        this.getSsTableOutputDirectory(),
        createTableCQL,
        insertStatementCQL);
  }

  public String getNamespace() {
    return namespace;
  }

  public CreateCollectionCommand getCreateCollectionCommand() {
    return createCollectionCommand;
  }

  public String getSsTableOutputDirectory() {
    return ssTableOutputDirectory;
  }

  public CollectionSettings getCollectionSettings() {
    return this.collectionSettings;
  }

  public EmbeddingService getEmbeddingService() {
    return this.embeddingService;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public FileWriterParams getFileWriterParams() {
    return this.fileWriterParams;
  }
}
