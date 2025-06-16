package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

public final class ModelUsage implements Recordable {

  public static final ModelUsage EMPTY = new ModelUsage(true);

  private final ModelProvider modelProvider;
  private final ModelType modelType;
  private final String modelName;
  private final String tenantId;
  private final ModelInputType inputType;
  private final int promptTokens;
  private final int totalTokens;
  private final int requestBytes;
  private final int responseBytes;
  private final long durationNanos;
  private final int batchCount;
  private final boolean isEmpty;

  public ModelUsage(
      ModelProvider modelProvider,
      ModelType modelType,
      String modelName,
      String tenantId,
      ModelInputType inputType,
      int promptTokens,
      int totalTokens,
      int requestBytes,
      int responseBytes,
      long durationNanos) {
    this(
        modelProvider,
        modelType,
        modelName,
        tenantId,
        inputType,
        promptTokens,
        totalTokens,
        requestBytes,
        responseBytes,
        durationNanos,
        1,
        false);
  }

  private ModelUsage(boolean isEmpty) {
    this(null, null, null, null, null, 0, 0, 0, 0, 0L, 0, false);
  }

  private ModelUsage(
      ModelProvider modelProvider,
      ModelType modelType,
      String modelName,
      String tenantId,
      ModelInputType inputType,
      int promptTokens,
      int totalTokens,
      int requestBytes,
      int responseBytes,
      long durationNanos,
      int batchCount,
      boolean isEmpty) {
    this.modelProvider = modelProvider;
    this.modelType = modelType;
    this.modelName = modelName;
    this.tenantId = tenantId;
    this.inputType = inputType;
    this.promptTokens = promptTokens;
    this.totalTokens = totalTokens;
    this.requestBytes = requestBytes;
    this.responseBytes = responseBytes;
    this.durationNanos = durationNanos;
    this.batchCount = batchCount;
    this.isEmpty = isEmpty;
  }

  public static ModelUsage fromEmbeddingGateway(EmbeddingGateway.ModelUsage grpcModelUsage) {

    return new ModelUsage(
        ModelProvider.fromApiName(grpcModelUsage.getModelProvider())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "ModelUsage() - Unknown grpcModelUsage.getModelProvider(): '%s'"
                            .formatted(grpcModelUsage.getModelProvider()))),
        ModelType.fromEmbeddingGateway(grpcModelUsage.getModelType())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "ModelUsage() - Unknown grpcModelUsage.getModelType(): '%s'"
                            .formatted(grpcModelUsage.getModelType()))),
        grpcModelUsage.getModelName(),
        grpcModelUsage.getTenantId(),
        ModelInputType.fromEmbeddingGateway(grpcModelUsage.getInputType())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Unknown Embedding Gateway modelInputType: "
                            + grpcModelUsage.getInputType())),
        grpcModelUsage.getPromptTokens(),
        grpcModelUsage.getTotalTokens(),
        grpcModelUsage.getRequestBytes(),
        grpcModelUsage.getResponseBytes(),
        grpcModelUsage.getCallDurationNanos());
  }

  public ModelUsage merge(ModelUsage other) {

    Objects.requireNonNull(other, "other must not be null");
    if (isEmpty && !other.isEmpty) {
      return other;
    }
    if (other.isEmpty && !isEmpty) {
      return this;
    }

    if (!this.modelProvider.equals(other.modelProvider)
        || !this.modelType.equals(other.modelType)
        || !this.modelName.equals(other.modelName)
        || !this.tenantId.equals(other.tenantId)
        || !this.inputType.equals(other.inputType)) {
      throw new IllegalArgumentException("Cannot merge ModelUsage with different properties");
    }

    return new ModelUsage(
        this.modelProvider,
        this.modelType,
        this.modelName,
        this.tenantId,
        this.inputType,
        this.promptTokens + other.promptTokens,
        this.totalTokens + other.totalTokens,
        this.requestBytes + other.requestBytes,
        this.responseBytes + other.responseBytes,
        this.durationNanos + other.durationNanos,
        this.batchCount + other.batchCount,
        false);
  }

  public boolean isEmpty() {
    return this.isEmpty;
  }

  public ModelProvider modelProvider() {
    return modelProvider;
  }

  public ModelType modelType() {
    return modelType;
  }

  public String modelName() {
    return modelName;
  }

  public String tenantId() {
    return tenantId;
  }

  public ModelInputType inputType() {
    return inputType;
  }

  public int promptTokens() {
    return promptTokens;
  }

  public int totalTokens() {
    return totalTokens;
  }

  public int requestBytes() {
    return requestBytes;
  }

  public int responseBytes() {
    return responseBytes;
  }

  public long durationNanos() {
    return durationNanos;
  }

  public int batchCount() {
    return batchCount;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("modelProvider", modelProvider)
        .append("modelType", modelType)
        .append("modelName", modelName)
        .append("tenantId", tenantId)
        .append("inputType", inputType)
        .append("promptTokens", promptTokens)
        .append("totalTokens", totalTokens)
        .append("requestBytes", requestBytes)
        .append("responseBytes", responseBytes)
        .append("durationNanos", durationNanos)
        .append("batchCount", batchCount)
        .append("isEmpty", isEmpty);
  }
}
