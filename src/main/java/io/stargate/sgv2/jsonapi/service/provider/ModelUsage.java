package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

/**
 * Usage of a model, any model, recorded for billing or metrics purposes.
 *
 * <p>When doing batching, create one instance and then use {@link #merge(ModelUsage)} to combine.
 * Note that the durations are added , use the batchCount to get average duration.
 */
public final class ModelUsage implements Recordable {

  private final ModelProvider modelProvider;
  private final ModelType modelType;
  private final String modelName;
  private final Tenant tenant;
  private final ModelInputType inputType;
  private final int promptTokens;
  private final int totalTokens;
  private final int requestBytes;
  private final int responseBytes;
  private final long durationNanos;
  private final int batchCount;

  public ModelUsage(
      ModelProvider modelProvider,
      ModelType modelType,
      String modelName,
      Tenant tenant,
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
        tenant,
        inputType,
        promptTokens,
        totalTokens,
        requestBytes,
        responseBytes,
        durationNanos,
        1);
  }

  private ModelUsage(
      ModelProvider modelProvider,
      ModelType modelType,
      String modelName,
      Tenant tenant,
      ModelInputType inputType,
      int promptTokens,
      int totalTokens,
      int requestBytes,
      int responseBytes,
      long durationNanos,
      int batchCount) {
    this.modelProvider = Objects.requireNonNull(modelProvider, "modelProvider must not be null");
    this.modelType = Objects.requireNonNull(modelType, "modelType must not be null");
    this.modelName = Objects.requireNonNull(modelName, "modelName must not be null");
    this.tenant = Objects.requireNonNull(tenant, "tenant must not be null");
    this.inputType = Objects.requireNonNull(inputType, "inputType must not be null");
    if (promptTokens < 0) {
      throw new IllegalArgumentException("promptTokens must not be negative");
    }
    this.promptTokens = promptTokens;
    if (totalTokens < 0) {
      throw new IllegalArgumentException("totalTokens must not be negative");
    }
    this.totalTokens = totalTokens;
    if (requestBytes < 0) {
      throw new IllegalArgumentException("requestBytes must not be negative");
    }
    this.requestBytes = requestBytes;
    if (responseBytes < 0) {
      throw new IllegalArgumentException("responseBytes must not be negative");
    }
    this.responseBytes = responseBytes;
    if (durationNanos < 0) {
      throw new IllegalArgumentException("durationNanos must not be negative");
    }
    this.durationNanos = durationNanos;
    if (batchCount < 1) {
      throw new IllegalArgumentException("batchCount must be at least 1");
    }
    this.batchCount = batchCount;
  }

  /** Create a ModelUsage from an EmbeddingGateway.ModelUsage. grpc object */
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
        TenantFactory.instance().create( grpcModelUsage.getTenantId()),
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

  public EmbeddingGateway.ModelUsage toEmbeddingGateway() {
    return EmbeddingGateway.ModelUsage.newBuilder()
        .setModelProvider(modelProvider.apiName())
        .setModelType(modelType.toEmbeddingGateway())
        .setModelName(modelName)
        .setTenantId(tenant.toString())
        .setInputType(inputType.toEmbeddingGateway())
        .setPromptTokens(promptTokens)
        .setTotalTokens(totalTokens)
        .setRequestBytes(requestBytes)
        .setResponseBytes(responseBytes)
        .setCallDurationNanos(durationNanos)
        .build();
  }

  /**
   * Creates a new model usage that merges this and the other usage, to combine after batching.
   *
   * @return A new ModelUsage instance that combines the properties of this and the other usage.
   */
  public ModelUsage merge(ModelUsage other) {

    Objects.requireNonNull(other, "other must not be null");
    if (!this.modelProvider.equals(other.modelProvider)
        || !this.modelType.equals(other.modelType)
        || !this.modelName.equals(other.modelName)
        || !this.tenant.equals(other.tenant)
        || !this.inputType.equals(other.inputType)) {
      throw new IllegalArgumentException(
          "Cannot merge ModelUsage with different properties, this: %s, other: %s"
              .formatted(PrettyPrintable.print(this), PrettyPrintable.print(other)));
    }

    return new ModelUsage(
        this.modelProvider,
        this.modelType,
        this.modelName,
        this.tenant,
        this.inputType,
        this.promptTokens + other.promptTokens,
        this.totalTokens + other.totalTokens,
        this.requestBytes + other.requestBytes,
        this.responseBytes + other.responseBytes,
        this.durationNanos + other.durationNanos,
        this.batchCount + other.batchCount);
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

  public Tenant tenant() {
    return tenant;
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
        .append("tenant", tenant)
        .append("inputType", inputType)
        .append("promptTokens", promptTokens)
        .append("totalTokens", totalTokens)
        .append("requestBytes", requestBytes)
        .append("responseBytes", responseBytes)
        .append("durationNanos", durationNanos)
        .append("batchCount", batchCount);
  }
}
