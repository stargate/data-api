package io.stargate.sgv2.jsonapi.service.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

@ApplicationScoped
public class MeteredCommandProcessor {

  private static final String UNKNOWN_VALUE = "unknown";

  private static final String NA = "NA";

  private final CommandProcessor commandProcessor;

  private final MeterRegistry meterRegistry;

  private final StargateRequestInfo stargateRequestInfo;

  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;

  /** The tag for error being true, created only once. */
  private final Tag errorTrue;

  /** The tag for error being false, created only once. */
  private final Tag errorFalse;

  /** The tag for tenant being unknown, created only once. */
  private final Tag tenantUnknown;

  private final Tag defaultErrorCode;

  private final Tag defaultErrorClass;

  @Inject
  public MeteredCommandProcessor(
      CommandProcessor commandProcessor,
      MeterRegistry meterRegistry,
      StargateRequestInfo stargateRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      MetricsConfig metricsConfig) {
    this.commandProcessor = commandProcessor;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    tenantConfig = metricsConfig.tenantRequestCounter();
    this.stargateRequestInfo = stargateRequestInfo;
    errorTrue = Tag.of(tenantConfig.errorTag(), "true");
    errorFalse = Tag.of(tenantConfig.errorTag(), "false");
    tenantUnknown = Tag.of(tenantConfig.tenantTag(), UNKNOWN_VALUE);
    defaultErrorCode = Tag.of(jsonApiMetricsConfig.errorCode(), NA);
    defaultErrorClass = Tag.of(jsonApiMetricsConfig.errorClass(), NA);
  }

  /**
   * Processes a single command in a given command context.
   *
   * @param commandContext {@link CommandContext}
   * @param command {@link Command}
   * @return Uni emitting the result of the command execution.
   * @param <T> Type of the command.
   */
  public <T extends Command> Uni<CommandResult> processCommand(
      CommandContext commandContext, T command) {
    Timer.Sample sample = Timer.start(meterRegistry);
    // start by resolving the command, get resolver
    Log.error("inside");
    return commandProcessor
        .processCommand(commandContext, command)
        .onItem()
        .invoke(
            result -> {
              Tags tags = getCustomTags(commandContext, command, result);
              // add metrics
              sample.stop(meterRegistry.timer(jsonApiMetricsConfig.metricsName(), tags));
            });
  }

  /**
   * Generate custom tags based on the command and result.
   *
   * @param command - request command
   * @param result - response command result
   * @return
   */
  private Tags getCustomTags(CommandContext commandContext, Command command, CommandResult result) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), command.getClass().getSimpleName());
    String tenant = stargateRequestInfo.getTenantId().orElse(UNKNOWN_VALUE);
    Tag tenantTag = Tag.of(tenantConfig.tenantTag(), tenant);
    Tag errorTag = errorFalse;
    Tag errorClassTag = defaultErrorClass;
    Tag errorCodeTag = defaultErrorCode;
    // if error is present, add error tags else use defaults
    if (null != result.errors() && !result.errors().isEmpty()) {
      errorTag = errorTrue;
      String errorClass =
          (String) result.errors().get(0).fields().getOrDefault("exceptionClass", UNKNOWN_VALUE);
      errorClassTag = Tag.of(jsonApiMetricsConfig.errorClass(), errorClass);
      String errorCode =
          (String) result.errors().get(0).fields().getOrDefault("errorCode", UNKNOWN_VALUE);
      errorCodeTag = Tag.of(jsonApiMetricsConfig.errorCode(), errorCode);
    }

    Tag vectorEnabled =
        commandContext.isVectorEnabled()
            ? Tag.of(jsonApiMetricsConfig.vectorEnabled(), "true")
            : Tag.of(jsonApiMetricsConfig.vectorEnabled(), "false");
    JsonApiMetricsConfig.SortType sortType = getVectorTypeTag(command);
    Tag sortTypeTag = Tag.of(jsonApiMetricsConfig.sortType(), sortType.name());
    Tags tags =
        Tags.of(
            commandTag,
            tenantTag,
            errorTag,
            errorClassTag,
            errorCodeTag,
            vectorEnabled,
            sortTypeTag);
    return tags;
  }

  private JsonApiMetricsConfig.SortType getVectorTypeTag(Command command) {
    int filterCount = 0;
    if (command instanceof Filterable fc && fc.filterClause() != null) {
      filterCount = fc.filterClause().comparisonExpressions().size();
    }
    if (command instanceof Sortable sc
        && sc.sortClause() != null
        && !sc.sortClause().sortExpressions().isEmpty()) {
      if (sc.sortClause() != null) {
        List<SortExpression> sortClause = sc.sortClause().sortExpressions();
        if (sortClause.size() == 1
            && (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(sortClause.get(0).path())
                || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(
                    sortClause.get(0).path()))) {
          if (filterCount == 0) {
            return JsonApiMetricsConfig.SortType.SIMILARITY_SORT;
          } else {
            return JsonApiMetricsConfig.SortType.SIMILARITY_SORT_WITH_FILTERS;
          }
        } else {
          return JsonApiMetricsConfig.SortType.SORT_BY_FIELD;
        }
      }
    }
    return JsonApiMetricsConfig.SortType.NONE;
  }
}
