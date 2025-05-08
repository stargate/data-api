package io.stargate.sgv2.jsonapi.service.processor;

import static io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants.MetricTags.ERROR_CODE;
import static io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants.MetricTags.EXCEPTION_CLASS;
import static io.stargate.sgv2.jsonapi.config.constants.LoggingConstants.*;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.micrometer.core.instrument.*;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A processor that wraps the core {@link CommandProcessor} to add metrics and command-level logging
 * capabilities.
 *
 * <p>It measures the execution time of commands, collects various tags (like command name, tenant,
 * errors, vector usage), and logs command details based on configuration.
 */
@ApplicationScoped
public class MeteredCommandProcessor {

  private static final Logger logger = LoggerFactory.getLogger(MeteredCommandProcessor.class);

  // ObjectWriter for serializing CommandLog instances.
  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

  // Constants for tag values
  private static final String UNKNOWN_VALUE = "unknown";
  private static final String NA = "NA";

  // Core processor dependency
  private final CommandProcessor commandProcessor;

  // Metrics and configuration dependencies
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;
  private final CommandLevelLoggingConfig commandLevelLoggingConfig;

  // Pre-computed common tags for efficiency
  private final Tag errorTrue;
  private final Tag errorFalse;
  private final Tag tenantUnknown;
  private final Tag defaultErrorCode;
  private final Tag defaultErrorClass;

  @Inject
  public MeteredCommandProcessor(
      CommandProcessor commandProcessor,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      MetricsConfig metricsConfig,
      CommandLevelLoggingConfig commandLevelLoggingConfig) {
    this.commandProcessor = commandProcessor;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.tenantConfig = metricsConfig.tenantRequestCounter();
    this.commandLevelLoggingConfig = commandLevelLoggingConfig;

    // Pre-compute common tags for efficiency
    errorTrue = Tag.of(tenantConfig.errorTag(), "true");
    errorFalse = Tag.of(tenantConfig.errorTag(), "false");
    tenantUnknown = Tag.of(tenantConfig.tenantTag(), UNKNOWN_VALUE);
    defaultErrorCode = Tag.of(jsonApiMetricsConfig.errorCode(), NA);
    defaultErrorClass = Tag.of(jsonApiMetricsConfig.errorClass(), NA);
  }

  /**
   * Processes a single command, adding metrics and logging around the core execution.
   *
   * @param commandContext {@link CommandContext} The context for the command execution, containing
   *     schema, request info, etc.
   * @param command The {@link Command} to be processed.
   * @param <CommandT> Type of the command.
   * @param <SchemaT> Type of the schema object.
   * @return A Uni emitting the {@link CommandResult} of the command execution.
   */
  public <CommandT extends Command, SchemaT extends SchemaObject> Uni<CommandResult> processCommand(
      CommandContext<SchemaT> commandContext, CommandT command) {

    Timer.Sample sample = Timer.start(meterRegistry);

    // Set up logging context (MDC)
    // use MDC to populate logs as needed(namespace,collection,tenantId)
    commandContext.schemaObject().name().addToMDC();
    MDC.put("tenantId", commandContext.requestContext().getTenantId().orElse(UNKNOWN_VALUE));

    // --- Defer Command Processing (from PR2076) ---
    // We wrap the call to `commandProcessor.processCommand` in `Uni.createFrom().deferred()`
    // for two main reasons:
    // 1. Defensive Programming for Synchronous Failures:
    //    Ensures that if `commandProcessor.processCommand` itself (or code executed synchronously
    //    within it before its own reactive chain fully forms and handles errors) throws a
    //    synchronous exception, this MeteredCommandProcessor can still catch it in its
    //    `.onFailure()` block for consistent logging (no metrics currently). This acts as a safety
    //    net.
    // 2. Lazy Execution (Benefit of `deferred`):
    //    The `commandProcessor.processCommand` method, which kicks off potentially significant
    //    work and returns a Uni, will only be invoked when this resulting Uni is actually
    //    subscribed to.
    return Uni.createFrom()
        .deferred(() -> commandProcessor.processCommand(commandContext, command))
        .onItem()
        .invoke(
            // Success path handling
            result -> {
              // --- Metrics Recording ---
              Tags tags = getCustomTags(commandContext, command, result);
              sample.stop(meterRegistry.timer(jsonApiMetricsConfig.metricsName(), tags));

              // --- Command Level Logging (Success) ---
              if (isCommandLevelLoggingEnabled(commandContext, result, false)) {
                logger.info(buildCommandLog(commandContext, command, result));
              }
            })
        .onFailure()
        .invoke(
            // Failure path handling.
            // This block will only be executed if the Uni returned by
            // commandProcessor.processCommand terminates with a failure signal that was not caught
            // and recovered by the .recoverWithItem() block inside commandProcessor.processCommand.
            // That is, this should not happen unless there are unexpected errors before/in the
            // .recoverWithItem() or there are some framework errors
            throwable -> {
              // TODO: Metrics timer (`sample.stop()`) is not called here by design?

              // --- Command Level Logging
              if (isCommandLevelLoggingEnabled(commandContext, null, true)) {
                logger.error(
                    "Command processing failed. Details: {}",
                    buildCommandLog(commandContext, command, null),
                    throwable);
              }
            })
        .eventually(
            () -> {
              // Cleanup MDC after processing completes (success or failure) to prevent data from
              // leaking into the next request handled by the same thread.
              commandContext.schemaObject().name().removeFromMDC();
              MDC.remove("tenantId");
            });
  }

  /**
   * Builds the command level log in string format.
   *
   * @param commandContext Command context
   * @param command Command
   * @param result Command result
   * @return Command log in string format
   */
  private <T extends SchemaObject> String buildCommandLog(
      CommandContext<T> commandContext, Command command, CommandResult result) {
    CommandLog commandLog =
        new CommandLog(
            command.getClass().getSimpleName(),
            commandContext.requestContext().getTenantId().orElse(UNKNOWN_VALUE),
            commandContext.schemaObject().name().keyspace(),
            commandContext.schemaObject().name().table(),
            commandContext.schemaObject().type().name(),
            getIncomingDocumentsCount(command),
            getOutgoingDocumentsCount(result),
            result != null ? result.errors() : Collections.emptyList());
    try {
      return OBJECT_WRITER.writeValueAsString(commandLog);
    } catch (JacksonException e) {
      return "ERROR: Failed to serialize CommandLog instance, cause = " + e;
    }
  }

  /**
   * Counts outgoing documents from a {@link CommandResult}.
   *
   * @param result The command result.
   * @return Document count as a String, or "NA" if not applicable or result is null.
   */
  private String getOutgoingDocumentsCount(CommandResult result) {
    if (result == null || result.data() == null) {
      return NA;
    }
    // Check specific ResponseData types that contain documents
    if (result.data() instanceof ResponseData.MultiResponseData
        || result.data() instanceof ResponseData.SingleResponseData) {
      return String.valueOf(result.data().getResponseDocuments().size());
    }
    // Other types don't have outgoing docs in this sense
    return NA;
  }

  /**
   * Counts incoming documents for relevant commands (InsertOne, InsertMany).
   *
   * @param command The command being executed.
   * @return Document count as a String, or "NA" if not applicable.
   */
  private String getIncomingDocumentsCount(Command command) {
    return switch (command) {
      case InsertManyCommand insertManyCmd ->
          String.valueOf(insertManyCmd.documents() != null ? insertManyCmd.documents().size() : 0);
      case InsertOneCommand insertOneCmd -> String.valueOf(insertOneCmd.document() != null ? 1 : 0);
      default ->
          // Command types without relevant incoming documents
          NA;
    };
  }

  /**
   * Checks if command-level logging should be performed based on configuration and result status.
   *
   * @param commandContext Command context (used for tenant filtering).
   * @param commandResult The result (used for error filtering), can be null for failure path.
   * @param isFailure Indicates if this check is being done during the failure handling path.
   * @return {@code true} if logging should proceed, {@code false} otherwise.
   */
  private boolean isCommandLevelLoggingEnabled(
      CommandContext<?> commandContext, CommandResult commandResult, boolean isFailure) {
    // Globally disabled?
    if (!commandLevelLoggingConfig.enabled()) {
      return false;
    }

    // Check tenant filter (if configured)
    Set<String> allowedTenants =
        commandLevelLoggingConfig.enabledTenants().orElse(Collections.singleton(ALL_TENANTS));
    if (!allowedTenants.contains(ALL_TENANTS)
        && !allowedTenants.contains(
            commandContext.requestContext().getTenantId().orElse(UNKNOWN_VALUE))) {
      // Logging disabled for this tenant
      return false;
    }

    // Disabled if no errors in command
    if (!isFailure
        && commandLevelLoggingConfig.onlyResultsWithErrors()
        && (commandResult == null
            || commandResult.errors() == null
            || commandResult.errors().isEmpty())) {
      return false;
    }

    // return true in all other cases
    return true;
  }

  /**
   * Generates metric tags based on the command, context, and result.
   *
   * @param commandContext The command context.
   * @param command The executed command.
   * @param result The result of the command execution (contains data and errors).
   * @param <T> Type of the schema object.
   * @return A set of Micrometer {@link Tags}.
   */
  private <T extends SchemaObject> Tags getCustomTags(
      CommandContext<T> commandContext, Command command, CommandResult result) {
    // --- Basic Tags ---
    // Identify the command being executed and the tenant associated with the request
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), command.getClass().getSimpleName());
    String tenant = commandContext.requestContext().getTenantId().orElse(UNKNOWN_VALUE);
    Tag tenantTag = Tag.of(tenantConfig.tenantTag(), tenant);

    // --- Error Tags ---
    // Determine if the command resulted in an error and capture details
    Tag errorTag = errorFalse;
    Tag errorClassTag = defaultErrorClass;
    Tag errorCodeTag = defaultErrorCode;
    if (result != null && null != result.errors() && !result.errors().isEmpty()) {
      errorTag = errorTrue;
      // Extract details from the first error object's metric fields.
      // TODO: Assumption use the first error is representative for metrics?
      var metricFields = result.errors().getFirst().fieldsForMetricsTag();

      // Safely extract error class and code, defaulting to UNKNOWN_VALUE
      String errorClass = (String) metricFields.getOrDefault(EXCEPTION_CLASS, UNKNOWN_VALUE);
      errorClassTag = Tag.of(jsonApiMetricsConfig.errorClass(), errorClass);
      String errorCode = (String) metricFields.getOrDefault(ERROR_CODE, UNKNOWN_VALUE);
      errorCodeTag = Tag.of(jsonApiMetricsConfig.errorCode(), errorCode);
    }

    // --- Schema Feature Tags ---
    // Indicate if the collection/table has vector search enabled in its schema
    Tag vectorEnabled =
        Tag.of(
            jsonApiMetricsConfig.vectorEnabled(),
            Boolean.toString(commandContext.schemaObject().vectorConfig().vectorEnabled()));

    // --- Sort Type Tag ---
    // Determine the type of sorting used (if any), primarily for FindCommand.
    // NOTE: This logic might need refinement or replacement when FeatureUsage is fully integrated,
    // especially for FindAndRerankCommand.
    JsonApiMetricsConfig.SortType sortType = getVectorTypeTag(commandContext, command);
    Tag sortTypeTag = Tag.of(jsonApiMetricsConfig.sortType(), sortType.name());

    // --- Combine All Tags ---
    return Tags.of(
        commandTag, tenantTag, errorTag, errorClassTag, errorCodeTag, vectorEnabled, sortTypeTag);
  }

  /**
   * Determines the {@link JsonApiMetricsConfig.SortType} of sorting used in the command based on
   * the command's sort clause. Primarily intended for vector-based sorting.
   *
   * @param commandContext The command context.
   * @param command The command being executed.
   * @return The type of sorting used (if any).
   */
  private JsonApiMetricsConfig.SortType getVectorTypeTag(
      CommandContext<?> commandContext, Command command) {
    // Get the count of filter conditions applied
    int filterCount = 0;
    if (command instanceof Filterable filterable) {
      filterCount = filterable.filterClause(commandContext).size();
    }

    // Check if the command supports sorting and has a sort clause defined
    if (command instanceof Sortable sc
        && !sc.sortClause(commandContext).sortExpressions().isEmpty()) {

      var sortExpressions = sc.sortClause(commandContext).sortExpressions();

      // Check if the only sort expression is for vector similarity ($vector or $vectorize)
      if (sortExpressions.size() == 1) {
        String sortPath = sortExpressions.getFirst().path();
        if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(sortPath) // $vector
            || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(sortPath)) // $vectorize
        {
          // It's a pure vector similarity sort and no filters were applied
          if (filterCount == 0) {
            return JsonApiMetricsConfig.SortType.SIMILARITY_SORT;
          }
          // Filters were applied alongside the vector sort
          return JsonApiMetricsConfig.SortType.SIMILARITY_SORT_WITH_FILTERS;
        }
      }
      // If more than one sort expression, or the single one isn't $vector/$vectorize
      return JsonApiMetricsConfig.SortType.SORT_BY_FIELD;
    }

    // Default if no sorting is detected or applicable
    return JsonApiMetricsConfig.SortType.NONE;
  }
}
