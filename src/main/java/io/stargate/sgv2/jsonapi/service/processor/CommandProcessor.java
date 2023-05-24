package io.stargate.sgv2.jsonapi.service.processor;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConstants;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Processes valid document {@link Command} to read, write, schema change, etc. This is a single
 * entry to run a Command without worrying how to.
 *
 * <p>Called from the API layer which deals with public JSON data formats, the command layer
 * translates from the JSON models to internal and back (shredding and de-shredding).
 *
 * <p>May provide a thread or resource boundary from calling API layer.
 */
@ApplicationScoped
public class CommandProcessor {

  private final MeterRegistry meterRegistry;

  private final QueryExecutor queryExecutor;

  private final CommandResolverService commandResolverService;

  private final StargateRequestInfo stargateRequestInfo;

  @Inject
  public CommandProcessor(
      MeterRegistry meterRegistry,
      QueryExecutor queryExecutor,
      CommandResolverService commandResolverService,
      StargateRequestInfo stargateRequestInfo) {
    this.meterRegistry = meterRegistry;
    this.queryExecutor = queryExecutor;
    this.commandResolverService = commandResolverService;
    this.stargateRequestInfo = stargateRequestInfo;
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
    final Tag commandTag = Tag.of(MetricsConstants.COMMAND, command.getClass().getSimpleName());
    final String tenant =
        stargateRequestInfo != null && stargateRequestInfo.getTenantId().isPresent()
            ? stargateRequestInfo.getTenantId().get()
            : MetricsConstants.UNKNOWN_VALUE;
    final Tag tenantTag = Tag.of(MetricsConstants.TENANT, tenant);
    final long start = System.currentTimeMillis();
    return Uni.createFrom()
        .item(start)
        .onItem()
        .transformToUni(
            startTime -> {
              // start by resolving the command, get resolver
              return commandResolverService
                  .resolverForCommand(command)

                  // resolver can be null, not handled in CommandResolverService for now
                  .flatMap(
                      resolver -> {
                        // if we have resolver, resolve operation and execute
                        Operation operation = resolver.resolveCommand(commandContext, command);
                        return operation.execute(queryExecutor);
                      })

                  // handler failures here
                  .onItemOrFailure()
                  .transform(
                      (item, failure) -> {
                        if (failure != null) {
                          if (failure instanceof JsonApiException jsonApiException) {
                            return jsonApiException;
                          }
                          return new ThrowableCommandResultSupplier(failure);

                        } else {
                          // return the tags and command result
                          return item;
                        }
                      })
                  .onItem()
                  .ifNotNull()
                  .transform(Supplier::get)
                  .onItem()
                  .transform(
                      result -> {
                        Tag errorTag = MetricsConstants.ERROR_FALSE_TAG;
                        ;
                        Tag errorClassTag = MetricsConstants.DEFAULT_ERROR_CLASS_TAG;
                        Tag errorCodeTag = MetricsConstants.DEFAULT_ERROR_CODE_TAG;
                        if (null != result.errors() && !result.errors().isEmpty()) {
                          errorTag = MetricsConstants.ERROR_TRUE_TAG;
                          String errorClass =
                              (String)
                                  result
                                      .errors()
                                      .get(0)
                                      .fields()
                                      .getOrDefault(
                                          MetricsConstants.ERROR_CLASS,
                                          MetricsConstants.UNKNOWN_VALUE);
                          errorClassTag = Tag.of(MetricsConstants.ERROR_CLASS, errorClass);
                          String errorCode =
                              (String)
                                  result
                                      .errors()
                                      .get(0)
                                      .fields()
                                      .getOrDefault(
                                          MetricsConstants.ERROR_CODE,
                                          MetricsConstants.UNKNOWN_VALUE);
                          errorCodeTag = Tag.of(MetricsConstants.ERROR_CODE, errorCode);
                        }
                        Tags tags =
                            Tags.of(commandTag, tenantTag, errorTag, errorClassTag, errorCodeTag);
                        // add metrics
                        meterRegistry
                            .counter(MetricsConstants.COUNT_METRICS_NAME, tags)
                            .increment();
                        meterRegistry
                            .timer(MetricsConstants.TIMER_METRICS_NAME, tags)
                            .record(
                                System.currentTimeMillis() - start,
                                java.util.concurrent.TimeUnit.MILLISECONDS);
                        // return the command result
                        return result;
                      });
            });
  }
}
