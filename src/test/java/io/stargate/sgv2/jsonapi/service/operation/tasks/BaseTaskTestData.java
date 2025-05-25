package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.mockito.Mockito.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.time.Duration;

public class BaseTaskTestData {

  private static final Tenant TENANT = Tenant.create(DatabaseType.ASTRA, "test-tenant");

  public BaseTaskTestData() {}

  public BaseTaskAssertions<
          BaseTaskTestTask, TableSchemaObject, BaseTask.UniSupplier<String>, String>
      defaultTask() {
    return createAssertions(null);
  }

  public BaseTaskAssertions<
          BaseTaskTestTask, TableSchemaObject, BaseTask.UniSupplier<String>, String>
      taskWithOneRetry() {
    var retryPolicy =
        new TaskRetryPolicy(1, Duration.ofMillis(1)) {
          @Override
          public boolean shouldRetry(Throwable throwable) {
            return true;
          }
        };

    return createAssertions(retryPolicy);
  }

  private BaseTaskAssertions<
          BaseTaskTestTask, TableSchemaObject, BaseTask.UniSupplier<String>, String>
      createAssertions(TaskRetryPolicy retryPolicy) {

    if (retryPolicy == null) {
      retryPolicy = TaskRetryPolicy.NO_RETRY;
    }

    var mockTable =
        BaseTaskAssertions.mockTable(TENANT,
            "keyspace" + System.currentTimeMillis(), "table" + System.currentTimeMillis());
    CommandContext<TableSchemaObject> mockCommandContext = mock(CommandContext.class);

    // spy() the attempt so we get default behaviour and can track calls to the methods
    var task = spy(new BaseTaskTestTask(0, mockTable, retryPolicy));

    return new BaseTaskAssertions<>(task, mockCommandContext);
  }
}
