package io.stargate.sgv2.jsonapi.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.CacheTestsBase;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Tests for schema lookups through {@link SchemaObjectCache}. */
public class SchemaObjectCacheTests extends CacheTestsBase {

  @Test
  public void concurrentTableLookupsRecoverFromCollectionMismatch() throws Exception {

    var requestContext = mock(RequestContext.class);
    when(requestContext.tenant()).thenReturn(TEST_CONSTANTS.TENANT);
    var tableIdentifier = TEST_CONSTANTS.TABLE_IDENTIFIER;
    var collectionIdentifier =
        SchemaObjectIdentifier.forCollection(
            tableIdentifier.tenant(), tableIdentifier.keyspace(), tableIdentifier.table());
    var table = new TableSchemaObject(tableIdentifier);
    var collectionLoad = new CompletableFuture<SchemaObject>();
    var mismatch =
        new SchemaObjectFactory.SchemaObjectTypeMismatchException(
            SchemaObjectType.COLLECTION, SchemaObjectType.TABLE);
    var factory = mock(SchemaObjectCache.SchemaObjectFactory.class);
    when(factory.apply(requestContext, collectionIdentifier, false)).thenReturn(collectionLoad);
    when(factory.apply(requestContext, tableIdentifier, false))
        .thenReturn(CompletableFuture.completedFuture(table));

    var cache =
        new SchemaObjectCache(
            CACHE_MAX_SIZE,
            LONG_TTL,
            TEST_CONSTANTS.SLA_USER_AGENT,
            SHORT_TTL,
            factory,
            new SimpleMeterRegistry(),
            true,
            new FakeTicker());

    var first =
        cache
            .getTableBased(requestContext, tableIdentifier, TEST_CONSTANTS.USER_AGENT, false)
            .subscribeAsCompletionStage();

    try (var executor = Executors.newSingleThreadExecutor()) {
      try {
        // The second lookup must return without blocking on the pending collection load.
        var second =
            executor
                .submit(
                    () ->
                        cache
                            .getTableBased(
                                requestContext, tableIdentifier, TEST_CONSTANTS.USER_AGENT, false)
                            .subscribeAsCompletionStage())
                .get(5, TimeUnit.SECONDS);

        collectionLoad.completeExceptionally(mismatch);

        assertThat(first.toCompletableFuture().get(5, TimeUnit.SECONDS)).isSameAs(table);
        assertThat(second.toCompletableFuture().get(5, TimeUnit.SECONDS)).isSameAs(table);
        verify(factory).apply(requestContext, collectionIdentifier, false);
        verify(factory).apply(requestContext, tableIdentifier, false);
        verifyNoMoreInteractions(factory);
      } finally {
        // Release a blocked lookup even if an assertion fails.
        collectionLoad.completeExceptionally(mismatch);
      }
    }
  }
}
