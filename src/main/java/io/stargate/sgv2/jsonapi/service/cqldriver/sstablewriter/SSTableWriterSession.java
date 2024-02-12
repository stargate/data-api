package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import java.util.concurrent.CompletionStage;

public class SSTableWriterSession {
  private final SSTWSessionMetadata metadata;

  public SSTableWriterSession(SSTWSessionMetadata metadata) {
    this.metadata = metadata;
  }

  public CompletionStage<AsyncResultSet> executeAsync(SimpleStatement simpleStatement) {
    // TODO
    return null;
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public DriverContext getContext() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public void close() {
  }
}
