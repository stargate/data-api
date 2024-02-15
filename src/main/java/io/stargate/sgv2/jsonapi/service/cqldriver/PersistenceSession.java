package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.SSTableWriterSession;
import io.stargate.sgv2.jsonapi.service.processor.SSTableWriterStatus;
import java.util.concurrent.CompletionStage;

public class PersistenceSession {
  private final Object session;
  private final SessionType sessionType;

  public PersistenceSession(Object session) {
    this.session = session;
    if (session instanceof CqlSession) {
      this.sessionType = SessionType.CqlSession;
    } else if (session instanceof SSTableWriterSession) {
      this.sessionType = SessionType.SSTableWriterSession;
    } else {
      throw new IllegalArgumentException("Invalid session type");
    }
  }

  public void close() {
    switch (sessionType) {
      case CqlSession -> ((CqlSession) session).close();
      case SSTableWriterSession -> ((SSTableWriterSession) session).close();
      default -> throw new UnsupportedOperationException("Unknown session type: " + sessionType);
    }
  }

  public CompletionStage<AsyncResultSet> executeAsync(SimpleStatement simpleStatement) {
    return switch (sessionType) {
      case CqlSession -> ((CqlSession) session).executeAsync(simpleStatement);
      case SSTableWriterSession -> ((SSTableWriterSession) session).executeAsync(simpleStatement);
      default -> throw new UnsupportedOperationException("Unknown session type: " + sessionType);
    };
  }

  public Metadata getMetadata() {
    return switch (sessionType) {
      case CqlSession -> ((CqlSession) session).getMetadata();
      case SSTableWriterSession -> ((SSTableWriterSession) session).getMetadata();
      default -> throw new UnsupportedOperationException("Unknown session type: " + sessionType);
    };
  }

  public DriverContext getContext() {
    return switch (sessionType) {
      case CqlSession -> ((CqlSession) session).getContext();
      case SSTableWriterSession -> ((SSTableWriterSession) session).getContext();
      default -> throw new UnsupportedOperationException("Unknown session type: " + sessionType);
    };
  }

  public SSTableWriterStatus getStatus() {
    return switch (sessionType) {
      case CqlSession -> throw new UnsupportedOperationException(
          "getStatus not supported by : " + sessionType);
      case SSTableWriterSession -> ((SSTableWriterSession) session).getStatus();
      default -> throw new UnsupportedOperationException("Unknown session type: " + sessionType);
    };
  }

  public void setNamespace(String namespace) {
    if (sessionType == SessionType.SSTableWriterSession) {
      ((SSTableWriterSession) session).setNamespace(namespace);
    }
    throw new UnsupportedOperationException("Unknown session type: " + sessionType);
  }

  public String getNamespace() {
    if (sessionType == SessionType.SSTableWriterSession) {
      return ((SSTableWriterSession) session).getNamespace();
    }
    throw new UnsupportedOperationException("Unknown session type: " + sessionType);
  }

  public void setCollection(String collection) {
    if (sessionType == SessionType.SSTableWriterSession) {
      ((SSTableWriterSession) session).setCollection(collection);
    }
    throw new UnsupportedOperationException("Unknown session type: " + sessionType);
  }

  public String getCollection() {
    if (sessionType == SessionType.SSTableWriterSession) {
      return ((SSTableWriterSession) session).getCollection();
    }
    throw new UnsupportedOperationException("Unknown session type: " + sessionType);
  }
}
