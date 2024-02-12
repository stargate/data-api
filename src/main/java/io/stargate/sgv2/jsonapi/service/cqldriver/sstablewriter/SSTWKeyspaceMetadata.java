package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public class SSTWKeyspaceMetadata implements KeyspaceMetadata {

  private final String keyspaceName;
  private final Map<CqlIdentifier, TableMetadata> tables;

  public SSTWKeyspaceMetadata(String keyspaceName, Map<CqlIdentifier, TableMetadata> tables) {
    this.keyspaceName = keyspaceName;
    this.tables = tables;
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return CqlIdentifier.fromCql(keyspaceName);
  }

  @Override
  public boolean isDurableWrites() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isVirtual() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<String, String> getReplication() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, TableMetadata> getTables() {
    return this.tables;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, ViewMetadata> getViews() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<FunctionSignature, FunctionMetadata> getFunctions() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<FunctionSignature, AggregateMetadata> getAggregates() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
