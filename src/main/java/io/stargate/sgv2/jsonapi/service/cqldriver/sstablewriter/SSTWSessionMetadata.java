package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class SSTWSessionMetadata implements Metadata {

  public static SSTWSessionMetadata forTable(String tableDef) {
    return null;
    // TODO
  }

  @NonNull
  @Override
  public Map<UUID, Node> getNodes() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
    return null;
  }

  @NonNull
  @Override
  public Optional<TokenMap> getTokenMap() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
