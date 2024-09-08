package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.util.Collection;

public class UnorderedCqlNamedValueContainer
    extends UnorderedNamedValueContainer<ColumnMetadata, Object, CqlNamedValue>
    implements CqlNamedValueContainer {

  public UnorderedCqlNamedValueContainer() {
    super();
  }

  public UnorderedCqlNamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public UnorderedCqlNamedValueContainer(CqlNamedValueContainer container) {
    super(container);
  }

  public UnorderedCqlNamedValueContainer(Collection<CqlNamedValue> values) {
    super();
    putAll(values);
  }
}
