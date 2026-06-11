package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;

/**
 * Extended vector type to support vector size.
 *
 * <p>Basically a clone of {@link DefaultVectorType} but changes the {@link #asCql} override.
 */
public class ExtendedVectorType extends DefaultVectorType {
  public ExtendedVectorType(DataType subtype, int vectorSize) {
    super(subtype, vectorSize);
  }

  @Override
  public String asCql(boolean includeFrozen, boolean pretty) {
    // NOTE: this is very similar to the DefaultVectorType.asCql() method, the difference
    // is passing along the includeFrozen and pretty parameters. Default sets them to true
    // which means frozen is included in places we dont want it.
    return String.format(
        "vector<%s, %d>", getElementType().asCql(includeFrozen, pretty), getDimensions());
  }
}
