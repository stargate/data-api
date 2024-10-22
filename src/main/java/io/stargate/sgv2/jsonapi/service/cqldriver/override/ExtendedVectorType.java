package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;

/**
 * Extended vector type to support vector size This is needed because java drivers
 * DataTypes.vectorOf() method has a bug
 */
public class ExtendedVectorType extends DefaultVectorType {
  public ExtendedVectorType(DataType subtype, int vectorSize) {
    super(subtype, vectorSize);
  }

  @Override
  public String asCql(boolean includeFrozen, boolean pretty) {
    return "VECTOR<" + getElementType().asCql(includeFrozen, pretty) + "," + getDimensions() + ">";
  }
}
