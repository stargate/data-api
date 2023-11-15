package io.stargate.sgv2.jsonapi.config;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Converts a string to a {@link ConsistencyLevel}, used in {@link OperationsConfig.QueriesConfig}.
 */
public class ConsistencyLevelConverter implements Converter<ConsistencyLevel> {
  /**
   * @param value the string representation of a property value
   * @return the converted ConsistencyLevel
   */
  @Override
  public ConsistencyLevel convert(String value)
      throws IllegalArgumentException, NullPointerException {
    return DefaultConsistencyLevel.valueOf(value.toUpperCase());
  }
}
