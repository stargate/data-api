package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlDuration;

abstract class CqlDurationConverter {
  public static String toISO8601Duration(CqlDuration value) {
    return value.toString();
  }
}
