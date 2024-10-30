package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlDuration;

abstract class CqlDurationConverter {
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
  private static final long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

  public static String toISO8601Duration(CqlDuration value) {
    int months = value.getMonths();
    int days = value.getDays();
    long nanoseconds = value.getNanoseconds();

    StringBuilder sb;

    // Do we have date part?
    if (months > 0 || days > 0) {
      sb = new StringBuilder("P");
      final int years = months / 12;
      months = months % 12;

      if (years > 0) {
        sb.append(years).append("Y");
      }
      if (months > 0) {
        sb.append(months).append("M");
      }
      if (days > 0) {
        sb.append(days).append("D");
      }

      // Are we done?
      if (nanoseconds == 0L) {
        return sb.toString();
      }

    } else { // No date part
      // Minor optimization: if all fields are zero, return the smallest possible duration
      if (nanoseconds == 0L) {
        return "PT0S";
      }
      sb = new StringBuilder("P");
    }

    // At this point we know we have a time part, i.e. nanoseconds > 0
    sb.append('T');

    long hours = nanoseconds / NANOS_PER_HOUR;
    if (hours > 0L) {
      sb.append(hours).append('H');
      nanoseconds -= hours * NANOS_PER_HOUR; // faster than modulo
    }

    long minutes = nanoseconds / NANOS_PER_MINUTE;
    if (minutes > 0L) {
      sb.append(minutes).append('M');
      nanoseconds -= minutes * NANOS_PER_MINUTE;
    }

    if (nanoseconds > 0L) {
      long seconds = nanoseconds / NANOS_PER_SECOND;
      if (seconds > 0L) {
        sb.append(seconds).append('S');
        nanoseconds -= seconds * NANOS_PER_SECOND;
      }
      // !!! TODO: fractions
    }
    return sb.toString();
  }
}
