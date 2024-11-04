package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.data.CqlDuration;

/**
 * Helper class for converting {@link CqlDuration} to ISO-8601 duration string ("PnYnMnDTnHnMnS"),
 * translation not supported by {@link CqlDuration} itself even tho it can parse such strings.
 */
abstract class CqlDurationConverter {
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
  private static final long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

  /**
   * Convert {@link CqlDuration} to ISO-8601 duration string ("PnYnMnDTnHnMnS").
   *
   * @param value CqlDuration to convert to ISO-8601 duration string
   * @return ISO-8601 compliant duration string
   */
  public static String toISO8601Duration(CqlDuration value) {
    int months = value.getMonths();
    int days = value.getDays();
    long nanoSeconds = value.getNanoseconds();

    // Negative value? To detect see if any value negative (all must be 0 or negative
    // if one is -- but `CqlDuration` ensures that invariant). Bit silly there's no
    // `CqlDuration.isNegative()` (or `CqlDuration.negate()` to create opposite value)
    // or such but it is what it is.
    final StringBuilder sb;
    boolean negative = (months < 0) || (days < 0) || (nanoSeconds < 0L);
    if (negative) {
      // and if we do have negative value, we need to prepend minus sign, negate all parts
      sb = new StringBuilder("-P");
      months = -months;
      days = -days;
      nanoSeconds = -nanoSeconds;
    } else {
      sb = new StringBuilder("P");
    }

    // Do we have date part?
    if (months > 0 || days > 0) { // yes
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
    }
    // Check if we have time part
    if (nanoSeconds == 0L) {
      if (sb.length() == 1) {
        return "PT0S"; // All zeroes case
      }
      return sb.toString(); // Only date part exists
    }

    sb.append('T');
    long hours = nanoSeconds / NANOS_PER_HOUR;
    if (hours > 0L) {
      sb.append(hours).append('H');
      nanoSeconds -= hours * NANOS_PER_HOUR; // faster than modulo
    }

    long minutes = nanoSeconds / NANOS_PER_MINUTE;
    if (minutes > 0L) {
      sb.append(minutes).append('M');
      nanoSeconds -= minutes * NANOS_PER_MINUTE;
    }

    // Seconds more challenging due to possibility of fractional seconds
    if (nanoSeconds > 0L) {
      long seconds = nanoSeconds / NANOS_PER_SECOND;
      sb.append(seconds);
      final long fractionalNanos = nanoSeconds - (seconds * NANOS_PER_SECOND);
      if (fractionalNanos > 0L) {
        String nanoString = String.format("%09d", fractionalNanos);
        if (nanoString.endsWith("0")) {
          nanoString = nanoString.replaceAll("0+$", "");
        }
        sb.append('.').append(nanoString);
      }
      sb.append('S');
    }
    return sb.toString();
  }
}
