package io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun;

import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public record TestUri(
    Scheme scheme,
    List<SegmentValue> segments) {

  public static TestUri.Builder builder(Scheme scheme){
    return new  TestUri.Builder(scheme);
  }

  public Segment leafType(){
    return segments.getLast().segment;
  }

  public URI uri() {
    var path = segments.stream()
        .map(SegmentValue::toString)
        .collect(joining("/"));
    return URI.create(scheme.name() + "://" + path);
  }

  public static TestUri parse(URI uri) {

    var builder = builder(Scheme.valueOf(uri.getScheme().toLowerCase()));
    SegmentValue.parse(uri)
        .forEach(builder::addSegment);
    return builder.build();
  }

  public enum Scheme {
    TESTRUN;

    public String pathName() {
      return name().toLowerCase();
    }
  }

  public enum Segment {
    TARGET(null),
    LIFECYCLE(null), // used in multiple places
    WORKFLOW(TARGET),
    JOB(WORKFLOW),
    SUITE(JOB),
    ENV(SUITE),
    STAGE(ENV),
    REQUEST(STAGE),
    COMMAND(REQUEST),
    ASSERTION_CONTAINER(REQUEST),
    ASSERTION(ASSERTION_CONTAINER);

    private final Segment parent;

    Segment(Segment parent) {
      this.parent = parent;
    }

    public Segment parent() {
      return parent;
    }

    public boolean isParentValid(Segment segment) {
      // XXX TODO: needs work
      return true;
      //return (parent == null) || (parent == segment) || (segment == LIFECYCLE) ;
    }

    public String pathName() {
      return name().toLowerCase();
    }
  }

  public record SegmentValue(Segment segment, String value) {

    private static final Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9\\-_.~]");

    public SegmentValue{
      Objects.requireNonNull(segment, "segment must not be null");
      Objects.requireNonNull(value, "value must not be null");
    }

    public static Stream<SegmentValue> parse(URI uri) {
      return Arrays.stream(uri.getPath().split("/"))
          .map(SegmentValue::parse);
    }

    public static SegmentValue parse(String segmentKeyValue) {
      var parts = segmentKeyValue.split("=", 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid segment, expected key=value format: " + segmentKeyValue);
      }
      try {
        return new SegmentValue(Segment.valueOf(parts[0].toUpperCase()), parts[1]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unknown segment key: " + parts[0]);
      }
    }

    @Override
    public String toString() {
      return segment.name() + "=" + INVALID_CHARS.matcher(value).replaceAll("_");
    }
  }


  public static class Builder {

    private final Scheme scheme;
    private final List<SegmentValue> segmentValues;

    protected Builder(Scheme scheme) {
      this(scheme, new ArrayList<>());
    }

    private Builder(Scheme scheme, List<SegmentValue> segmentValues) {
      this.scheme = Objects.requireNonNull(scheme, "scheme must not be null");
      this.segmentValues = Objects.requireNonNull(segmentValues, "segmentValues must not be null");
    }

    public Builder addSegment(SegmentValue segmentValue) {
      segmentValues.add(segmentValue);
      return this;
    }

    public Builder addSegment(Segment segment, String value) {
      segmentValues.add(new SegmentValue(segment, value));
      return this;
    }

    public Builder clone(){
      return new Builder(scheme, new ArrayList<>(segmentValues));
    }

    public TestUri build() {
      for (int i = 0; i < segmentValues.size(); i++) {
        var current = segmentValues.get(i);
        var previous = i == 0 ? null : segmentValues.get(i - 1).segment();
        if (!current.segment().isParentValid(previous)) {
          throw new IllegalArgumentException(
              "Invalid segment order. segment=%s expected parent=%s but previous=%s"
                  .formatted(current.segment(), current.segment().parent(), previous));

        }
      }
      return new TestUri(scheme, List.copyOf(segmentValues));
    }
  }

}
