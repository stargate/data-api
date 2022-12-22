package io.stargate.sgv3.docsapi.service.shredding.model;

import com.google.common.base.Splitter;
import java.util.Objects;

/**
 * Model for any path into the JSON document. Not *true* JsonPath as it is not designed for all
 * querying https://github.com/json-path/JsonPath
 *
 * <p>Examples are "_id" or "user.fist_name"
 *
 * <p>The python lab overloads this with ideas like "my_array.$size"
 *
 * <p>NOTE: this is a partial implementation of what was in the python lab, more will be needed to
 * do the full shredding work.
 */
public class JsonPath {

  private static final Splitter PATH_SPLITTER = Splitter.on(".").trimResults().omitEmptyStrings();
  public static final JsonPath ROOT = new JsonPath(null, "$");

  private JsonPath prev;
  private String name;
  private String path;

  public JsonPath(JsonPath prev, String name) {
    this.prev = prev;
    this.name = name;

    // Build the path now because it is immutable
    // We do not include the root "$" in the path, that is something for queries not what we need in
    // the DB

    if (prev != null && !JsonPath.ROOT.equals(prev)) {
      path = String.join(".", prev.path, name);
    } else {
      path = name;
    }
  }

  public static JsonPath from(String path) {
    JsonPath current = null;

    for (String part : PATH_SPLITTER.splitToList(path)) {
      current = (current == null) ? new JsonPath(JsonPath.ROOT, part) : new JsonPath(current, part);
    }
    assert current != null;
    return current;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    JsonPath other = (JsonPath) obj;
    if (path == null) {
      if (other.path != null) return false;
    } else if (!path.equals(other.path)) return false;
    return true;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }
}
