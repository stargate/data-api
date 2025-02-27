package io.stargate.sgv2.jsonapi.util;

public interface Recordable {

  DataRecorder recordTo(DataRecorder dataRecorder);

  default DataRecorder recordToSubRecorder(DataRecorder dataRecorder) {
    return recordTo(dataRecorder.beginSubRecorder(getClass())).endSubRecorder();
  }

  abstract class DataRecorder {

    protected final Class<?> clazz;
    protected final boolean pretty;
    protected final DataRecorder parent;

    protected DataRecorder(Class<?> clazz, boolean pretty) {
      this(clazz, pretty, null);
    }

    protected DataRecorder(Class<?> clazz, boolean pretty, DataRecorder parent) {
      this.clazz = clazz;
      this.pretty = pretty;
      this.parent = parent;
    }

    protected static String className(Class<?> clazz) {
      // See getSimpleName() may be an empty string at times, use the full name in these cases
      var simpleName = clazz.getSimpleName();
      return simpleName.isEmpty() ? clazz.getName() : simpleName;
    }

    public abstract DataRecorder beginSubRecorder(Class<?> clazz);

    public abstract DataRecorder endSubRecorder();

    public abstract DataRecorder append(String key, Object value);
  }
}
