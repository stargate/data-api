package io.stargate.sgv2.jsonapi.util.recordable;

import java.util.*;
import java.util.function.Supplier;

/**
 * Recordable objects provide a snapshot of their data, that is intended to be used in logs,
 * tracing, and other areas where we do not want a full serialisation or the basic toString.
 *
 * <p>For example detailed trace messages about the data we got from the DB, or a hierarchical
 * taskGroup for a composite pipeline. See {@link PrettyPrintable} and {@link Jsonable} for
 * examples.
 *
 * <p>Classes should implement the {@link Recordable#recordTo(DataRecorder)} and call {@link
 * DataRecorder#append(String, Object)} for each key value. If they pass a Recordable object it will
 * automatically be recorded as a sub object and it's implementations called.
 *
 * <p>If you have a collection of Recordable objects, call {@link Recordable#copyOf(Collection)} to
 * wrap them in a recordable object that will be recorded as an array of objects.
 */
@FunctionalInterface
public interface Recordable {

  /**
   * Called for the implementer to record its data to the {@link DataRecorder}, values should be
   * appended using {@link DataRecorder#append(String, Object)}. Values that implement {@link
   * Recordable} will be added as a sub object.
   *
   * @param dataRecorder The recorder to append the data to.
   * @return The chained result of calling recordTo
   */
  DataRecorder recordTo(DataRecorder dataRecorder);

  /**
   * Convenience method to record to a new {@link DataRecorder} that is a sub recorder of the given
   * {@link DataRecorder}. Called when the object is a sub object of another object.
   *
   * @param dataRecorder The parent recorder to record to.
   * @return The chained result of calling recordTo
   */
  default DataRecorder recordToSubRecorder(DataRecorder dataRecorder) {
    return recordTo(dataRecorder.beginSubRecorder(getClass())).endSubRecorder();
  }

  /**
   * Wraps a collection of Recordable objects in a Recordable object that will be recorded as an
   * array of objects. So you do not need collections to implement Recordable.
   *
   * <p>For example:
   *
   * <pre>
   * commandContext
   *     .requestTracing()
   *     .maybeTrace("Parsed JSON Documents", Recordable.copyOf(parsedDocuments));
   * </pre>
   *
   * @param values The collection of Recordable objects to wrap.
   * @return A Recordable object that will record the collection as an array of objects.
   */
  static Recordable copyOf(Collection<?> values) {
    return new RecordableCollection(values);
  }

  static Supplier<Recordable> collectionSupplier(Supplier<Collection<?>> values) {
    return () -> copyOf(values.get());
  }

  static Recordable copyOf(Map<?, ?> values) {
    return new RecordableMap(values);
  }

  static Supplier<Recordable> mapSupplier(Supplier<Map<?, ?>> values) {
    return () -> copyOf(values.get());
  }

  /**
   * A {@link DataRecorder} is used to record the data from a {@link Recordable} object.
   *
   * <p>Classes that want to serialise {@link Recordable} objects needs to implement a DataRecorder
   * that can be used to record the data.
   */
  abstract class DataRecorder {

    protected final Class<?> clazz;
    protected final DataRecorder parent;

    protected DataRecorder(Class<?> clazz) {
      this(clazz, null);
    }

    protected DataRecorder(Class<?> clazz, DataRecorder parent) {
      this.clazz = clazz;
      this.parent = parent;
    }

    /** getSimpleName() may be an empty string at times, use the full name in these cases */
    protected static String className(Class<?> clazz) {
      var simpleName = clazz.getSimpleName();
      return simpleName.isEmpty() ? clazz.getName() : simpleName;
    }

    /**
     * Called when a new sub object is being recorded, to get a new {@link DataRecorder}
     * implementation.
     *
     * @param clazz The class of the sub object
     * @return The new {@link DataRecorder} for the sub object
     */
    public abstract DataRecorder beginSubRecorder(Class<?> clazz);

    /**
     * Called when the sub object has been fully recorded, to return the parent {@link
     * DataRecorder}.
     *
     * @return The parent {@link DataRecorder}
     */
    public abstract DataRecorder endSubRecorder();

    /**
     * Records the named value to the data recorder, the value may be a {@link Recordable} object
     * which will result in a sub object being created.
     *
     * @param key String name of the value to include in the recording.
     * @param value The value to record, may be a {@link Recordable} object.
     * @return The chained result of calling append
     */
    public abstract DataRecorder append(String key, Object value);
  }

  class RecordableCollection extends ArrayList<Object> implements Recordable {
    RecordableCollection(Collection<?> recordables) {
      super(recordables);
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      // create a new list to avoid recursive calls
      return dataRecorder.append(null, List.copyOf(this));
    }
  }

  class RecordableMap extends HashMap<Object, Object> implements Recordable {
    RecordableMap(Map<?, ?> values) {
      super(values);
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      var recorder = dataRecorder;
      for (var entry : entrySet()) {
        recorder = recorder.append(entry.getKey().toString(), entry.getValue());
      }
      return recorder;
    }
  }
}
