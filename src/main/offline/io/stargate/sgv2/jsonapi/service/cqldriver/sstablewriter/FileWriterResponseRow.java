package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Row} implementation that is used to represent the response of a write operation to an
 * SSTable file
 */
public class FileWriterResponseRow implements Row {
  /* The default codec registry */
  private final CodecRegistry DEFAULT_CODECS = new DefaultCodecRegistry("data-api-offline");
  /* The column definitions */
  private final ColumnDefinitions columnDefs;
  /* The index */
  private final int index;
  /* The values */
  private final List<ByteBuffer> values;

  public FileWriterResponseRow(ColumnDefinitions columnDefs, int index, List<ByteBuffer> values) {
    this.columnDefs = columnDefs;
    this.index = index;
    this.values = values;
  }

  @Override
  public int size() {
    return columnDefs.size();
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    return DEFAULT_CODECS;
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    return DefaultProtocolVersion.V5;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return columnDefs;
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull String name) {
    return columnDefs.allIndicesOf(name);
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    return columnDefs.firstIndexOf(name);
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    return columnDefs.allIndicesOf(id);
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    return columnDefs.firstIndexOf(id);
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return columnDefs.get(i).getType();
  }

  @NonNull
  @Override
  public DataType getType(@NonNull String name) {
    return columnDefs.get(name).getType();
  }

  @NonNull
  @Override
  public DataType getType(@NonNull CqlIdentifier id) {
    return columnDefs.get(id).getType();
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values.get(i);
  }

  @Override
  public boolean isDetached() {
    return true;
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileWriterResponseRow that = (FileWriterResponseRow) o;
    return index == that.index
        && Objects.equals(columnDefs, that.columnDefs)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnDefs, index, values);
  }
}
