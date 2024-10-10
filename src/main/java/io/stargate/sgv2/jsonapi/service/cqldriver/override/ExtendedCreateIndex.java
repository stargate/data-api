package io.stargate.sgv2.jsonapi.service.cqldriver.override;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateIndex;
import com.datastax.oss.driver.internal.querybuilder.schema.OptionsUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.UnmodifiableIterator;
import java.util.Map;

/**
 * An extension of the {@link DefaultCreateIndex} class, This is needed because the column name
 * appended to the builder needs to use `asCql(true)` to keep the quotes.
 */
public class ExtendedCreateIndex extends DefaultCreateIndex {

  public ExtendedCreateIndex(DefaultCreateIndex defaultCreateIndex) {

    super(
        defaultCreateIndex.getIndex(),
        defaultCreateIndex.isIfNotExists(),
        defaultCreateIndex.getKeyspace(),
        defaultCreateIndex.getTable(),
        defaultCreateIndex.getColumnToIndexType(),
        defaultCreateIndex.getUsingClass(),
        // This is fine as the internal options object is ImmutableMap
        (ImmutableMap<String, Object>) defaultCreateIndex.getOptions());
  }

  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("CREATE ");
    if (this.getUsingClass() != null) {
      builder.append("CUSTOM ");
    }

    builder.append("INDEX");
    if (this.isIfNotExists()) {
      builder.append(" IF NOT EXISTS");
    }

    if (this.getIndex() != null) {
      builder.append(' ').append(this.getIndex().asCql(true));
    }

    if (this.getTable() == null) {
      return builder.toString();
    } else {
      builder.append(" ON ");
      CqlHelper.qualify(this.getKeyspace(), this.getTable(), builder);
      if (this.getColumnToIndexType().isEmpty()) {
        return builder.toString();
      } else {
        builder.append(" (");
        boolean firstColumn = true;
        UnmodifiableIterator var3 = this.getColumnToIndexType().entrySet().iterator();

        while (var3.hasNext()) {
          Map.Entry<CqlIdentifier, String> entry = (Map.Entry) var3.next();
          if (firstColumn) {
            firstColumn = false;
          } else {
            builder.append(",");
          }

          if (((String) entry.getValue()).equals("__NO_INDEX_TYPE")) {
            builder.append(entry.getKey().asCql(true));
          } else {
            builder
                .append((String) entry.getValue())
                .append("(")
                .append(entry.getKey().asCql(true))
                .append(")");
          }
        }

        builder.append(")");
        if (this.getUsingClass() != null) {
          builder.append(" USING '").append(this.getUsingClass()).append('\'');
        }

        if (!this.getOptions().isEmpty()) {
          builder.append(OptionsUtils.buildOptions(this.getOptions(), true));
        }

        return builder.toString();
      }
    }
  }
}
