package com.thinkmorestupidless.ankka.flow.flink;

import java.io.Serializable;
import java.util.Arrays;

/** A record header: a key and its bytes. */
public record Header(String key, byte[] value) implements Serializable {
  @Override
  public boolean equals(Object o) {
    return o instanceof Header h && h.key.equals(key) && Arrays.equals(h.value, value);
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + Arrays.hashCode(value);
  }

  @Override
  public String toString() {
    return "Header(" + key + ", " + value.length + " bytes)";
  }
}
