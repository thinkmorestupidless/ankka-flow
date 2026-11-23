package com.thinkmorestupidless.ankka.flow.flink;

import java.io.Serializable;
import java.util.List;

/**
 * What a port carries: a key (null when the record has none), a value of the port's type, the
 * record's headers, and its Kafka timestamp in milliseconds since the epoch.
 */
public record Record<A>(String key, A value, List<Header> headers, long timestamp)
    implements Serializable {

  public Record {
    headers = headers == null ? List.of() : List.copyOf(headers);
  }

  /** A record with no headers. */
  public static <A> Record<A> of(String key, A value, long timestamp) {
    return new Record<>(key, value, List.of(), timestamp);
  }

  /** The same key, headers and timestamp with another value. */
  public <B> Record<B> withValue(B other) {
    return new Record<>(key, other, headers, timestamp);
  }
}
