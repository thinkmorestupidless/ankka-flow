package com.thinkmorestupidless.ankka.flow.flink;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * What a port carries, by name, as ankka names a contract. With a schema (a classpath resource,
 * usually a JSON Schema) the descriptor also carries the schema's fingerprint.
 */
public final class Contract implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final String schema;

  private Contract(String name, String schema) {
    this.name = Objects.requireNonNull(name, "name");
    this.schema = schema;
  }

  public static Contract of(String name) {
    return new Contract(name, null);
  }

  public static Contract of(String name, String schemaResource) {
    return new Contract(name, Objects.requireNonNull(schemaResource, "schemaResource"));
  }

  public String name() {
    return name;
  }

  public Optional<String> schema() {
    return Optional.ofNullable(schema);
  }

  /** The schema's fingerprint, read from the classpath; a problem named when it cannot be. */
  Optional<String> fingerprint(ClassLoader loader) {
    if (schema == null) return Optional.empty();
    try (InputStream in = loader.getResourceAsStream(schema)) {
      if (in == null) {
        throw new IllegalArgumentException(
            "contract '" + name + "': schema '" + schema + "' is not on the classpath");
      }
      try {
        return Optional.of(Canonical.fingerprint(in.readAllBytes()));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "contract '" + name + "': schema '" + schema + "' " + e.getMessage(), e);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "contract '" + name + "': schema '" + schema + "' could not be read: " + e.getMessage(), e);
    }
  }

  @Override
  public String toString() {
    return schema == null ? "Contract(" + name + ")" : "Contract(" + name + ", " + schema + ")";
  }
}
