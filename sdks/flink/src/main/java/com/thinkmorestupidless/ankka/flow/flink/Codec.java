package com.thinkmorestupidless.ankka.flow.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Bytes to a value and back. A record whose bytes do not decode fails the job: never skipped. An
 * author who would rather drop such records reads {@link #bytes()} and decodes in the job.
 */
public interface Codec<A> extends Serializable {
  byte[] encode(A value);

  A decode(byte[] bytes);

  /** The type the codec reads, for Flink's type information. */
  Class<A> type();

  /** JSON through Jackson, as the class's fields or record components name it. */
  static <A> Codec<A> json(Class<A> type) {
    return new JsonCodec<>(type);
  }

  /** The record's bytes as they are. */
  static Codec<byte[]> bytes() {
    return new Codec<>() {
      private static final long serialVersionUID = 1L;

      @Override
      public byte[] encode(byte[] value) {
        return value;
      }

      @Override
      public byte[] decode(byte[] bytes) {
        return bytes;
      }

      @Override
      public Class<byte[]> type() {
        return byte[].class;
      }
    };
  }

  /** UTF-8 text. */
  static Codec<String> string() {
    return new Codec<>() {
      private static final long serialVersionUID = 1L;

      @Override
      public byte[] encode(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
      }

      @Override
      public Class<String> type() {
        return String.class;
      }
    };
  }

  /** Jackson's mapper is not serializable, so each copy of the codec makes its own. */
  final class JsonCodec<A> implements Codec<A> {
    private static final long serialVersionUID = 1L;
    private final Class<A> type;
    private transient ObjectMapper mapper;

    JsonCodec(Class<A> type) {
      this.type = type;
    }

    private ObjectMapper mapper() {
      if (mapper == null) mapper = new ObjectMapper().findAndRegisterModules();
      return mapper;
    }

    @Override
    public byte[] encode(A value) {
      try {
        return mapper().writeValueAsBytes(value);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public A decode(byte[] bytes) {
      try {
        return mapper().readValue(bytes, type);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public Class<A> type() {
      return type;
    }
  }
}
