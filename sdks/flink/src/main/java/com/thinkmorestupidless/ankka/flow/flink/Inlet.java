package com.thinkmorestupidless.ankka.flow.flink;

import java.io.Serializable;

/** A port a streamlet reads. */
public final class Inlet<A> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String name;
  private final Contract contract;
  private final Codec<A> codec;

  Inlet(String name, Contract contract, Codec<A> codec) {
    this.name = name;
    this.contract = contract;
    this.codec = codec;
  }

  public String name() {
    return name;
  }

  public Contract contract() {
    return contract;
  }

  public Codec<A> codec() {
    return codec;
  }

  @Override
  public String toString() {
    return "Inlet(" + name + ", " + contract + ")";
  }
}
