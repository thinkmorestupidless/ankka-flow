package com.thinkmorestupidless.ankka.flow.flink;

import java.io.Serializable;

/** A port a streamlet writes. */
public final class Outlet<A> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String name;
  private final Contract contract;
  private final Codec<A> codec;

  Outlet(String name, Contract contract, Codec<A> codec) {
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
    return "Outlet(" + name + ", " + contract + ")";
  }
}
