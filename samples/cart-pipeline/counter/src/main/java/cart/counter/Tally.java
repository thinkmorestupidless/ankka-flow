package cart.counter;

/**
 * The state of one cart's open window: a POJO, so Flink can carry it through a changed class
 * (fields added or removed, never retyped or renamed).
 */
public class Tally {
  public long count;

  public Tally() {}
}
