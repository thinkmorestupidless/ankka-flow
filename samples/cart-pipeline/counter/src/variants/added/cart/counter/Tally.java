package cart.counter;

/**
 * The cluster suite's second image of the counter: the window's state gains a field, which Flink
 * carries a POJO through, so the open window is kept.
 */
public class Tally {
  public long count;
  public long revision;

  public Tally() {}
}
