package cart.counter;

/**
 * The cluster suite's third image of the counter: the state's field is retyped, which Flink cannot
 * carry, so the job cannot read its state.
 */
public class Tally {
  public int count;

  public Tally() {}
}
