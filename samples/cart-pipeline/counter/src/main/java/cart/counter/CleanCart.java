package cart.counter;

/** A cleaned cart event, as the cleaner writes it: `at` is the event's own time, in milliseconds. */
public record CleanCart(String cartId, long seq, long total, long at) {}
