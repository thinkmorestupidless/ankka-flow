package cart.counter;

/** How many of a cart's events fell in one window, `[windowStart, windowEnd)` in milliseconds. */
public record CartCount(String cartId, long windowStart, long windowEnd, long count) {}
