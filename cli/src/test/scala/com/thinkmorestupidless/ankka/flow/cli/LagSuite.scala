package com.thinkmorestupidless.ankka.flow.cli

import com.thinkmorestupidless.ankka.flow.cli.mcp.{Lag, LagSample}

class LagSuite extends munit.FunSuite:

  test("records_lag samples are parsed from a sidecar's metrics, other lines ignored") {
    val metrics =
      """# HELP kafka_consumer_consumer_fetch_manager_metrics_records_lag The latest lag of the partition
        |# TYPE kafka_consumer_consumer_fetch_manager_metrics_records_lag gauge
        |kafka_consumer_consumer_fetch_manager_metrics_records_lag{client_id="cart.router.in",partition="0",topic="shop_cart-events_v1"} 0.0
        |kafka_consumer_consumer_fetch_manager_metrics_records_lag{client_id="cart.router.in",partition="2",topic="shop_cart-events_v1"} 17.0
        |kafka_consumer_consumer_fetch_manager_metrics_records_lag_max{client_id="cart.router.in"} 17.0
        |kafka_producer_producer_metrics_record_send_rate{client_id="cart.router.valid"} 1.5
        |""".stripMargin
    assertEquals(
      Lag.parse(metrics),
      Vector(
        LagSample("cart.router.in", "shop_cart-events_v1", "0", 0.0),
        LagSample("cart.router.in", "shop_cart-events_v1", "2", 17.0)
      )
    )
  }
