/*
 * Copyright (C) 2016-2026 Lightbend Inc. <https://www.lightbend.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.thinkmorestupidless.ankka.flow.protocol

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.Base64

/**
 * JSON contracts, carried from Cloudflow's `cloudflow.streamlets.json.JsonSchema`
 * (core/cloudflow-json/src/main/scala/cloudflow/streamlets/json/Json.scala).
 *
 * A JSON contract is verified by schema name: the fingerprint is derived from the name alone, so
 * equal names connect and different names do not. Versioning the name ("cart-events.v2") is how a
 * shape changes. The algorithm is part of the protocol; every SDK computes the same bytes.
 */
object Fingerprint:

  val Format = "json"

  def fingerprint(schemaName: String): String =
    Base64.getEncoder.encodeToString(
      MessageDigest.getInstance("SHA-256").digest(schemaName.getBytes(UTF_8))
    )
