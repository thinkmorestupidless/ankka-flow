package {{package}}

import com.thinkmorestupidless.ankka.flow.sdk.Serve

/** Serves the streamlet on 127.0.0.1:$FLOW_PROCESS_PORT (9010), where the sidecar finds it. */
object Main:
  def main(args: Array[String]): Unit = Serve.run(new {{class}})
