package cart

// docs:start main
import com.thinkmorestupidless.ankka.flow.sdk.Serve

object Main:
  def main(args: Array[String]): Unit = Serve.run(new CartRouter)
// docs:end main
