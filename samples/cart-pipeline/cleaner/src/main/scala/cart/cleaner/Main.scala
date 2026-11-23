package cart.cleaner

// docs:start main
import com.thinkmorestupidless.ankka.flow.pekko.PekkoStreamlets

object Main:
  def main(args: Array[String]): Unit = PekkoStreamlets.run(Cleaner, args)
// docs:end main
