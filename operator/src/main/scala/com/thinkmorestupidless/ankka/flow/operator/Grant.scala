package com.thinkmorestupidless.ankka.flow.operator

/** What the broker lets a machine do with one topic, as an observe pass finds it (R16). */
enum Grant:
  case Granted

  /** The machine holds no grant for this right: the first of the needed rights it lacks. */
  case Lacks(right: Grant.Right)

  /** The broker, or ankka's token endpoint, did not accept the machine's credentials. */
  case CredentialRefused(reason: String)

  /** The broker could not be asked; nothing is concluded. */
  case Unknown(reason: String)

object Grant:
  /** A right a port needs: `Read` for an inlet, `Write` for an outlet. */
  enum Right:
    case Read, Write

    /** The word for it in ankka's grant command. */
    def grantWord: String = this match
      case Read  => "consume"
      case Write => "produce"
