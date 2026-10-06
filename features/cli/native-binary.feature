Feature: The native binary is the CLI
  A native binary does exactly what the JVM build does, and is held to it by the suite and the
  smoke script rather than by a build that succeeded.

  Scenario: the suite passes against the native binary with every case kept
    Given the suite passes against the JVM build
    When the suite is run against the native binary
    Then every case passes
    And no case is skipped

  Scenario: verify and generate give byte-identical output from the native binary and the JVM build
    Given a blueprint, descriptors and deploy-time configuration
    When "flow verify" and "flow generate" are run with the native binary and with the JVM build
    Then the output of each is identical byte for byte, the resource and every note included

  Scenario: a reset from the native binary is the JVM build's reset
    Given a cluster running a pipeline
    When "flow reset" is run with the native binary
    Then the reset is requested exactly as the JVM build requests it
    And a streamlet still running draws the same refusal

  Scenario: a native binary missing something the CLI needs fails the smoke script by name
    Given a native binary from which something the CLI needs at run time is missing
    When the smoke script runs it
    Then the smoke script fails, naming what is missing
    And the release does not ship that native binary

  Scenario: the usage text is the JVM build's
    Given the native binary
    When it is run with no arguments, a wrong flag, or "--help"
    Then the usage text is the JVM build's

  Scenario: the native binary's version is the tag's
    Given a native binary built at a tag
    When the smoke script asks "flow version"
    Then the version printed is the tag's version
    And a version that differs fails the smoke script
