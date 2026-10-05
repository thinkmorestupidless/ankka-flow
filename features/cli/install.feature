Feature: Installing flow
  flow is installed with one command through the tap, or from an archive on a release, and runs with
  no Java virtual machine.

  Scenario: flow is installed through the tap with one command
    Given a macOS or Linux machine with Homebrew and no Java virtual machine
    When the person installs "ankka-flow" from the tap
    Then "flow" is on the machine's path
    And "flow version" prints the release's version and the protocol version

  Scenario: a release offers an archive and a checksum for each platform
    Given a tagged release
    When the person looks at the release
    Then it offers one archive for each of the four platforms
    And a checksum is published beside each archive

  Scenario: an archive holds one native binary that runs with no Java virtual machine
    Given a downloaded archive and its checksum
    When the person verifies the archive against the checksum and unpacks it
    Then the archive holds exactly one executable, "flow"
    And "flow" runs with no Java virtual machine installed

  Scenario: ankka's CLI and flow are installed side by side from one tap
    Given a machine with ankka's CLI installed from the tap
    When the person installs "ankka-flow" from the tap
    Then both are installed
    And upgrading one does not touch the other

  Scenario: an upgrade through the tap installs the later release
    Given "flow" installed from the tap at one release
    When the person upgrades it through Homebrew after a later release
    Then "flow version" prints the later release's version

  Scenario: a Java virtual machine on the machine is not used
    Given a machine with a Java virtual machine installed
    When the person runs the native binary
    Then it runs the same as on a machine without one

  Scenario: a platform the release does not build for is refused by name
    Given a machine of a platform the release does not build for
    When the person installs "ankka-flow" from the tap
    Then Homebrew refuses, naming the platform

  Scenario: an archive whose checksum does not match is refused
    Given a downloaded archive whose checksum does not match the published one
    When Homebrew installs it
    Then the install is refused
