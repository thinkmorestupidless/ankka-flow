Feature: Releasing the native binary
  A tagged release builds, checks, attaches and publishes the native binaries with no step done by
  hand, and updates the tap only when every platform's archive is attached.

  Scenario: a tag attaches four archives and updates the formula with no manual step
    Given a tag is pushed
    When the release runs
    Then four archives and four checksums are attached to the tag's release
    And the formula "ankka-flow" in the tap holds the release's version and the four checksums

  Scenario: a platform that fails leaves the formula untouched and names itself
    Given a tag is pushed
    When one platform's native binary fails its build or its smoke script
    Then the other platforms' archives are still attached
    And the formula in the tap is untouched
    And the failure names the platform

  Scenario: a platform's leg run again replaces its archive
    Given a release with an archive attached for a platform
    When that platform's leg is run again
    Then the archive is replaced, not duplicated
    And the checksum beside it matches the replacement

  Scenario: ankka's release and ankka-flow's release each update only their own formula
    Given a release of ankka and a release of ankka-flow on the same day
    When both update the tap
    Then neither touches the other's formula

  Scenario: a release refuses to build from a dirty tree
    Given a checkout with a dirty tree
    When a platform's leg builds the native binary
    Then the leg refuses, before anything is attached

  Scenario: the images, the Python SDK and the plugin are published whatever the native binaries do
    Given a tag is pushed
    When a platform's leg fails
    Then the images, the Python SDK and the plugin are published as before
    And only the formula waits on the native binaries
