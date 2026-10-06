Feature: The Scala SDK is published with every release
  A release publishes the Scala SDK to Maven Central at the release's version and the Scala cart
  router's image to the registry, with no manual step; a pre-release publishes neither.

  Scenario: a release publishes the Scala SDK and the Scala sample's image with no manual step
    Given a release tag
    When the release runs
    Then the Scala SDK is on Maven Central at the release's version
    And it reports that version and the release's protocol version in discovery and in the descriptor
    And the Scala cart router's image is in the registry at the release's version

  Scenario: a project depends on the published SDK by version
    Given a fresh Scala project naming the Scala SDK at a published version
    When the project compiles the cart router and runs its tests
    Then the cart router compiles and its tests pass

  Scenario: a pre-release publishes neither the SDK nor the image
    Given a pre-release tag
    When the release runs
    Then the Scala SDK is not published
    And the Scala cart router's image is not published

  Scenario: a release run again does not publish the SDK twice
    Given a release whose Scala SDK is already on Maven Central
    When the release runs again
    Then the Scala SDK is not uploaded again
    And the rest of the release runs
