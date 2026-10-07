Feature: flow init starts a streamlet project
  One command writes a project that builds, tests, writes and checks its descriptor, verifies its
  blueprint and runs beside the sidecar, in Scala or in Python, from templates carried inside flow.

  Scenario Outline: a project from flow init passes its own tests
    Given a project written by "flow init" in "<language>"
    When its tests are run
    Then every test passes, with no file edited

    Examples:
      | language |
      | scala    |
      | python   |

  Scenario: a project's committed descriptor is what its streamlet declares
    Given a project written by "flow init"
    When its descriptor check is run
    Then the committed descriptor equals what the SDK writes for the streamlet

  Scenario: a project's blueprint verifies against its descriptor
    Given a project written by "flow init"
    When "flow verify" is run on its blueprint with its descriptor
    Then the blueprint is verified

  Scenario: a project depends on the SDK of the flow that wrote it
    Given a flow of a released version
    When it writes a project
    Then the project depends on the SDK of that same version

  Scenario: the streamlet does something visible to each record
    Given a project written by "flow init"
    When a record goes through its streamlet
    Then the record on the output topic differs from the input in a way a person can see

  Scenario Outline: flow init refuses what it cannot write a working project for
    Given <problem>
    When "flow init" is run
    Then it refuses, naming what to change
    And nothing is written

    Examples:
      | problem                                                  |
      | a name with a capital letter                             |
      | a name longer than sixty-three characters                |
      | a directory to write into that already holds files       |
      | a package for a language that takes none                 |
      | a package that is not a package name                     |

  Scenario: a project runs on a laptop beside the sidecar
    Given a project written by "flow init", Kafka and the sidecar in containers
    When the streamlet is run on the host and records are produced to the input topic
    Then each record reaches the output topic through the streamlet

  Scenario: a project builds its image
    Given a project written by "flow init"
    When its image is built
    Then the image holds the streamlet and the SDK and exposes no port

  Scenario: a project's CI checks its tests and its descriptor
    Given a project written by "flow init"
    When its CI runs on a change
    Then its tests and its descriptor check run

  Scenario: the native binary writes the same project as the JVM build
    Given the native binary and the JVM build of one flow
    When each writes a project with the same name and language
    Then the two projects are identical byte for byte

  Scenario: a project carries the agent skills of its flow's version
    Given a project written by "flow init"
    When a coding agent is opened in it
    Then it finds the skills of that flow's version
