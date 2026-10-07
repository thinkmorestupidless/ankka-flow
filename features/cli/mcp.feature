Feature: flow mcp serves the platform to a coding agent
  flow mcp serves the documentation and flow's abilities over the Model Context Protocol, with each
  tool saying whether it changes a cluster, and acts only on the cluster the project names.

  Background:
    Given a client connected to "flow mcp"

  Scenario: the server answers initialize with its tools and resources
    When the client initializes the session
    Then the server names the newest protocol version it speaks that the client offered
    And lists every tool with a description, an input schema and whether it changes a cluster
    And lists every documentation page as a resource

  Scenario: a blueprint is verified through a tool as flow verifies it
    Given a blueprint, descriptors and deploy-time configuration
    When the client calls the verify tool with them
    Then the answer is what "flow verify" prints for them, the notes and refusals included

  Scenario: a resource is generated through a tool and nothing is applied
    Given a blueprint, descriptors, images and deploy-time configuration
    When the client calls the generate tool with them
    Then the answer is the resource "flow generate" writes
    And no cluster is touched

  Scenario: every documentation page is a resource and a search finds it
    When the client searches the documentation for a page's title
    Then the page is among the answers
    And reading the page's resource gives the page as Markdown

  Scenario: a tool's failure is a tool result, not a protocol error
    When the client calls a tool with a blueprint that does not exist
    Then the answer is a tool result marked as an error, naming the file
    And the session continues

  Scenario Outline: a cluster tool acts only on the cluster the project names
    Given "flow mcp" started with no cluster named
    When the client calls the <tool> tool
    Then the tool refuses, saying how to name a cluster
    And no cluster is reached

    Examples:
      | tool           |
      | list pipelines |
      | get pipeline   |
      | pipeline logs  |
      | pipeline lag   |
      | apply pipeline |
      | reset pipeline |

  Scenario: a pipeline's status, conditions and events are read through a tool
    Given "flow mcp" started with a cluster named, running a pipeline
    When the client calls the get pipeline tool for it
    Then the answer holds the pipeline's phase, its conditions and its recent events

  Scenario: a streamlet's logs are read through a tool
    Given "flow mcp" started with a cluster named, running a pipeline
    When the client calls the pipeline logs tool for one of its streamlets
    Then the answer holds the recent lines of the streamlet's process container, or of its sidecar when asked

  Scenario: a pipeline's lag is read through a tool
    Given "flow mcp" started with a cluster named, running a pipeline
    When the client calls the pipeline lag tool for it
    Then the answer holds the consumer lag of each inlet of each streamlet

  Scenario: a pipeline is applied through a tool as kubectl would apply it
    Given "flow mcp" started with a cluster named, and a project whose blueprint verifies
    When the client calls the apply pipeline tool
    Then the pipeline's resource is created or updated on the named cluster, in the named namespace
    And a blueprint that does not verify is refused before anything is sent

  Scenario: a reset is requested through a tool as flow requests it
    Given "flow mcp" started with a cluster named, running a pipeline
    When the client calls the reset pipeline tool
    Then the reset is requested exactly as "flow reset" requests it
    And a streamlet still running draws the same refusal

  Scenario: a tool that changes the cluster says so
    When the client lists the tools
    Then the apply pipeline tool and the reset pipeline tool are marked as changing the cluster
    And every other tool is marked read-only

  Scenario: flow mcp install connects a client without disturbing its other servers
    Given a client's settings holding another server
    When "flow mcp install" is run for that client
    Then the settings hold an "ankka-flow" entry naming "flow mcp"
    And the other server's entry is as it was
    And a dry run changes nothing

  Scenario: the native binary serves the documentation
    Given the native binary
    When "flow mcp" is initialized and its resources are listed
    Then the documentation pages are among them
