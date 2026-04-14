Feature: Loki Engine with a local LM Studio model
  As an application developer
  I want complete workflows to run against a local language model
  So that agents, context, tools, routing, and batches work together

  Background:
    Given LM Studio is serving "prism-ml/bonsai-27b"

  Scenario: A single agent answers a user question
    Given an agent that must answer with "PARIS"
    When I run the agent with "What is the capital of France?"
    Then the final output contains "PARIS"
    And agent start and end events are traced

  Scenario: Attached context grounds an agent response
    Given an agent that may answer only from attached context
    And context containing "The private verification code is ZEPHYR-741." is attached
    When I run the agent with "What is the private verification code?"
    Then the final output contains "ZEPHYR-741"
    And a context injection event is traced

  Scenario: An agent invokes a registered tool
    Given an agent required to use an echo tool
    When I ask the agent to echo "MAGIC-TOOL-582"
    Then at least one tool call is traced
    And the final output contains "MAGIC-TOOL-582"

  Scenario: An orchestrator delegates to a specialist
    Given an orchestrator that always routes to a specialist
    When I run the orchestrator with "Summarize why local models are useful."
    Then outputs are recorded for both agents
    And a routing decision is traced

  Scenario: A batch processes multiple documents
    Given a batch summarizer agent
    When I process 2 documents concurrently
    Then every batch item completes successfully
    And the reduced batch output is not empty
