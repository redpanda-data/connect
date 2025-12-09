#!/usr/bin/env python3
"""
Generate Promptfoo test cases from fixture JSON files.

Usage:
    ./generate-tests.py --type search --output search-eval-generated.yaml
    ./generate-tests.py --type blobl --filter difficulty=basic
    ./generate-tests.py --type pipeline --count 10
"""

import json
import argparse
from pathlib import Path
from typing import List, Dict, Any


def load_fixtures(fixture_type: str) -> List[Dict[str, Any]]:
    """Load fixtures from JSON file."""
    fixture_map = {
        "search": "search_queries.json",
        "blobl": "blobl_transformations.json",
        "pipeline": "pipeline_descriptions.json",
    }

    if fixture_type not in fixture_map:
        raise ValueError(f"Unknown fixture type: {fixture_type}")

    fixture_file = Path(__file__).parent / "fixtures" / fixture_map[fixture_type]
    with open(fixture_file) as f:
        return json.load(f)


def filter_fixtures(
    fixtures: List[Dict[str, Any]], filters: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Filter fixtures based on criteria."""
    filtered = fixtures
    for key, value in filters.items():
        filtered = [f for f in filtered if f.get(key) == value]
    return filtered


def generate_search_test(fixture: Dict[str, Any]) -> str:
    """Generate YAML test case for component search."""
    yaml_lines = [
        f"  # {fixture.get('description', fixture['id'])}",
        f"  - vars:",
        f"      query: \"{fixture['query']}\"",
        f"    assert:",
        f"      - type: javascript",
        f"        value: |",
        f"          const response = output.toLowerCase();",
    ]

    # Add component checks
    if fixture.get("expected_components"):
        components = fixture["expected_components"]
        checks = " || ".join([f"response.includes('{c}')" for c in components])
        yaml_lines.extend([
            f"          if ({checks}) {{",
            f"            return {{ pass: true, score: 1.0 }};",
            f"          }}",
            f"          return {{",
            f"            pass: false,",
            f"            score: 0,",
            f"            reason: 'Missing expected components: {', '.join(components)}'",
            f"          }};",
        ])

    yaml_lines.extend([
        f"",
        f"      - type: llm-rubric",
        f"        value: |",
        f"          Score 0-1 based on:",
        f"          - Mentions relevant components (0.4)",
        f"          - Provides configuration details (0.3)",
        f"          - Explains why it matches the query (0.3)",
        f"",
    ])

    return "\n".join(yaml_lines)


def generate_blobl_test(fixture: Dict[str, Any]) -> str:
    """Generate YAML test case for Bloblang script."""
    sample_input = json.dumps(fixture.get("sample_input", {}))

    yaml_lines = [
        f"  # {fixture.get('description', fixture['id'])}",
        f"  - vars:",
        f"      description: \"{fixture['description']}\"",
        f"      sample_input: '{sample_input}'",
        f"    assert:",
        f"      - type: javascript",
        f"        value: |",
        f"          const scriptMatch = output.match(/```(?:blobl|bloblang)\\n([\\s\\S]*?)\\n```/);",
        f"          if (!scriptMatch) {{",
        f"            return {{ pass: false, score: 0, reason: 'No Bloblang script found' }};",
        f"          }}",
        f"          const script = scriptMatch[1];",
        f"          // Add specific validations here",
        f"          return {{ pass: true, score: 1.0 }};",
        f"",
        f"      - type: llm-rubric",
        f"        value: |",
        f"          Score 0-1 based on:",
    ]

    # Add criteria from fixture
    if fixture.get("validation_criteria"):
        for i, criterion in enumerate(fixture["validation_criteria"][:3]):
            weight = 0.4 if i == 0 else 0.3
            yaml_lines.append(f"          - {criterion} ({weight})")

    yaml_lines.append("")
    return "\n".join(yaml_lines)


def generate_pipeline_test(fixture: Dict[str, Any]) -> str:
    """Generate YAML test case for pipeline config."""
    yaml_lines = [
        f"  # {fixture.get('description', fixture['id'])}",
        f"  - vars:",
        f"      description: \"{fixture['description']}\"",
    ]

    if fixture.get("context"):
        yaml_lines.append(f"      context: \"{fixture['context']}\"")

    yaml_lines.extend([
        f"    assert:",
        f"      - type: javascript",
        f"        value: |",
        f"          const yamlMatch = output.match(/```(?:yaml|yml)\\n([\\s\\S]*?)\\n```/);",
        f"          if (!yamlMatch) {{",
        f"            return {{ pass: false, score: 0, reason: 'No YAML config found' }};",
        f"          }}",
        f"          const config = yamlMatch[1];",
        f"",
        f"          // Check for environment variables",
        f"          const usesEnvVars = config.includes('${{');",
        f"          if (!usesEnvVars) {{",
        f"            return {{ pass: false, score: 0.5, reason: 'Not using environment variables' }};",
        f"          }}",
        f"",
        f"          return {{ pass: true, score: 1.0 }};",
        f"",
        f"      - type: llm-rubric",
        f"        value: |",
        f"          Score 0-1 based on:",
    ])

    # Add criteria from fixture
    if fixture.get("validation_criteria"):
        for i, criterion in enumerate(fixture["validation_criteria"][:4]):
            weight = [0.3, 0.3, 0.2, 0.2][i]
            yaml_lines.append(f"          - {criterion} ({weight})")

    yaml_lines.append("")
    return "\n".join(yaml_lines)


def generate_yaml_header(fixture_type: str) -> str:
    """Generate YAML file header."""
    titles = {
        "search": "Component Search Quality",
        "blobl": "Bloblang Script Generation Quality",
        "pipeline": "Pipeline Configuration Quality",
    }

    return f"""# Promptfoo Evaluation Config: {titles[fixture_type]}
#
# Generated from fixtures using generate-tests.py
# Edit fixtures/*.json to modify test cases

# Provider configuration
providers:
  - id: anthropic:messages:claude-sonnet-4-5-20250929
    config:
      temperature: 0.7
      max_tokens: 4096

# Test cases
tests:
"""


def main():
    parser = argparse.ArgumentParser(description="Generate Promptfoo test cases from fixtures")
    parser.add_argument(
        "--type",
        choices=["search", "blobl", "pipeline"],
        required=True,
        help="Type of tests to generate",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--filter",
        action="append",
        help="Filter criteria (e.g., difficulty=basic)",
    )
    parser.add_argument(
        "--count",
        type=int,
        help="Limit number of tests",
    )
    parser.add_argument(
        "--ids",
        type=str,
        help="Comma-separated list of test IDs to include",
    )

    args = parser.parse_args()

    # Load fixtures
    fixtures = load_fixtures(args.type)

    # Filter by IDs if specified
    if args.ids:
        ids = set(args.ids.split(","))
        fixtures = [f for f in fixtures if f["id"] in ids]

    # Apply filters
    if args.filter:
        filters = {}
        for f in args.filter:
            key, value = f.split("=")
            filters[key] = value
        fixtures = filter_fixtures(fixtures, filters)

    # Limit count
    if args.count:
        fixtures = fixtures[: args.count]

    # Generate YAML
    output_lines = [generate_yaml_header(args.type)]

    generator_map = {
        "search": generate_search_test,
        "blobl": generate_blobl_test,
        "pipeline": generate_pipeline_test,
    }
    generator = generator_map[args.type]

    for fixture in fixtures:
        output_lines.append(generator(fixture))

    # Add output path
    output_lines.append(f"\n# Output configuration")
    output_lines.append(f"outputPath: ../promptfoo-{args.type}-results.json\n")

    output = "\n".join(output_lines)

    # Write output
    if args.output:
        output_file = Path(args.output)
        output_file.write_text(output)
        print(f"Generated {len(fixtures)} test cases to {output_file}")
    else:
        print(output)


if __name__ == "__main__":
    main()
