#!/usr/bin/env python3
"""
Format bloblang functions or methods metadata from jsonschema output into category files.

Usage: rpk connect list --format jsonschema <category> | ./format-bloblang.py --output-dir <dir>
Examples:
    rpk connect list --format jsonschema bloblang-functions | ./format-bloblang.py --output-dir /tmp/blobl-session-abc123
    rpk connect list --format jsonschema bloblang-methods | ./format-bloblang.py --output-dir /tmp/blobl-session-abc123

Output:
    Creates separate files per category:
    - functions-General.txt
    - functions-Environment.txt
    - methods-String_Manipulation.txt
    - etc.
"""

import sys
import json
import argparse
from typing import Dict, Any, List
from pathlib import Path
from collections import defaultdict


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Format bloblang metadata into category files'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Directory to write category files to'
    )
    return parser.parse_args()


def get_category_names(category_type: str) -> tuple:
    """Get the tag type and file prefix based on category type.

    Returns:
        tuple: (tag_type, file_prefix) where tag_type is singular (function/method)
               and file_prefix is plural (functions/methods)
    """
    if category_type == "bloblang-functions":
        return ("function", "functions")
    else:
        return ("method", "methods")


def group_by_category(items: List[Dict[str, Any]], category_type: str) -> Dict[str, List[Dict]]:
    """Group items by category (functions) or tags (methods)."""
    grouped = defaultdict(list)

    for item in items:
        if category_type == "bloblang-functions":
            category = item.get('category', 'Uncategorized')
        else:  # methods
            categories = item.get('categories', [])
            if categories:
                # Methods can have multiple categories - use first one
                category = categories[0].get('Category', 'Uncategorized')
            else:
                category = 'Uncategorized'

        grouped[category].append(item)

    return dict(grouped)


def format_item(item: Dict[str, Any], category_type: str) -> str:
    """Format a single function or method as a tagged section (no category field)."""
    name = item['name']

    # Build params string
    params = item.get("params", {}).get("named", [])
    if params:
        param_strs = [f"{p['name']}:{p['type']}" for p in params]
        params_attr = ', '.join(param_strs)
    else:
        params_attr = ""

    # Determine tag type (function or method)
    tag_type, _ = get_category_names(category_type)

    # Opening tag with name and params attributes
    lines = [f'<{tag_type} name="{name}" params="{params_attr}">']

    # Description (no prefix, only if meaningful)
    desc = item.get('description', '')
    if desc:
        # Split description into sentences (each sentence on its own line)
        # Split on '. ' to preserve sentence boundaries
        sentences = desc.split('. ')
        for i, sentence in enumerate(sentences):
            if sentence:  # Skip empty strings
                # Add period back if not the last sentence
                if i < len(sentences) - 1 and not sentence.endswith('.'):
                    lines.append(sentence + '.')
                else:
                    lines.append(sentence)
    else:
        # Add error for missing description
        lines.append(f"ERROR missing description for {name}")

    # Examples (print all if present)
    examples = item.get("examples", [])
    for idx, example in enumerate(examples):
        if isinstance(example, dict):
            summary = example.get("summary", "")
            mapping = example.get("mapping", "")
        else:
            summary = ""
            mapping = example

        if mapping:  # Only add if not empty
            # Always use code block format (mapping on new line)
            if summary:
                lines.append(f'<example summary="{summary}">')
            else:
                lines.append('<example>')
            lines.append(mapping)
            lines.append('</example>')

    # Closing tag
    lines.append(f'</{tag_type}>')
    return "\n".join(lines)


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read JSON from stdin
    schema = json.load(sys.stdin)

    # Find category type and items
    category_type = None
    items = None
    for key in ["bloblang-functions", "bloblang-methods"]:
        if key in schema:
            category_type = key
            items = schema[key]
            break

    if not items:
        print("Error: No bloblang items found in schema", file=sys.stderr)
        sys.exit(1)

    # Group by category
    grouped = group_by_category(items, category_type)

    # Determine file prefix based on type
    _, file_prefix = get_category_names(category_type)

    # Write each category to separate file
    for category_name in sorted(grouped.keys()):
        # Skip empty and deprecated categories
        if not category_name or category_name == 'Deprecated':
            continue

        # Sanitize category name for filename (replace spaces with underscores)
        safe_category = category_name.replace(' ', '_').replace('/', '_').replace('&', '_')
        filename = f"{file_prefix}-{safe_category}.txt"
        filepath = output_dir / filename

        with open(filepath, 'w') as f:
            # Write opening tag
            f.write(f"<{category_type}>\n")

            # Sort items within category by name
            category_items = sorted(grouped[category_name], key=lambda x: x['name'])

            # Format each item (no category field needed)
            formatted_items = []
            for item in category_items:
                formatted_items.append(format_item(item, category_type))

            f.write("\n\n".join(formatted_items))
            f.write("\n")

            # Write closing tag
            f.write(f"</{category_type}>\n")

        # Log to stderr for visibility
        print(f"Written: {filename}", file=sys.stderr)


if __name__ == "__main__":
    main()
