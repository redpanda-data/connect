#!/usr/bin/env python3
"""
Format component fields from jsonschema output into tagged sections.

Usage: rpk connect list --format jsonschema <category>s <component> | ./format-component-fields.py
Example: rpk connect list --format jsonschema inputs kafka_franz | ./format-component-fields.py
"""

import sys
import json
from typing import Dict, List, Any, Tuple


def format_type(type_str: str, is_array: bool = False) -> str:
    """Format type string with array notation if needed."""
    if is_array:
        return f"array[{type_str}]"
    return type_str


def extract_fields(properties: Dict[str, Any], parent_name: str = "") -> List[Dict[str, Any]]:
    """
    Extract fields recursively, flattening nested objects with dot notation.

    For arrays of primitives: note as "array[type]"
    For objects: inline child fields with parent.child notation
    For arrays of objects: inline with parent.child notation and note as array
    """
    fields = []

    for field_name, field_info in properties.items():
        full_name = f"{parent_name}.{field_name}" if parent_name else field_name
        field_type = field_info.get("type", "unknown")
        is_advanced = field_info.get("is_advanced", False)
        is_optional = field_info.get("is_optional", False)
        is_deprecated = field_info.get("is_deprecated", False)
        is_secret = field_info.get("is_secret", False)

        # Skip deprecated fields
        if is_deprecated:
            continue

        if field_type == "object":
            # Object: inline nested fields with dot notation
            nested_props = field_info.get("properties", {})
            if nested_props:
                # Recursively extract nested fields
                nested_fields = extract_fields(nested_props, full_name)
                fields.extend(nested_fields)
            else:
                # Empty object or no properties defined
                fields.append({
                    "name": full_name,
                    "type": "object",
                    "is_advanced": is_advanced,
                    "is_optional": is_optional,
                    "is_secret": is_secret,
                })

        elif field_type == "array":
            # Array: check items type
            items = field_info.get("items", {})
            items_type = items.get("type", "unknown")

            if items_type == "object":
                # Array of objects: inline nested fields with dot notation
                nested_props = items.get("properties", {})
                if nested_props:
                    nested_fields = extract_fields(nested_props, full_name)
                    # Mark all nested fields as array types
                    for nf in nested_fields:
                        nf["type"] = f"array[{nf['type']}]"
                    fields.extend(nested_fields)
                else:
                    fields.append({
                        "name": full_name,
                        "type": "array[object]",
                        "is_advanced": is_advanced,
                        "is_optional": is_optional,
                        "is_secret": is_secret,
                    })
            else:
                # Array of primitives
                fields.append({
                    "name": full_name,
                    "type": format_type(items_type, is_array=True),
                    "is_advanced": is_advanced,
                    "is_optional": is_optional,
                    "is_secret": is_secret,
                })

        else:
            # Primitive type
            fields.append({
                "name": full_name,
                "type": field_type,
                "is_advanced": is_advanced,
                "is_optional": is_optional,
                "is_secret": is_secret,
            })

    return fields


def group_fields(fields: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """Group fields into required, optional, advanced, and secrets."""
    required = []
    optional = []
    advanced = []
    secrets = []

    for field in fields:
        if field["is_secret"]:
            secrets.append(field)

        if field["is_advanced"]:
            advanced.append(field)
        elif field["is_optional"]:
            optional.append(field)
        else:
            required.append(field)

    return required, optional, advanced, secrets


def format_field(field: Dict[str, Any]) -> str:
    """Format a single field for output."""
    return f"  - {field['name']} ({field['type']})"


def main():
    # Component name passed as command line argument
    if len(sys.argv) < 2:
        print("Error: Component name required as argument", file=sys.stderr)
        sys.exit(1)

    target_component = sys.argv[1]

    # Read JSON from stdin
    schema = json.load(sys.stdin)

    # Find the target component in the schema
    component_def = None

    for category_name, category_def in schema.get("definitions", {}).items():
        for item in category_def.get("allOf", [{}])[0].get("anyOf", []):
            if target_component in item.get("properties", {}):
                component_def = item["properties"][target_component]
                break
        if component_def:
            break

    if not component_def:
        print(f"Error: Component '{target_component}' not found in schema", file=sys.stderr)
        sys.exit(1)

    # Extract and group fields
    properties = component_def.get("properties", {})
    fields = extract_fields(properties)
    required, optional, advanced, secrets = group_fields(fields)

    # Output tagged sections
    if required:
        print("<required_fields>")
        for field in sorted(required, key=lambda f: f["name"]):
            print(format_field(field))
        print("</required_fields>")

    if optional:
        print("<optional_fields>")
        for field in sorted(optional, key=lambda f: f["name"]):
            print(format_field(field))
        print("</optional_fields>")

    if advanced:
        print("<advanced_fields>")
        for field in sorted(advanced, key=lambda f: f["name"]):
            print(format_field(field))
        print("</advanced_fields>")

    if secrets:
        print("<secret_fields>")
        for field in sorted(secrets, key=lambda f: f["name"]):
            print(format_field(field))
        print("</secret_fields>")


if __name__ == "__main__":
    main()
