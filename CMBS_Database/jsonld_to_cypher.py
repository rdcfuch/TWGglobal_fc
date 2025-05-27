import json

def escape_cypher_string(s):
    """Escape single quotes in strings for Cypher compatibility."""
    return s.replace("'", "''")

def cypher_value(v):
    """Format a value for Cypher based on its type."""
    if isinstance(v, str):
        return f"'{escape_cypher_string(v)}'"
    elif isinstance(v, (int, float)):
        return str(v)
    else:
        return f"'{escape_cypher_string(str(v))}'"

def is_reference(value):
    """Check if a value is a reference (dict with '@id')."""
    return isinstance(value, dict) and '@id' in value

def is_reference_list(value):
    """Check if a value is a list of references."""
    return isinstance(value, list) and all(is_reference(item) for item in value)

def jsonld_to_cypher(data):
    """Convert JSON-LD data to a list of Cypher commands."""
    graph = data.get('@graph', [])
    node_cmds = []
    rel_cmds = []

    # Create nodes
    for item in graph:
        label = item['@type']
        id = item['@id']
        # Collect simple properties (non-references)
        properties = {
            k: v for k, v in item.items()
            if k not in ['@id', '@type'] and not is_reference(v) and not is_reference_list(v)
        }
        # Generate node creation command
        node_cmd = f"MERGE (n:{label} {{id: '{escape_cypher_string(id)}'}})"
        if properties:
            set_props = ', '.join([f"n.{k} = {cypher_value(v)}" for k, v in properties.items()])
            node_cmd += f" SET {set_props}"
        node_cmds.append(node_cmd)

        # Collect relationships
        for key, value in item.items():
            rel_type = key.upper().replace(' ', '_')  # Convert property name to relationship type
            if is_reference(value):
                target_id = value['@id']
                rel_cmd = (
                    f"MATCH (a {{id: '{escape_cypher_string(id)}'}}), "
                    f"(b {{id: '{escape_cypher_string(target_id)}'}}) "
                    f"CREATE (a)-[:{rel_type}]->(b)"
                )
                rel_cmds.append(rel_cmd)
            elif is_reference_list(value):
                for ref in value:
                    target_id = ref['@id']
                    rel_cmd = (
                        f"MATCH (a {{id: '{escape_cypher_string(id)}'}}), "
                        f"(b {{id: '{escape_cypher_string(target_id)}'}}) "
                        f"CREATE (a)-[:{rel_type}]->(b)"
                    )
                    rel_cmds.append(rel_cmd)

    return node_cmds + rel_cmds

def convert_jsonld_file_to_cypher(input_path, output_path):
    """Read a JSON-LD file, convert to Cypher, and write to output file."""
    import os
    if os.path.exists(output_path):
        os.remove(output_path)
    with open(input_path, 'r') as f:
        data = json.load(f)
    cypher_commands = jsonld_to_cypher(data)
    with open(output_path, 'w') as f:
        for cmd in cypher_commands:
            f.write(cmd + ';\n')
# Optional: keep CLI usage for standalone script
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Convert JSON-LD to Cypher commands.')
    parser.add_argument('input', help='Input JSON-LD file')
    parser.add_argument('output', help='Output Cypher file')
    args = parser.parse_args()
    convert_jsonld_file_to_cypher(args.input, args.output)
