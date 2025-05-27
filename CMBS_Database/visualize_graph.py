import json
import networkx as nx
import argparse

parser = argparse.ArgumentParser(description='Visualize a CMBS JSON-LD graph.')
parser.add_argument('filename', type=str, help='Path to the JSON-LD file')
args = parser.parse_args()

with open(args.filename, 'r') as f:
    data = json.load(f)

G = nx.DiGraph()
default_color = 'gray'

for item in data['@graph']:
    node_id = item['@id']
    node_type = item.get('@type', 'Unknown')
    color = default_color
    if node_type == 'Security':
        color = 'lightblue'
    elif node_type == 'Organization':
        color = 'lightgreen'
    elif node_type == 'Property':
        color = 'lightpink'
    elif node_type == 'Deal':
        color = 'lightyellow'
    if not G.has_node(node_id):
        G.add_node(node_id, color=color, type=node_type)
    else:
        G.nodes[node_id]['color'] = color
        G.nodes[node_id]['type'] = node_type

for item in data['@graph']:
    node_id = item['@id']
    
    # Existing relationship handlers
    if 'issuedBy' in item:
        target_node_id = item['issuedBy']['@id']
        if not G.has_node(target_node_id):
            G.add_node(target_node_id, color=default_color, type='Unknown')
        G.add_edge(node_id, target_node_id, type='issuedBy')
    if 'issues' in item:
        target_node_id = item['issues']['@id']
        if not G.has_node(target_node_id):
            G.add_node(target_node_id, color=default_color, type='Unknown')
        G.add_edge(node_id, target_node_id, type='issues')
    if 'partOfDeal' in item:
        target_node_id = item['partOfDeal']['@id']
        if not G.has_node(target_node_id):
            G.add_node(target_node_id, color='lightyellow', type='Deal')
        G.add_edge(node_id, target_node_id, type='partOfDeal')

    # Add new relationship handlers for properties
    if 'collateral' in item:  # Connects Security to Property
        target_node_id = item['collateral']['@id']
        if not G.has_node(target_node_id):
            G.add_node(target_node_id, color='lightpink', type='Property')
        G.add_edge(node_id, target_node_id, type='collateral')

    if 'ownedBy' in item:  # Connects Property to Organization
        target_node_id = item['ownedBy']['@id']
        if not G.has_node(target_node_id):
            G.add_node(target_node_id, color='lightgreen', type='Organization')
        G.add_edge(node_id, target_node_id, type='ownedBy')

    # Add property attribute relationships
    prop_relations = ['trusteePropType', 'address', 'yearBuilt', 'propertyType', 'locatedAt', 'builtAt']
    for rel in prop_relations:
        if rel in item:
            target_value = item[rel]
            target_node_id = target_value['@id'] if isinstance(target_value, dict) else target_value
            if not G.has_node(target_node_id):
                G.add_node(target_node_id, color='lightcoral', type=f"PropertyAttribute/{rel}")
            G.add_edge(node_id, target_node_id, type=rel)

nodes = []
for n, attr in G.nodes(data=True):
    nodes.append({
        'id': n,
        'group': attr.get('type', 'Unknown'),
        'color': attr.get('color', default_color),
        'label': n  # or use a more descriptive property if available
    })

links = []
for u, v, attr in G.edges(data=True):
    links.append({
        'source': u,
        'target': v,
        'type': attr.get('type', '')
    })

html_template = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <script src="https://d3js.org/d3.v7.min.js"></script>
  <style>
    html, body {{ height: 100%; margin: 0; padding: 0; }}
    body {{ width: 100vw; height: 100vh; overflow: hidden; }}
    svg {{ width: 100vw; height: 100vh; display: block; }}
    .links line {{ stroke: #999; stroke-opacity: 0.6; }}
    .nodes circle {{ stroke: #fff; stroke-width: 1.5px; }}
    text {{ font-family: sans-serif; font-size: 12px; }}
  </style>
</head>
<body>
<svg></svg>
<script>
const nodes = {nodes};
const links = {links};

const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3.select("svg")
    .attr("viewBox", `0 0 ${{width}} ${{height}}`)
    .attr("preserveAspectRatio", "xMidYMid meet");

const g = svg.append("g"); // Group to hold graph elements for zooming

const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id).distance(100))
    .force("charge", d3.forceManyBody().strength(-300))
    .force("center", d3.forceCenter(width / 2, height / 2));

const link = g.append("g") // Append links to the group
    .attr("class", "links")
    .selectAll("line")
    .data(links)
    .enter().append("line")
    .attr("stroke-width", 2); // Ensure links are visible

const node = g.append("g") // Append nodes to the group
    .attr("class", "nodes")
    .selectAll("circle")
    .data(nodes)
    .enter().append("circle")
    .attr("r", 5)
    .attr("fill", d => d.color)
    .call(d3.drag()
        .on("start", dragstarted)
        .on("drag", dragged)
        .on("end", dragged));

const labels = g.append("g") // Append labels to the group
    .attr("class", "labels")
    .selectAll("text")
    .data(nodes)
    .enter().append("text")
    .attr("dx", 12)
    .attr("dy", ".35em")
    .text(d => d.label);

simulation
    .on("tick", ticked);

function ticked() {{
    link
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);

    node
        .attr("cx", d => d.x)
        .attr("cy", d => d.y);

    labels
        .attr("x", d => d.x)
        .attr("y", d => d.y);
}}

function dragstarted(event, d) {{
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
}}

function dragged(event, d) {{
    d.fx = event.x;
    d.fy = event.y;
}}

function dragged(event, d) {{
    if (!event.active) simulation.alphaTarget(0); // Set alphaTarget to 0 when dragging ends
    d.fx = null; // Release fixed position
    d.fy = null; // Release fixed position
}}

// Add zoom behavior
const zoom = d3.zoom()
    .scaleExtent([0.1, 40]) // Set zoom limits
    .on("zoom", zoomed);

svg.call(zoom);

function zoomed(event) {{
    g.attr("transform", event.transform);
}}

</script>
</body>
</html>
"""

# Write the HTML file
with open('cmbs_graph.html', 'w') as f:
    f.write(html_template.format(nodes=json.dumps(nodes), links=json.dumps(links)))
print('Interactive D3.js graph visualization has been saved as cmbs_graph.html')