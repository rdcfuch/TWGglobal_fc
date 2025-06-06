from mcp.server.fastmcp import FastMCP
from neo4j_handler import DealLister

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7689"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "testtest"
DATABASE = "gi-cmbs"

# Initialize DealLister
deal_lister = DealLister(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DATABASE)

# Create FastMCP server
server = FastMCP()

# Define MCP tools
# @server.tool()
# def list_deals():
#     """List all deals in the database with their properties and addresses."""
#     return deal_lister.list_deals()

# @server.tool()
# def search_deal_by_address(address: str):
#     """Search for deals by address and return a list of matching deals."""
#     return deal_lister.search_deal_by_address(address)

# @server.tool()
# def list_properties_by_deal_id(deal_id: str):
#     """List properties associated with a specific deal ID."""
#     return deal_lister.list_properties_by_deal_id(deal_id)

@server.tool()
def get_bloomberg_name_by_deal_id(deal_id: str):
    """Retrieve the Bloomberg name for a given deal ID."""
    return deal_lister.get_bloomberg_name_by_deal_id(deal_id)

@server.tool()
def search_deal_id_by_address(address: str):
    """Search for deal IDs by address and return a list of matching deal IDs."""
    return deal_lister.search_deal_id_by_address(address)

# @server.tool()
# def show_address_by_property_id(property_id: str):
#     """Show the address for a given property ID."""
#     return deal_lister.show_address_by_property_id(property_id)

if __name__ == "__main__":
    server.run()