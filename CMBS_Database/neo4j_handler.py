from neo4j import GraphDatabase

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7689"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "testtest"

class DealLister:
    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def list_deals(self):
        with self.driver.session() as session:
            result = session.run("MATCH (d:Deal) RETURN d")
            print("Deals in the database:")
            for record in result:
                deal = record["d"]
                print(deal)

    def clean_database(self, database=None):
        """Remove all nodes and relationships from the database."""
        with self.driver.session(database=database) if database else self.driver.session() as session:
            print("Cleaning up the database (removing all nodes and relationships)...")
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleanup complete.")

    def list_databases(self):
        """List all database names in the Neo4j instance."""
        with self.driver.session(database="system") as session:
            result = session.run("SHOW DATABASES")
            db_names = [record["name"] for record in result]
            print("Databases:")
            for name in db_names:
                print(name)
        return db_names

    def create_database(self, db_name):
        """Create a new database with the given name."""
        with self.driver.session(database="system") as session:
            try:
                session.run(f"CREATE DATABASE `{db_name}`")
                print(f"Database '{db_name}' created successfully.")
            except Exception as e:
                print(f"Failed to create database '{db_name}': {e}")

    def execute_cypher_file(self, file_path, database=None):
        with open(file_path, 'r', encoding='utf-8') as f:
            cypher_commands = [cmd.strip() for cmd in f.read().split(';') if cmd.strip()]
        total = len(cypher_commands)
        with self.driver.session(database=database) if database else self.driver.session() as session:
            for idx, command in enumerate(cypher_commands, 1):
                print(f"Executing command {idx}/{total}...")
                try:
                    session.run(command)
                    print(f"Command {idx} executed successfully.")
                except Exception as e:
                    print(f"Error executing command {idx}: {e}")

    def search_deal_by_address(self, address):
        with self.driver.session(database=self.database) as session:
            # Find deals by address
            deal_query = (
                "MATCH (d:Deal) WHERE d.address = $address RETURN d"
            )
            deal_results = session.run(deal_query, address=address)
            deals = [record["d"] for record in deal_results]
            if deals:
                print(f"Deals found for address '{address}':")
                for deal in deals:
                    print(dict(deal))
                    # Now find properties related to this deal via PARTOFDEAL
                    prop_query = (
                        "MATCH (p)-[:PARTOFDEAL]->(d {id: $deal_id}) RETURN p"
                    )
                    prop_results = session.run(prop_query, deal_id=deal['id'])
                    properties = [record["p"] for record in prop_results]
                    if properties:
                        print(f"  Properties for deal ID '{deal['id']}':")
                        for prop in properties:
                            print(f"    {dict(prop)}")
                    else:
                        print(f"  No properties found for deal ID '{deal['id']}'.")
            else:
                print(f"No deals found for address '{address}'.")
        return deals

    def list_properties_by_deal_id(self, deal_id):
        with self.driver.session(database=self.database) as session:
            query = (
                "MATCH (p)-[:PARTOFDEAL]->(d {id: $deal_id}) "
                "RETURN p"
            )
            result = session.run(query, deal_id=deal_id)
            properties = [record["p"] for record in result]
            if properties:
                print(f"Properties for deal ID '{deal_id}':")
                for prop in properties:
                    print(dict(prop))
            else:
                print(f"No properties found for deal ID '{deal_id}'.")
        return properties

    def get_cusip_by_deal_id(self, deal_id):
        with self.driver.session(database=self.database) as session:
            query = (
                "MATCH (d {id: $deal_id})-[:HASSECURITY]->(c {id: $cusip_id}) "
                "RETURN c.id AS cusip"
            )
            # Extract CUSIP part from deal_id (e.g., 'deal:14' -> 'cusip:05591XAE1')
            # You may need to adjust this logic based on your actual mapping
            # For demonstration, let's assume you pass the correct cusip_id
            cusip_id = self.get_cusip_id_for_deal(deal_id) if hasattr(self, 'get_cusip_id_for_deal') else None
            result = session.run(query, deal_id=deal_id, cusip_id=cusip_id)
            record = result.single()
            if record and record["cusip"]:
                print(f"CUSIP for deal ID '{deal_id}': {record['cusip']}")
                return record["cusip"]
            else:
                print(f"No CUSIP found for deal ID '{deal_id}'.")
                return None

    def get_bloomberg_name_by_deal_id(self, deal_id):
        """Retrieve the Bloomberg name for a given deal ID from the Neo4j database."""
        with self.driver.session(database=self.database) as session:
            result = session.run(
                "MATCH (d:Deal {id: $deal_id}) RETURN d.bloomberg AS bloomberg",
                deal_id=deal_id
            )
            record = result.single()
            if record and record["bloomberg"]:
                print(f"Bloomberg name for deal ID '{deal_id}': {record['bloomberg']}")
                return record["bloomberg"]
            else:
                print(f"No Bloomberg name found for deal ID '{deal_id}'.")
                return None

if __name__ == "__main__":
    lister = DealLister(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,'gi-cmbs')
    lister.execute_cypher_file('/Users/jackyfox/Documents/CMBS_Database/cmbs_graph_05591XAE1.cypher',database='gi-cmbs')
    # lister.execute_cypher_file('/Users/jackyfox/Documents/CMBS_Database/cmbs_graph_05491UBE7.cypher',database='gi-cmbs')

    try:
        lister.list_deals()
        lister.list_properties_by_deal_id("deal:14")
        lister.list_databases()
        lister.get_bloomberg_name_by_deal_id("deal:14")
        # lister.create_database("gi-cmbs")
        # lister.clean_database()
    finally:
        lister.close()


