# CMBS KG

This application provides a pipeline for converting Intex data into a knowledge graph, enabling advanced querying and AI-driven analysis using Neo4j.

## Overview

The workflow consists of three main components:

1. **Data Extraction and Conversion**
   - The script `CMBS_Database/extract_intex_db_to_kg.py` is responsible for extracting data from Intex SQLite databases and converting it into two formats:
     - **JSON-LD files**: These files represent the knowledge graph structure in a standard linked data format.
     - **Cypher command files**: These files contain Cypher queries for importing the data into a Neo4j database.
   - The script supports extracting CUSIP, deal, property, issuer, and related information, and organizes them into graph nodes and relationships.

2. **Neo4j Database Management and Data Import**
   - The `CMBS_Database/neo4j_handler.py` module provides a `DealLister` class and related utilities for managing the Neo4j database.
   - Features include:
     - Creating and deleting databases
     - Importing Cypher files generated from the previous step
     - Listing deals, properties, and related information
     - Querying by address, deal ID, or property ID
   - This module connects to a Neo4j server (default: `bolt://localhost:7689`) and supports database operations for the imported knowledge graph.

3. **API Exposure for AI Agents**
   - The `CMBS_Database/neo4j_cmbs_mcp_server.py` script exposes selected Neo4j database operations as API endpoints using the FastMCP framework.
   - This allows AI agents or external applications to interact with the knowledge graph, retrieve deal information, search by address, and more, via simple API calls.

## Usage

1. **Extract and Convert Data**
   - Run `extract_intex_db_to_kg.py` to process your Intex SQLite database and generate the required `.jsonld` and `.cypher` files.

2. **Import Data into Neo4j**
   - Use `neo4j_handler.py` to create a Neo4j database and import the generated Cypher file.
   - Example: `DealLister.execute_cypher_file('path/to/cypher_file.cypher', database='your-db-name')`

3. **Expose API for AI Agents**
   - Start the MCP server with `neo4j_cmbs_mcp_server.py` to provide API access to the Neo4j knowledge graph.

## Project Structure

- `CMBS_Database/extract_intex_db_to_kg.py`: Data extraction and conversion to JSON-LD and Cypher
- `CMBS_Database/neo4j_handler.py`: Neo4j database management and data import
- `CMBS_Database/neo4j_cmbs_mcp_server.py`: API server exposing Neo4j operations for AI agents

## Requirements

- Python 3.x
- pandas
- sqlite3
- neo4j Python driver
- FastMCP (for API server)
- Neo4j server (local or remote)

## Example Workflow

1. Extract data:
   ```bash
   python3 CMBS_Database/extract_intex_db_to_kg.py
   ```
2. Import into Neo4j:
   ```bash
   python3 CMBS_Database/neo4j_handler.py
   ```
3. Start API server:
   ```bash
   python3 CMBS_Database/neo4j_cmbs_mcp_server.py
   ```



