import sqlite3
import pandas as pd
import os
import json
from typing import List, Dict, Any, Optional, Union
from jsonld_to_cypher import convert_jsonld_file_to_cypher


# Main handler class for CMBS database operations
class CMBSDatabaseHandler:
    """
    A class to handle operations on CMBS SQLite database files.
    """

    def __init__(self, db_path: str):
        """
        Initialize the database handler with a path to the SQLite database.
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self._validate_db_path()

    def _validate_db_path(self) -> None:
        """
        Ensure the database file exists at the given path.
        Raises:
            FileNotFoundError: If the database file does not exist
        """
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database file not found at: {self.db_path}")

    def _execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """
        Execute a SQL query and return the results as a DataFrame.
        Args:
            query (str): SQL query to execute
            params (tuple): Parameters for the query
        Returns:
            pd.DataFrame: Results of the query
        """
        conn = None  # Initialize conn to None
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()  # Ensure connection is closed in finally block

    def get_all_holdings_cusip(self) -> list:
        """
        Retrieve all CUSIPs from the account_holding table.
        Returns:
            List[str]: A list of all CUSIPs in the account_holding table
        """
        try:
            query = "SELECT cusip FROM account_holding"
            cusips_df = self._execute_query(query)
            return cusips_df['cusip'].tolist()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'account_holding' table was not found.")
                return []
            else:
                print(f"An error occurred: {e}")
                return []

    def get_deal_id_by_cusip(self, cusip: str) -> Optional[int]:
        """
        Retrieve the deal_id associated with a given CUSIP from the deal_tranche table.
        Args:
            cusip (str): The CUSIP to look up
        Returns:
            Optional[int]: The deal_id associated with the CUSIP, or None if not found
        """
        try:
            query = "SELECT deal_id FROM deal_tranche WHERE tr_cusip = ?"
            result = self._execute_query(query, (cusip,))
            if not result.empty:
                return result['deal_id'].iloc[0]
            print(f"No deal_id found for CUSIP: {cusip}")
            return None
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'deal_tranche' table was not found.")
                # Attempt alternative lookup if schema is different
                print("Attempting to find deal_id through alternative methods...")
                try:
                    custom_query = """
                    SELECT d.deal_id 
                    FROM deals d 
                    JOIN some_intermediate_table sit ON d.some_id = sit.some_id 
                    JOIN account_holding ah ON sit.some_other_id = ah.some_other_id 
                    WHERE ah.cusip = ?
                    """
                    result = self._execute_query(custom_query, (cusip,))
                    if not result.empty:
                        return result['deal_id'].iloc[0]
                except sqlite3.Error:
                    pass
                print(f"Could not find deal_id for CUSIP: {cusip} using alternative methods")
                return None
            else:
                print(f"An error occurred: {e}")
                return None

    def get_deals(self) -> pd.DataFrame:
        """
        Retrieve all deals from the deals table.
        
        Returns:
            pd.DataFrame: DataFrame containing all deals
        """
        try:
            query = "SELECT * FROM deals"
            return self._execute_query(query)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'deals' table was not found.")
                return pd.DataFrame()
            else:
                print(f"An error occurred: {e}")
                return pd.DataFrame()

    def get_collateral(self, deal_id: Optional[int] = None) -> pd.DataFrame:
        """
        Retrieve collateral data, optionally filtered by deal_id.
        
        Args:
            deal_id (Optional[int]): Deal ID to filter by
            
        Returns:
            pd.DataFrame: DataFrame containing collateral data
        """
        try:
            if deal_id is not None:
                query = "SELECT * FROM collateral WHERE deal_id = ?"
                return self._execute_query(query, (deal_id,))
            else:
                query = "SELECT * FROM collateral"
                return self._execute_query(query)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'collateral' table was not found.")
                return pd.DataFrame()
            else:
                print(f"An error occurred: {e}")
                return pd.DataFrame()

    def get_account_holdings(self, cusip: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve account holdings data, optionally filtered by CUSIP.
        
        Args:
            cusip (Optional[str]): CUSIP to filter by
            
        Returns:
            pd.DataFrame: DataFrame containing account holdings data
        """
        try:
            if cusip is not None:
                query = "SELECT * FROM account_holding WHERE cusip = ?"
                return self._execute_query(query, (cusip,))
            else:
                query = "SELECT * FROM account_holding"
                return self._execute_query(query)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'account_holding' table was not found.")
                return pd.DataFrame()
            else:
                print(f"An error occurred: {e}")
                return pd.DataFrame()

    def execute_custom_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """
        Execute a custom SQL query.
        
        Args:
            query (str): SQL query to execute
            params (tuple): Parameters for the query
            
        Returns:
            pd.DataFrame: Results of the query
        """
        return self._execute_query(query, params)

    def get_issuer_name_by_cusip(self, cusip: str) -> Optional[str]:
        """
        Function to retrieve the issuer name associated with a given CUSIP.
        This function looks for the CUSIP in the account_holding table and returns the associated ult_issuer_name.
        
        Args:
            cusip (str): The CUSIP to look up
            
        Returns:
            Optional[str]: The issuer name associated with the CUSIP, or None if not found
        """
        try:
            query = "SELECT ult_issuer_name FROM account_holding WHERE cusip = ?"
            result = self._execute_query(query, (cusip,))

            if not result.empty:
                return result['ult_issuer_name'].iloc[0]

            # If we can't find the CUSIP, return None
            print(f"No issuer name found for CUSIP: {cusip}")
            return None

        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'account_holding' table was not found.")
                return None
            elif "no such column" in str(e):
                print("The 'ult_issuer_name' column was not found in the account_holding table.")
                return None
            else:
                print(f"An error occurred: {e}")
                return None

    def get_bloomberg_name_by_deal_id(self, deal_id: int) -> Optional[str]:
        """
        Function to retrieve the Bloomberg name associated with a given deal_id.
        This function looks for the deal_id in the deals table and returns the associated bloomberg_name.
        
        Args:
            deal_id (int): The deal_id to look up
            
        Returns:
            Optional[str]: The Bloomberg name associated with the deal_id, or None if not found
        """
        try:
            query = "SELECT bloomberg_name FROM deals WHERE deal_id = ?"
            result = self._execute_query(query, (deal_id,))

            if not result.empty:
                return result['bloomberg_name'].iloc[0]

            # If we can't find the deal_id, return None
            print(f"No Bloomberg name found for deal_id: {deal_id}")
            return None

        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'deals' table was not found.")
                return None
            elif "no such column" in str(e):
                print("The 'bloomberg_name' column was not found in the deals table.")
                return None
            else:
                print(f"An error occurred: {e}")
                return None

    def get_owner_info_by_deal_id(self, deal_id: int) -> Optional[list]:
        """
        Retrieve owner_name and owner_type from the propinfo table for a given deal_id.
        Args:
            deal_id (int): The deal_id to look up
        Returns:
            Optional[list]: A list of dictionaries with owner_name and owner_type, or None if not found
        """
        try:
            query = "SELECT owner_name, owner_type FROM propinfo WHERE deal_id = ?"
            result = self._execute_query(query, (deal_id,))
            if not result.empty:
                return result.apply(lambda row: {
                    'owner_name': row['owner_name'],
                    'owner_type': row['owner_type']
                }, axis=1).tolist()
            print(f"No owner information found for deal_id: {deal_id}")
            return None
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'propinfo' table was not found.")
                return None
            elif "no such column" in str(e):
                print("The 'owner_name' or 'owner_type' column was not found in the propinfo table.")
                return None
            else:
                print(f"An error occurred: {e}")
                return None

    def get_property_info_by_deal_id(self, deal_id: int) -> Optional[Dict[str, Any]]:
        """
        Function to retrieve property information (address, year_built, trustee_prop_type_full)
        for a given deal_id from the propinfo table.
        
        Args:
            deal_id (int): The deal_id to look up
            
        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the property information,
                                     or None if not found
        """
        try:
            query = """
            SELECT address, year_built, trustee_prop_type_full, state, msa_name, prop_name
            FROM propinfo 
            WHERE deal_id = ?
            """
            result = self._execute_query(query, (deal_id,))

            if not result.empty:
                return result.apply(lambda row: {
                    'address': row['address'],
                    'year_built': row['year_built'],
                    'trustee_prop_type_full': row['trustee_prop_type_full'],
                    'state': row['state'],
                    'msa_name': row['msa_name'],
                    'prop_name': row['prop_name']
                }, axis=1).tolist()

            print(f"No property information found for deal_id: {deal_id}")
            return None

        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print("The 'propinfo' table was not found.")
                return None
            else:
                print(f"An error occurred: {e}")
                return None

    def print_node_info_from_jsonld(self, cusip_to_load):
        """Reads a JSON-LD file for a given CUSIP and prints node information."""
        input_filename = f"cmbs_graph_{cusip_to_load}.jsonld"
        input_file_path = os.path.join(os.path.dirname(self.db_path), input_filename)

        try:
            with open(input_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"Error: JSON-LD file not found at {input_file_path}")
            return
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {input_file_path}")
            return

        print(f"\n--- Node Information for CUSIP: {cusip_to_load} ---")
        if "@graph" in data:
            for node in data["@graph"]:
                print(f"\nNode ID: {node.get('@id')}")
                print(f"  Type: {node.get('@type')}")
                for key, value in node.items():
                    if key not in ["@id", "@type"]:
                        if isinstance(value, dict) and "@id" in value:  # Link to another node
                            print(f"  {key}: -> {value['@id']}")
                        elif isinstance(value, list):
                            print(f"  {key}:")
                            for item in value:
                                if isinstance(item, dict) and "@id" in item:
                                    print(f"    - -> {item['@id']}")
                                else:
                                    print(f"    - {item}")
                        else:
                            print(f"  {key}: {value}")
        else:
            print("No graph data found in the JSON-LD file.")

    def export_cusip_data_to_jsonld(self, cusip_to_export):
        """
        Exports all relevant data for a single CUSIP to a JSON-LD file, which can be used for graph database import.
        """
        json_ld_data = {
            "@context": {
                "cusip": "http://schema.org/identifier",
                "deal": "http://schema.org/Product",
                "property": "http://schema.org/Place",
                "issuer": "http://schema.org/Organization",
                "bloomberg": "http://schema.org/identifier",
                "address": "http://schema.org/address",
                "yearBuilt": "http://schema.org/dateCreated",
                "propertyType": "http://schema.org/propertyType",
                "dealId": "http://schema.org/productID",
                "hasProperty": "http://schema.org/location",
                "issuedBy": "http://schema.org/issuedBy",
                "issues": "http://schema.org/makesOffer",
                "partOfDeal": "http://schema.org/isPartOf"
            },
            "@graph": []
        }
        deal_id = str(self.get_deal_id_by_cusip(cusip_to_export))
        issuer_name = self.get_issuer_name_by_cusip(cusip_to_export)
        RAG_deal_description = ""
        plt_vertex_nodes = "dealId|bloombergName|cusip|propertyId|addressId|yearBuilt|trusteePropType"+"\n"
        plt_vertex_relations = "parent|relation|child"+"\n"
        if deal_id is not None and deal_id.lower() != 'none':
            bloomberg_name = self.get_bloomberg_name_by_deal_id(deal_id)
            # print(f"*********Deal_id: {deal_id}")
            prop_info_list = self.get_property_info_by_deal_id(deal_id)
            # print(f"*********Prop_info_list: {prop_info_list}")
            # Create the Deal node
            deal_node = {
                "@type": "Deal",
                "@id": f"{deal_id}",
                "dealId": deal_id,
                "bloomberg": bloomberg_name if bloomberg_name else "None",
                "cusip": cusip_to_export if cusip_to_export else "None"
            }
            if prop_info_list:
                deal_node["hasProperty"] = []
                # For each property, create nodes and link them
                for prop_info in prop_info_list:
                    address_for_id = (
                        f"{prop_info['address']}, {prop_info['state']}" if prop_info['address'] and 'state' in prop_info and prop_info['state']
                        else (prop_info['address'] if prop_info['address'] else "UnknownAddress")
                    )
                    # print(f"*********address_for_id: {address_for_id}")
                    # a=input("Press any key to continue...")
                    property_id = f"property:{deal_id}:{address_for_id}"
                    address_id = f"{address_for_id}"
                    year_built_id = f"yearbuilt:{prop_info['year_built']}"
                    trustee_prop_type_full_id = f"trusteeproptype:{prop_info['trustee_prop_type_full']}"
                    owner_name = prop_info.get('owner_name', 'UnknownOwner')
                    owner_type = prop_info.get('owner_type', 'UnknownType')
                    property_owner_id = f"property_owner:{owner_name}"
                    # Add msa_name and prop_name nodes
                    msa_name = prop_info.get('msa_name', 'UnknownMSA')
                    prop_name = prop_info.get('prop_name', 'UnknownPropName')
                    msa_name_id = f"{msa_name}"
                    prop_name_id = f"{prop_name}"

                    # Create the descption entry for RAG
                    description = f"the deal {deal_id} has cusip:{cusip_to_export}, the name of this security is {bloomberg_name}, it contains property: {prop_name}, which address is {address_for_id},in the MSA arae:{msa_name} , was built in {prop_info['year_built']} , the trustee property type is {prop_info['trustee_prop_type_full']}"
                    RAG_deal_description += description + "\n"



                    # Create the vertex entry for plt
                    vertex_node_entry = f"{deal_id}|{bloomberg_name}|{cusip_to_export}|{property_id}|{address_id}|{year_built_id}|{trustee_prop_type_full_id}"+"\n"
                    plt_vertex_nodes += vertex_node_entry

                    vertex_relation_entry = f"{bloomberg_name}|hasProperty|{prop_name}"+"\n"+f"{prop_name}|locatedAt|{address_id}"+"\n"+f"{prop_name}|isUsedAs|{trustee_prop_type_full_id}"+"\n"
                    plt_vertex_relations += vertex_relation_entry


                    # Create the knowledge graph node
                    address_node = {
                        "@type": "Address",
                        "@id": address_id,
                    }
                    year_built_node = {
                        "@type": "YearBuilt",
                        "@id": year_built_id,
                    }
                    trustee_prop_type_full_node = {
                        "@type": "TrusteePropTypeFull",
                        "@id": trustee_prop_type_full_id,
                        "usedProperty": {"@id": property_id}
                    }
                    property_owner_node = {
                        "@type": "PropertyOwner",
                        "@id": property_owner_id,
                        "ownerName": owner_name,
                        "ownerType": owner_type
                    }

                    msa_name_node = {
                        "@type": "MSAName",
                        "@id": msa_name_id,
                        "name": msa_name
                    }
                    prop_name_node = {
                        "@type": "PropName",
                        "@id": prop_name_id,
                        "name": prop_name
                    }
                    property_node = {
                        "@type": "Property",
                        "@id": property_id,
                        "locatedAt": {"@id": address_id},
                        "builtAt": {"@id": year_built_id},
                        "partOfDeal": {"@id": f"deal:{deal_id}"},
                        "propertyType": {"@id": trustee_prop_type_full_id},
                        "ownedBy": {"@id": property_owner_id},
                        "inMsa": {"@id": msa_name_id},
                        "namedAs": {"@id": prop_name_id}
                    }
                    deal_node["hasProperty"].append({"@id": property_id})
                    json_ld_data["@graph"].append(address_node)
                    json_ld_data["@graph"].append(year_built_node)
                    json_ld_data["@graph"].append(property_node)
                    json_ld_data["@graph"].append(trustee_prop_type_full_node)
                    json_ld_data["@graph"].append(property_owner_node)
                    json_ld_data["@graph"].append(msa_name_node)
                    json_ld_data["@graph"].append(prop_name_node)
                json_ld_data["@graph"].append(deal_node)
            output_filename = f"cmbs_graph_{cusip_to_export}.jsonld"
            output_file_path = os.path.join(os.path.dirname(self.db_path), output_filename)
            print(f"*********output_file_path: {output_file_path}")
            if os.path.exists(output_file_path):
                os.remove(output_file_path)
            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(json_ld_data, f, indent=2, ensure_ascii=False)
            print(f"\nJSON-LD data for CUSIP {cusip_to_export} has been exported to: {output_file_path}")

            rag_output_filename = f"cmbs_rag_{cusip_to_export}.txt"
            rag_output_file_path = os.path.join(os.path.dirname(self.db_path), rag_output_filename)
            if os.path.exists(rag_output_file_path):
                os.remove(rag_output_file_path)
            with open(rag_output_file_path, 'w', encoding='utf-8') as f:
                f.write(RAG_deal_description)
            print(f"\nRAG description for CUSIP {cusip_to_export} has been exported to: {rag_output_file_path}")

            pltr_vertex_relations_output_filename = f"cmbs_pltr_nodes_{cusip_to_export}.csv"
            pltr_vertex_nodes_output_file_path = os.path.join(os.path.dirname(self.db_path), pltr_vertex_relations_output_filename)
            if os.path.exists(pltr_vertex_nodes_output_file_path):
                os.remove(pltr_vertex_nodes_output_file_path)
            with open(pltr_vertex_nodes_output_file_path, 'w', encoding='utf-8') as f:
                f.write(plt_vertex_nodes)
            print(f"\nPalantir description for CUSIP {cusip_to_export} has been exported to: {pltr_vertex_relations_output_filename}")
            
            pltr_vertex_relations_output_filename = f"cmbs_pltr_edges_{cusip_to_export}.csv"
            pltr_vertex_nodes_output_file_path = os.path.join(os.path.dirname(self.db_path), pltr_vertex_relations_output_filename)
            if os.path.exists(pltr_vertex_nodes_output_file_path):
                os.remove(pltr_vertex_nodes_output_file_path)
            with open(pltr_vertex_nodes_output_file_path, 'w', encoding='utf-8') as f:
                f.write(plt_vertex_relations)
            print(f"\nPalantir description for CUSIP {cusip_to_export} has been exported to: {pltr_vertex_relations_output_filename}")
            

            return output_file_path
        return None


# Example usage of the class
if __name__ == "__main__":
    # Set the default path to the Intex SQLite database
    default_db_path = '/Users/jackyfox/PycharmProjects/TWGglobal_fc/CMBS_Database/CMBS_H_20250430'
    db_handler = CMBSDatabaseHandler(default_db_path)
    # Get all CUSIPs and process one as an example
    all_cusips = db_handler.get_all_holdings_cusip()
    if all_cusips:
        # cusip_to_process = all_cusips[6]  # Taking one of the CUSIPs as an example
        for cusip_to_process in all_cusips[12:13]:
            print(f"Processing data for CUSIP: {cusip_to_process}")
            input_jsonld=db_handler.export_cusip_data_to_jsonld(cusip_to_process)  # Export to JSON-LD
            # input_jsonld = f"/Users/jacpltr_vertex_nodes_output_fikyfox/PycharmProjects/TWGglobal_fc/CMBS_Database/cmbs_graph_{cusip_to_process}.jsonld"
            if input_jsonld != None:
                output_cypher = f"/Users/jackyfox/PycharmProjects/TWGglobal_fc/CMBS_Database/cmbs_graph_{cusip_to_process}.cypher"
                convert_jsonld_file_to_cypher(input_jsonld, output_cypher)
    else:
        print("No CUSIPs found to process.")
