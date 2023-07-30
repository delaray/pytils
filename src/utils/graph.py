# *********************************************************************
# KGML Graph Utilities and Functions
# *********************************************************************

# Python Imports
import os
import platform
import pandas as pd

# Neo4j imports
from neo4j import GraphDatabase

# Project imports
from utils.utils import timing

# *********************************************************************
# Neo4j Connection
# *********************************************************************

# Operating System
PLATFORM = platform.system()

# Detect platform and choose local or remote NEO4J server

def set_graph_platform(platform):
    if platform == "Windows" or platform == 'Darwin':
        NEO4J_URI = 'neo4j://localhost:7687'
        NEO4J_USER = 'neo4j'
        NEO4J_PWD = os.environ.get('PPWD1', 'neo4j')
    else:
        NEO4J_URI = os.environ.get('KGML_NEO4J_URI')
        NEO4J_USER = os.environ.get('KGML_NEO4J_USER')
        NEO4J_PWD = os.environ.get('KGML_NEO4J_PWD')

# --------------------------------------------------------------------------

NEO4J_URI = os.environ.get('KGML_NEO4J_URI', os.environ['NEO4J_ADEO_URI'])
NEO4J_USER = os.environ.get('KGML_NEO4J_USER', os.environ['NEO4J_ADEO_USER'])
NEO4J_PWD = os.environ.get('KGML_NEO4J_PWD', os.environ['NEO4J_ADEO_PWD'])


ADEO_NEO4J_URI = NEO4J_URI
ADEO_NEO4J_USER = NEO4J_USER
ADEO_NEO4J_PWD = NEO4J_PWD

# --------------------------------------------------------------------------
# Graph_app Neo4j driver session Class
# --------------------------------------------------------------------------

class Graph_app:

    def __init__(self, uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD):
        self.uri = uri
        self.user = user
        self.pwd = pwd
        self.driver = None
        try:
            self.driver = \
                GraphDatabase.driver(self.uri, auth=(self.user, self.pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def query(self, query, db=None):
        assert self.driver is not None, "Driver not initialized!"
        with self.driver.session() as session:
            return list(session.run(query))

    def __del__(self):
        self.close()


# --------------------------------------------------------------------------

def graph_driver(uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD):
    return Graph_app(uri, user, pwd)


# --------------------------------------------------------------------------

def run_query(query, uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD,
              driver=None):
    'Returns the result of running the Cypher query.'

    driver = Graph_app(uri, user, pwd) if driver is None else driver
    query_result = driver.query(query)
    return query_result, driver


# --------------------------------------------------------------------------

def extract_nodes(result, key='n'):
    'Returns a list of objects extracted from result records with key'
    
    return list(map(lambda r: r[key], result))


# **************************************************************************
# ADEO Graph Specific Operations
# **************************************************************************

SERVER = 'remote'

# --------------------------------------------------------------------------
# Adeo_graph_p 
# --------------------------------------------------------------------------

def adeo_graph_p(uri, server=SERVER):
    'Return True if <uri> is an Adeo graph and server is not local.'
    
    return not (uri == 'neo4j://localhost:7687') and not (server == 'local')


# --------------------------------------------------------------------------
# Valid Results
# --------------------------------------------------------------------------

def valid_result_p(result):
    'Returns True if result is non-empty.'
    
    return (result is not None) and (result != [])

# --------------------------------------------------------------------------
# Get Graph Node by ID
# --------------------------------------------------------------------------

def get_node(id, uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD):
    'Returns the node with the specied Neo4j ID or None.'

    query = f"MATCH (n) WHERE ID(n) = {id} RETURN n"
    result = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    
    return extract_nodes(result)[0] if result != [] else None
    

# --------------------------------------------------------------------------

def node_properties(node):
    return node._properties

# --------------------------------------------------------------------------
# Node Keys and Labels
# --------------------------------------------------------------------------

def node_keys(node):
    return list(node._properties.keys())


def nodes_keys(nodes):
    keys = set()
    [[keys.add(key) for key in node_keys(node)] for node in nodes]
    return list(keys)


# --------------------------------------------------------------------------

def node_id(node):
    'Returns the trailing numeric portion to the Poolparty URL'
    
    return node['code'].split('/')[-1]


# --------------------------------------------------------------------------

def node_label(node):
    return node['prefLabel']


def node_labels(node):
    return {'label': node_label(node),
            'alternate': node['altLabels'],
            'hidden': node['hiddenLabels']}


# --------------------------------------------------------------------------
# Node To DataFrames
# --------------------------------------------------------------------------

def nodes_to_df(nodes):
    'Returns a DataFrame of nodes and their properties.'
    rows = []
    keys = nodes_keys(nodes)
    
    # Extract properties of each node
    for node in nodes:
        id = node.id
        properties = node._properties
        row = [id]
        for key in keys:
            row.append(properties.get(key, None))
        rows.append(row)
    # Create and return the DataFrame
    cols = ['id'] + keys
    df = pd.DataFrame(rows, columns=cols)
    return df

# --------------------------------------------------------------------------
# ADEO Concepts
# --------------------------------------------------------------------------

@timing
def query_nodes(uri=ADEO_NEO4J_URI, user=ADEO_NEO4J_USER, pwd=ADEO_NEO4J_PWD,
                label='ADEO_Concept'):
    'Returns a dataframe of result nodes.'
    
    query = f"MATCH (n:{label}) RETURN n"
    result, driver = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    
    if valid_result_p(result) is True:
        nodes = [r['n'] for r in result]
        return nodes_to_df(nodes)
    else:
        return None

# ----------------------------------------------------------------

def load_concepts(bu, data_name='knowledge_graph_concepts', 
                  source='storage', storage='orphans'):
    'Loads a nodes file for a particular BU.'

    df = load_data(bu, data_name, 'csv', source)
    if 'index' in df.columns:
        df.drop('index', axis=1, inplace=True)
        
    return df


# ----------------------------------------------------------------

def save_adeo_concepts(bu, df, data_name='knowledge_graph_concepts',
                       destination='storage', storage='orphans'):
    'Saves the concept relationships of a particular BU.'

    save_data(bu, data_name, 'csv', df, destination)
    
    return True


# --------------------------------------------------------------------------
# Adeo Products
# --------------------------------------------------------------------------

def get_adeo_products(uri=ADEO_NEO4J_URI, user=ADEO_NEO4J_USER,
                      pwd=ADEO_NEO4J_PWD):
    query = "MATCH (n:ADEO_Product) RETURN n"
    result, driver = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    if valid_result_p(result) is True:
        nodes = [r['n'] for r in result]
        return nodes_to_df(nodes)
    else:
        return None


# --------------------------------------------------------------------------
# Adeo Inter-Product Relationships
# --------------------------------------------------------------------------

def get_adeo_product_relationships(uri=ADEO_NEO4J_URI, user=ADEO_NEO4J_USER,
                                   pwd=ADEO_NEO4J_PWD):
    query = """MATCH
    p=(n:ADEO_Product)-[:COULD_TRIGGER_THE_PURCHASE_OF]-(m:ADEO_Product)
    RETURN p, n, m
    """
    result, driver = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    return result

# ****************************************************************
# End of File
# ****************************************************************
