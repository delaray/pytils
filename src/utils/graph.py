# *********************************************************************
# Graph Utilities and Functions
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

# Retrieves the Operating System platform.

PLATFORM = platform.system()

# --------------------------------------------------------------------------

NEO4J_URI = os.environ.get('BABAR_NEO4J_URI','neo4j://localhost:7687')
NEO4J_USER = os.environ.get('BABAR_NEO4J_USER', 'neo4j')
NEO4J_PWD = os.environ.get('BABAR_NEO4J_PWD', 'neo4j')


# --------------------------------------------------------------------------

def set_graph_platform(platform):
    'Sets Neo4j authentication globals based on platform.'

    global NEO4J_URI, NEO4J_USER, NEO4J_PWD
    
    if platform == "Windows" or platform == 'Darwin':
        NEO4J_URI = 'neo4j://localhost:7687'
        NEO4J_USER = 'neo4j'
        NEO4J_PWD = os.environ.get('PPWD1', 'neo4j')
    else:
        NEO4J_URI = os.environ.get('BABAR_NEO4J_URI','neo4j://localhost:7687')
        NEO4J_USER = os.environ.get('BABAR_NEO4J_USER', 'neo4j')
        NEO4J_PWD = os.environ.get('BABAR_NEO4J_PWD', 'neo4j')

    return True


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
# Graph Specific Operations
# **************************************************************************

SERVER = 'remote'

# --------------------------------------------------------------------------
# Remote Graph Predicate
# --------------------------------------------------------------------------

def remote_graph_p(uri, server=SERVER):
    'Return True if <uri> is a remote graph and server is not local.'
    
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
# Babar Concepts
# --------------------------------------------------------------------------

@timing
def query_nodes(uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD,
                label=None):
    'Returns a dataframe of result nodes.'

    if label is not None:
        query = f"MATCH (n:{label}) RETURN n"
    else:
        query = "MATCH (n) RETURN n"

    result, driver = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    
    if valid_result_p(result) is True:
        nodes = [r['n'] for r in result]
        return nodes_to_df(nodes)
    else:
        return None

# ----------------------------------------------------------------

def load_concepts(project_name, data_name='babar_concepts', 
                  source='storage', storage='data'):
    'Loads a nodes file for a particular project.'

    df = load_data(project_name, data_name, 'csv', source)
    if 'index' in df.columns:
        df.drop('index', axis=1, inplace=True)
        
    return df


# ----------------------------------------------------------------

def save_concepts(project_name, df, data_name='babar_concepts',
                       destination='storage', storage='data'):
    'Saves the concept relationships of a particular BU.'

    save_data(project_name, data_name, 'csv', df, destination)
    
    return True


# --------------------------------------------------------------------------
# Products
# --------------------------------------------------------------------------

def get_products(uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD):
    query = "MATCH (n) RETURN n"
    result, driver = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    if valid_result_p(result) is True:
        nodes = [r['n'] for r in result]
        return nodes_to_df(nodes)
    else:
        return None


# --------------------------------------------------------------------------
# Babar Inter-Product Relationships
# --------------------------------------------------------------------------

def get_product_relationships(uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PWD):
    query = '''
    MATCH p=(n)-[]-(m) RETURN p, n, m
    '''
    result, driver = run_query(query, uri=uri, user=user, pwd=pwd)
    driver.close()
    return result

# ****************************************************************
# End of File
# ****************************************************************
