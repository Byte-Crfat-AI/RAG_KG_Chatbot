import streamlit as st
from neo4j import GraphDatabase
from openai import OpenAI
import os

# Neo4j connection details
NEO4J_URL = "bolt://localhost:7687"
NEO4J_DATABASE = "neo4j"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# OpenAI API key
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
EMBEDDING_MODEL = "text-embedding-ada-002"

def get_embedding(client, text, model):
    response = client.embeddings.create(
        input=text,
        model=model,
    )
    return response.data[0].embedding

def load_embedding(label, property):
    driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD), database=NEO4J_DATABASE)
    openai_client = OpenAI(api_key=OPENAI_API_KEY)

    with driver.session() as session:
        result = session.run(f"MATCH (ch:{label}) RETURN id(ch) AS id, ch.{property} AS text")
        count = 0
        for record in result:
            id = record["id"]
            text = record["text"]
            embedding = get_embedding(openai_client, text, EMBEDDING_MODEL)
            cypher = "CREATE (e:Embedding) SET e.key=$key, e.value=$embedding, e.model=$model"
            cypher += " WITH e MATCH (n) WHERE id(n) = $id CREATE (n) -[:HAS_EMBEDDING]-> (e)"
            session.run(cypher, key=property, embedding=embedding, id=id, model=EMBEDDING_MODEL)
            count += 1
        session.close()
        return count

# Streamlit UI
st.title("Knowledge Graph Embedding Loader")

label = st.text_input("Node Label", "Chunk")
property = st.text_input("Property", "text")

if st.button("Load Embeddings"):
    count = load_embedding(label, property)
    st.success(f"Loaded embeddings for {count} nodes.")

if st.button("Initialize Schema"):
    initialiseNeo4jSchema()
    st.success("Schema initialized.")

def initialiseNeo4jSchema():
    cypher_schema = [
        "CREATE CONSTRAINT sectionKey IF NOT EXISTS FOR (c:Section) REQUIRE (c.key) IS UNIQUE;",
        "CREATE CONSTRAINT chunkKey IF NOT EXISTS FOR (c:Chunk) REQUIRE (c.key) IS UNIQUE;",
        "CREATE CONSTRAINT documentKey IF NOT EXISTS FOR (c:Document) REQUIRE (c.url_hash) IS UNIQUE;",
        "CREATE VECTOR INDEX `chunkVectorIndex` IF NOT EXISTS FOR (e:Embedding) ON (e.value) OPTIONS { indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}};"
    ]

    driver = GraphDatabase.driver(NEO4J_URL, database=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        for cypher in cypher_schema:
            session.run(cypher)
    driver.close()

# Run the Streamlit app
if __name__ == "__main__":
    st.run()