from openai import OpenAI
from neo4j import GraphDatabase

def get_embedding(client, text, model):
    response = client.embeddings.create(
                    input=text,
                    model=model,
                )
    return response.data[0].embedding

def LoadEmbedding(label, property):
    driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD), database=NEO4J_DATABASE)
    openai_client = OpenAI (api_key = os.environ["OPENAI_API_KEY"])

    with driver.session() as session:
        # get chunks in document, together with their section titles
        result = session.run(f"MATCH (ch:{label}) RETURN id(ch) AS id, ch.{property} AS text")
        # call OpenAI embedding API to generate embeddings for each proporty of node
        # for each node, update the embedding property
        count = 0
        for record in result:
            id = record["id"]
            text = record["text"]
            
            # For better performance, text can be batched
            embedding = get_embedding(openai_client, text, EMBEDDING_MODEL)
            
            # key property of Embedding node differentiates different embeddings
            cypher = "CREATE (e:Embedding) SET e.key=$key, e.value=$embedding, e.model=$model"
            cypher = cypher + " WITH e MATCH (n) WHERE id(n) = $id CREATE (n) -[:HAS_EMBEDDING]-> (e)"
            session.run(cypher,key=property, embedding=embedding, id=id, model=EMBEDDING_MODEL) 
            count = count + 1

        session.close()
        return count
    
LoadEmbedding("Chunk", "text")