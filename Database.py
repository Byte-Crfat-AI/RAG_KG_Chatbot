from llama_index.core.node_parser import MarkdownElementNodeParser
from llama_parse import LlamaParse
from neo4j import GraphDatabase

pdf_file_name = 'D:\RAG_KG_Chatbot\RAG_KG_Chatbot\ugrulebook.pdf'
documents = LlamaParse(result_type="markdown").load_data(pdf_file_name)
node_parser = MarkdownElementNodeParser(llm=llm, num_workers=8)
nodes = node_parser.get_nodes_from_documents(documents)
base_nodes, objects = node_parser.get_nodes_and_objects(nodes)

# Local Neo4j instance
# NEO4J_URL = "bolt://localhost:7687"
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
    
initialiseNeo4jSchema()
driver = GraphDatabase.driver(NEO4J_URL, database=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))
i = 0
with driver.session() as session:
    for doc in documents:
        cypher = "MERGE (d:Document {url_hash: $doc_id}) ON CREATE SET d.url=$url;"
        session.run(cypher, doc_id=doc.doc_id, url=doc.doc_id)
        i = i + 1
    session.close()


i = 0
with driver.session() as session:
    for node in base_nodes: 

        # >>1 Create Section node
        cypher  = "MERGE (c:Section {key: $node_id})\n"
        cypher += " FOREACH (ignoreMe IN CASE WHEN c.type IS NULL THEN [1] ELSE [] END |\n"
        cypher += "     SET c.hash = $hash, c.text=$content, c.type=$type, c.class=$class_name, c.start_idx=$start_idx, c.end_idx=$end_idx )\n"
        cypher += " WITH c\n"
        cypher += " MATCH (d:Document {url_hash: $doc_id})\n"
        cypher += " MERGE (d)<-[:HAS_DOCUMENT]-(c);"

        node_json = json.loads(node.json())

        session.run(cypher, node_id=node.node_id, hash=node.hash, content=node.get_content(), type='TEXT', class_name=node.class_name()
                          , start_idx=node_json['start_char_idx'], end_idx=node_json['end_char_idx'], doc_id=node.ref_doc_id)

        # >>2 Link node using NEXT relationship

        if node.next_node is not None: # and node.next_node.node_id[-1*len(TABLE_REF_SUFFIX):] != TABLE_REF_SUFFIX:
            cypher  = "MATCH (c:Section {key: $node_id})\n"    # current node should exist
            cypher += "MERGE (p:Section {key: $next_id})\n"    # previous node may not exist
            cypher += "MERGE (p)<-[:NEXT]-(c);"

            session.run(cypher, node_id=node.node_id, next_id=node.next_node.node_id)

        if node.prev_node is not None:  # Because tables are in objects list, so we need to link from the opposite direction
            cypher  = "MATCH (c:Section {key: $node_id})\n"    # current node should exist
            cypher += "MERGE (p:Section {key: $prev_id})\n"    # previous node may not exist
            cypher += "MERGE (p)-[:NEXT]->(c);"

            if node.prev_node.node_id[-1 * len(TABLE_ID_SUFFIX):] == TABLE_ID_SUFFIX:
                prev_id = node.prev_node.node_id + '_ref'
            else:
                prev_id = node.prev_node.node_id

            session.run(cypher, node_id=node.node_id, prev_id=prev_id)

        i = i + 1
    session.close()

i = 0
with driver.session() as session:
    for node in objects:               
        node_json = json.loads(node.json())

        # Object is a Table, then the ????_ref_table object is created as a Section, and the table object is Chunk
        if node.node_id[-1 * len(TABLE_REF_SUFFIX):] == TABLE_REF_SUFFIX:
            if node.next_node is not None:  # here is where actual table object is loaded
                next_node = node.next_node

                obj_metadata = json.loads(str(next_node.json()))

                cypher  = "MERGE (s:Section {key: $node_id})\n"
                cypher += "WITH s MERGE (c:Chunk {key: $table_id})\n"
                cypher += " FOREACH (ignoreMe IN CASE WHEN c.type IS NULL THEN [1] ELSE [] END |\n"
                cypher += "     SET c.hash = $hash, c.definition=$content, c.text=$table_summary, c.type=$type, c.start_idx=$start_idx, c.end_idx=$end_idx )\n"
                cypher += " WITH s, c\n"
                cypher += " MERGE (s) <-[:UNDER_SECTION]- (c)\n"
                cypher += " WITH s MATCH (d:Document {url_hash: $doc_id})\n"
                cypher += " MERGE (d)<-[:HAS_DOCUMENT]-(s);"

                session.run(cypher, node_id=node.node_id, hash=next_node.hash, content=obj_metadata['metadata']['table_df'], type='TABLE'
                                  , start_idx=node_json['start_char_idx'], end_idx=node_json['end_char_idx']
                                  , doc_id=node.ref_doc_id, table_summary=obj_metadata['metadata']['table_summary'], table_id=next_node.node_id)
                
            if node.prev_node is not None:
                cypher  = "MATCH (c:Section {key: $node_id})\n"    # current node should exist
                cypher += "MERGE (p:Section {key: $prev_id})\n"    # previous node may not exist
                cypher += "MERGE (p)-[:NEXT]->(c);"

                if node.prev_node.node_id[-1 * len(TABLE_ID_SUFFIX):] == TABLE_ID_SUFFIX:
                    prev_id = node.prev_node.node_id + '_ref'
                else:
                    prev_id = node.prev_node.node_id
                
                session.run(cypher, node_id=node.node_id, prev_id=prev_id)
                
        i = i + 1
    session.close()

with driver.session() as session:

    cypher  = "MATCH (s:Section) WHERE s.type='TEXT' \n"
    cypher += "WITH s CALL {\n"
    cypher += "WITH s WITH s, split(s.text, '\n') AS para\n"
    cypher += "WITH s, para, range(0, size(para)-1) AS iterator\n"
    cypher += "UNWIND iterator AS i WITH s, trim(para[i]) AS chunk, i WHERE size(chunk) > 0\n"
    cypher += "CREATE (c:Chunk {key: s.key + '_' + i}) SET c.type='TEXT', c.text = chunk, c.seq = i \n"
    cypher += "CREATE (s) <-[:UNDER_SECTION]-(c) } IN TRANSACTIONS OF 500 ROWS ;"
    
    session.run(cypher)
    
    session.close()

driver.close()
    