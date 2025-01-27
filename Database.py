from llama_index.core.node_parser import MarkdownElementNodeParser
from llama_parse import LlamaParse


pdf_file_name = 'D:\RAG_KG_Chatbot\RAG_KG_Chatbot\ugrulebook.pdf'
documents = LlamaParse(result_type="markdown").load_data(pdf_file_name)
node_parser = MarkdownElementNodeParser(llm=llm, num_workers=8)
nodes = node_parser.get_nodes_from_documents(documents)
base_nodes, objects = node_parser.get_nodes_and_objects(nodes)