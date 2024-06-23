import socket
import threading
import json
import os

with open('replica_config.json', 'r') as f:
    config = json.load(f)

HOST = config['host']
PORT = config['port']
DOCUMENTS_DIR = './documents'

def count_words_in_files(words):
    results = {}
    for filename in os.listdir(DOCUMENTS_DIR):
        if filename.endswith('.txt'):
            with open(os.path.join(DOCUMENTS_DIR, filename), 'r') as f:
                content = f.read()
                doc_results = {}
                for word in words:
                    count = content.count(word)
                    if count > 0:
                        doc_results[word] = count
                if doc_results:
                    results[filename] = doc_results
    return results

def handle_root_node_conection(root_socket):
    request = root_socket.recv(1024).decode('utf-8')
    print(f"[Replica {PORT}] Palavras recebidas: {request}")
    keywords = json.loads(request)
    results = count_words_in_files(keywords)
    root_socket.send(json.dumps(results).encode('utf-8'))
    print(f"[Replica {PORT}] Resultados Enviados: {results}")
    root_socket.close()

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f'[*] Replica {PORT} escutando em {HOST}:{PORT}')

    while True:
        root_socket, addr = server.accept()
        print(f'[*] Conexão aceita em {addr}')
        root_handler = threading.Thread(target=handle_root_node_conection, args=(root_socket,))
        root_handler.start()

if __name__ == '__main__':
    main()
