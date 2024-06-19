import socket
import threading
import json
import os

# Carregar configurações
with open('replica_config.json', 'r') as f:
    config = json.load(f)

HOST = config['host']
PORT = config['port']
DOCUMENTS_DIR = './documents'

def search_documents(keywords):
    print('numero deocumentos -- ',len(os.listdir(DOCUMENTS_DIR)))
    results = {}
    for filename in os.listdir(DOCUMENTS_DIR):
        if filename.endswith('.txt'):
            with open(os.path.join(DOCUMENTS_DIR, filename), 'r') as f:
                content = f.read()
                count = sum(content.count(keyword) for keyword in keywords)
                if count > 0:
                    results[filename] = count
    return results

def handle_root_node(root_socket):
    request = root_socket.recv(1024).decode('utf-8')
    print(f"[Replica {PORT}] Received keywords: {request}")
    keywords = json.loads(request)
    results = search_documents(keywords)
    root_socket.send(json.dumps(results).encode('utf-8'))
    print(f"[Replica {PORT}] Sent results: {results}")
    root_socket.close()

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f'[*] Replica {PORT} listening on {HOST}:{PORT}')

    while True:
        root_socket, addr = server.accept()
        print(f'[*] Accepted connection from {addr}')
        root_handler = threading.Thread(target=handle_root_node, args=(root_socket,))
        root_handler.start()

if __name__ == '__main__':
    main()
