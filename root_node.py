import socket
import threading
import json

# Carregar configurações
with open('root_config.json', 'r') as f:
    config = json.load(f)

HOST = config['host']
PORT = config['port']
REPLICAS = [(replica['host'], replica['port']) for replica in config['replicas']]

def handle_client(client_socket):
    request = client_socket.recv(1024).decode('utf-8')
    print(f"[Root Node] Received query: {request}")
    keywords = request.split()

    results = []

    def query_replica(replica, keywords):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(replica)
        print(f"[Root Node] Sending keywords {keywords} to replica {replica}")
        s.send(json.dumps(keywords).encode('utf-8'))
        response = s.recv(4096).decode('utf-8')
        print(f"[Root Node] Received response from replica {replica}")
        results.append(json.loads(response))
        s.close()

    threads = []
    for i, replica in enumerate(REPLICAS):
        thread = threading.Thread(target=query_replica, args=(replica, keywords))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Combinar resultados
    combined_results = {}
    for result in results:
        for doc, counts in result.items():
            if doc not in combined_results:
                combined_results[doc] = {}
            for word, count in counts.items():
                if word in combined_results[doc]:
                    combined_results[doc][word] += count
                else:
                    combined_results[doc][word] = count

    client_socket.send(json.dumps(combined_results).encode('utf-8'))
    print(f"[Root Node] Sent combined results to client: {combined_results}")
    client_socket.close()

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)
    print(f'[*] Root Node listening on {HOST}:{PORT}')

    while True:
        client_socket, addr = server.accept()
        print(f'[*] Accepted connection from {addr}')
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

if __name__ == '__main__':
    main()
