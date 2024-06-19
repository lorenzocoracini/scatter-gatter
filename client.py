import socket
import json
import time
import random

# Carregar configurações
with open('client_config.json', 'r') as f:
    config = json.load(f)

HOST = config['host']
PORT = config['port']
REQUESTS = config['requests']

def main():
    for query in REQUESTS:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((HOST, PORT))

        print(f"[Client] Sending query: {query}")
        client.send(query.encode('utf-8'))

        response = client.recv(4096).decode('utf-8')
        results = json.loads(response)
        print(f"[Client] Received results: {results}")

        client.close()
        time.sleep(random.uniform(1, 2))

if __name__ == '__main__':
    main()
