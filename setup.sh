#!/bin/bash
# Roda uma vez em cada máquina (servidor e todos os peers)

pip install grpcio grpcio-tools

python3 -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. scoreboard.proto
python3 -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. node.proto

echo "Setup concluído!"
