# Eleição de Coordenador - ASR 14 (Algoritmo Bully)

---

Na ASR 13, um peer fixo (peer1) atuava como coordenador de exclusão mútua. Se esse peer falhasse, todo o sistema parava - ninguém mais conseguiria adquirir o lock.

Na ASR 14, qualquer peer pode se tornar coordenador. Se o coordenador atual falhar, os demais nós detectam a falha e elegem automaticamente um novo coordenador - o sistema continua funcionando.

---

## Algoritmo: Bully

Cada nó tem um **ID numérico fixo** (1, 2, 3...). Maior ID = maior prioridade.

- **Heartbeat:** todo nó que NÃO é o coordenador faz `Ping` no coordenador a cada 2s.
- **Falha detectada:** se o `Ping` falhar, o nó inicia uma **ELEIÇÃO** - manda `Election` para todos os nós com ID **maior** que o seu.
  - Se **nenhum** responder → este nó se torna o coordenador e avisa todos via `Victory`.
  - Se **algum** responder → esse nó assume a eleição (ele também roda sua própria eleição ao receber `Election`).
- **Resultado:** o nó vivo com **maior ID** sempre vence.

---

## Estrutura de arquivos

```
scoreboard.proto   # Contrato do scoreboard
server.py          # Servidor de scoreboard
node.proto         # Contrato do nó: mutex + eleição (Bully)
node.py            # Programa principal - roda em cada peer
setup.sh           # Instala dependências e gera os arquivos gRPC
```
---

## Setup (rodar uma vez em cada máquina)

```bash
bash setup.sh
```

---

## Máquinas necessárias

```
instância-servidor  -> roda server.py        (porta 5678)
instância-peer1     -> roda node.py --id 1   (porta 5679)
instância-peer2     -> roda node.py --id 2   (porta 5679)
instância-peer3     -> roda node.py --id 3   (porta 5679)
```

---

## Como rodar

**1. instância-servidor:**
```bash
python3 server.py --host 0.0.0.0 --port 5678
```

**2. Em CADA peer**, definir a mesma string `--peers` (IDs -> IP:porta de cada peer):

```bash
PEERS="1=IP_PEER1:5679,2=IP_PEER2:5679,3=IP_PEER3:5679"
```

**peer1:**
```bash
python3 node.py --id 1 --port 5679 --peers "$PEERS" \
    --scoreboard IP_SERVIDOR:5678 \
    --players 2 --rounds 60 --think 1 --startup-delay 10
```

**peer2:**
```bash
python3 node.py --id 2 --port 5679 --peers "$PEERS" \
    --scoreboard IP_SERVIDOR:5678 \
    --players 2 --rounds 60 --think 1 --startup-delay 10
```

**peer3:**
```bash
python3 node.py --id 3 --port 5679 --peers "$PEERS" \
    --scoreboard IP_SERVIDOR:5678 \
    --players 2 --rounds 60 --think 1 --startup-delay 10
```

> Inicie os 3 peers o mais próximo possível um do outro. O `--startup-delay 10`
> dá 10 segundos para você abrir os 3 terminais antes da primeira eleição rodar.
>
> Inicialmente, o nó de maior ID (peer3) será o coordenador.

---

