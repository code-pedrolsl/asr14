# Eleição de Coordenador — ASR 14 (Algoritmo Bully)

**Disciplina:** INF0344A-BCC — Sistemas Distribuídos

---

## O problema (ASR 13 → ASR 14)

Na ASR 13, um peer fixo (peer1) atuava como coordenador de exclusão mútua. Se esse peer falhasse, **todo o sistema parava** — ninguém mais conseguiria adquirir o lock.

Na ASR 14, **qualquer peer pode se tornar coordenador**. Se o coordenador atual falhar, os demais nós detectam a falha e elegem automaticamente um novo coordenador — o sistema continua funcionando.

---

## Algoritmo: Bully

Cada nó tem um **ID numérico fixo** (1, 2, 3...). Maior ID = maior prioridade.

- **Heartbeat:** todo nó que NÃO é o coordenador faz `Ping` no coordenador a cada 2s.
- **Falha detectada:** se o `Ping` falhar, o nó inicia uma **ELEIÇÃO** — manda `Election` para todos os nós com ID **maior** que o seu.
  - Se **nenhum** responder → este nó se torna o coordenador e avisa todos via `Victory`.
  - Se **algum** responder → esse nó assume a eleição (ele também roda sua própria eleição ao receber `Election`).
- **Resultado:** o nó vivo com **maior ID** sempre vence.

```
3 nós (IDs 1, 2, 3) — nó 3 é o coordenador inicial

   nó1          nó2          nó3 (coordenador)
    |            |             |
    |  ping --------------------> X   (nó3 caiu)
    |            |  ping -----> X
    |            |
    |  ELECTION --> nó2 (vivo)     nó1 não sabe se é o maior, espera
    |            |  ELECTION --> nó3 (morto, sem resposta)
    |            |  TORNEI-ME COORDENADOR (nó2)
    | <------ VICTORY(2) -------|
    |            |
  coord=2      coord=2
```

---

## Estrutura de arquivos

```
scoreboard.proto   # Contrato do scoreboard (ASR 12, sem mudanças)
server.py          # Servidor de scoreboard (ASR 12, sem mudanças)
node.proto         # Contrato do nó: mutex + eleição (Bully)
node.py            # Programa principal — roda em cada peer
setup.sh           # Instala dependências e gera os arquivos gRPC
```

`node.py` substitui o `coordinator.py` + `client_mutex.py` da ASR 13. Cada peer roda o **mesmo programa**, diferenciado apenas pelo `--id`.

---

## Setup (rodar uma vez em cada máquina)

```bash
bash setup.sh
```

---

## Máquinas necessárias

```
instância-servidor  -> roda server.py        (porta 5678)
instância-peer1     -> roda node.py --id 1   (porta 6000)
instância-peer2     -> roda node.py --id 2   (porta 6000)
instância-peer3     -> roda node.py --id 3   (porta 6000)
```

Liberar no Security Group:
- porta **5678** na instância-servidor
- porta **6000** em TODAS as instâncias peer (election/heartbeat usa essa porta entre elas)

---

## Como rodar

**1. instância-servidor:**
```bash
python3 server.py --host 0.0.0.0 --port 5678
```

**2. Em CADA peer**, definir a mesma string `--peers` (IDs -> IP:porta de cada peer):

```bash
PEERS="1=IP_PEER1:6000,2=IP_PEER2:6000,3=IP_PEER3:6000"
```

**peer1:**
```bash
python3 node.py --id 1 --port 6000 --peers "$PEERS" \
    --scoreboard IP_SERVIDOR:5678 \
    --players 2 --rounds 60 --think 1 --startup-delay 10
```

**peer2:**
```bash
python3 node.py --id 2 --port 6000 --peers "$PEERS" \
    --scoreboard IP_SERVIDOR:5678 \
    --players 2 --rounds 60 --think 1 --startup-delay 10
```

**peer3:**
```bash
python3 node.py --id 3 --port 6000 --peers "$PEERS" \
    --scoreboard IP_SERVIDOR:5678 \
    --players 2 --rounds 60 --think 1 --startup-delay 10
```

> Inicie os 3 peers o mais próximo possível um do outro. O `--startup-delay 10`
> dá 10 segundos para você abrir os 3 terminais antes da primeira eleição rodar.
>
> Inicialmente, **o nó de maior ID (peer3) será o coordenador**.

---

## Demonstração (para o vídeo)

1. **Mostre os 3 logs em paralelo.** Após ~10s, todos convergem:
   ```
   Coordenador inicial assumido: nó 3
   ...
   TORNEI-ME O NOVO COORDENADOR (nó 3)
   ```
   Os jogadores começam a rodar, todos com `(coord=nó3)`.

2. **Simule a falha:** no terminal do peer3, aperte `Ctrl+C` (ou `kill -9 <pid>`).

3. **Observe peer1 e peer2:**
   - Dentro de ~2-4s (próximo heartbeat), aparece:
     ```
     !!! Coordenador (nó 3) NÃO RESPONDE -- possível falha !!!
     === INICIANDO ELEIÇÃO (Bully) ===
       nó 3 não respondeu (fora do ar)
     ```
   - peer2 (próximo maior ID vivo) imprime:
     ```
     >>> TORNEI-ME O NOVO COORDENADOR (nó 2) <<<
     ```
   - peer1 imprime:
     ```
     >>> Novo coordenador anunciado: nó 2 <<<
     ```

4. **Mostre que os jogadores continuam** rodando normalmente, agora com `(coord=nó2)` — sem nenhuma rodada perdida ou travada.

5. **Mostre o servidor de scoreboard:** a versão (`version=`) continua incrementando de forma contínua, sem conflitos — confirmando que a exclusão mútua se manteve mesmo após a troca de coordenador.

---

## Resultado do teste local

**Configuração:** 3 nós, 2 jogadores cada, peer3 (ID 3) inicial coordenador, morto em ~t+7s.

| Evento | Resultado |
|---|---|
| Eleição inicial | nó 3 (maior ID) eleito coordenador |
| Jogadores rodando | coord=nó3, versão incrementando normalmente |
| peer3 morto (kill -9) | -- |
| Detecção da falha | heartbeat de nó1 e nó2 falha em ~2-5s |
| Nova eleição | nó2 não recebe resposta de nó3 -> torna-se coordenador |
| nó1 atualizado | recebe Victory(2) -> coord=nó2 |
| Jogadores após falha | continuam normalmente com coord=nó2, sem interrupção |
| Conflitos no scoreboard | 0 (mutex preservado durante e após a troca) |
| Versão final do scoreboard | cresceu monotonicamente (147+), sem perdas |

A troca de coordenador foi **transparente para os jogadores** — eles apenas redirecionaram suas chamadas de `Acquire`/`Release` para o novo coordenador (nó2), sem perder nenhuma rodada.
