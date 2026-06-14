import grpc, threading, logging, argparse, time, uuid, random
from concurrent import futures

import node_pb2 as pb2
import node_pb2_grpc as pb2_grpc
import scoreboard_pb2 as pb2_sb
import scoreboard_pb2_grpc as pb2_grpc_sb

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s", datefmt="%H:%M:%S")

HEARTBEAT_INTERVAL = 2.0
RPC_TIMEOUT        = 2.0


class NodeServicer(pb2_grpc.NodeServiceServicer):
    """
    Implementa o NodeService: exclusão mútua (quando coordenador)
    + algoritmo Bully de eleição.
    """

    def __init__(self, my_id: int, peers: dict[int, str]):
        self.my_id = my_id
        self.peers = peers 
        self.log   = logging.getLogger(f"N{my_id}")

        self.lock = threading.RLock()
        self.coordinator_id       = max(peers.keys())
        self.election_in_progress = False

        self.owner       = None
        self.owner_token = None
        self.queue       = []
        self._lease_acquired_at = 0.0
        threading.Thread(target=self._lease_watchdog, daemon=True).start()

    def _stub(self, peer_id: int) -> pb2_grpc.NodeServiceStub:
        return pb2_grpc.NodeServiceStub(grpc.insecure_channel(self.peers[peer_id]))

    # Exclusão Mútua (coordenador)

    def Acquire(self, request, context):
        with self.lock:
            if self.my_id != self.coordinator_id:
                return pb2.AcquireResponse(granted=False, token="",
                                            coordinator_id=self.coordinator_id)

            if self.owner is None:
                token = str(uuid.uuid4())[:8]
                self.owner, self.owner_token = request.client_id, token
                self._lease_acquired_at = time.time()
                self.log.info("GRANT imediato -> %-12s (token=%s)", request.client_id, token)
                return pb2.AcquireResponse(granted=True, token=token,
                                            coordinator_id=self.my_id)

            token = str(uuid.uuid4())[:8]
            event = threading.Event()
            self.queue.append((request.client_id, event, token))
            pos = len(self.queue)
            self.log.info("FILA  posição=%-2d cliente=%-12s (dono atual=%s)",
                           pos, request.client_id, self.owner)

        event.wait(timeout=120)
        if not event.is_set():
            with self.lock:
                self.queue = [q for q in self.queue if q[0] != request.client_id]
            return pb2.AcquireResponse(granted=False, token="",
                                        coordinator_id=self.coordinator_id)

        self.log.info("GRANT após fila -> %-12s (token=%s)", request.client_id, token)
        return pb2.AcquireResponse(granted=True, token=token, coordinator_id=self.my_id)

    def Release(self, request, context):
        with self.lock:
            if self.my_id != self.coordinator_id:
                return pb2.ReleaseResponse(success=False, message="not coordinator")
            if self.owner != request.client_id or self.owner_token != request.token:
                return pb2.ReleaseResponse(success=False, message="not owner")

            self.log.info("RELEASE <- %-12s", request.client_id)
            self.owner = self.owner_token = None

            if self.queue:
                nc, ne, nt = self.queue.pop(0)
                self.owner, self.owner_token = nc, nt
                self._lease_acquired_at = time.time()
                self.log.info("GRANT próximo -> %-12s (token=%s) fila=%d", nc, nt, len(self.queue))
                ne.set()

        return pb2.ReleaseResponse(success=True, message="OK")

    LEASE_TIMEOUT = 8.0

    def _lease_watchdog(self):
        while True:
            time.sleep(2.0)
            with self.lock:
                if (self.owner is not None
                        and self.my_id == self.coordinator_id
                        and (time.time() - self._lease_acquired_at) > self.LEASE_TIMEOUT):
                    self.log.warning(
                        "LEASE EXPIRADO: liberando owner=%s após %.1fs sem Release",
                        self.owner, time.time() - self._lease_acquired_at)
                    self.owner = self.owner_token = None
                    if self.queue:
                        nc, ne, nt = self.queue.pop(0)
                        self.owner, self.owner_token = nc, nt
                        self._lease_acquired_at = time.time()
                        self.log.info("GRANT (watchdog) -> %-12s (token=%s) fila=%d",
                                      nc, nt, len(self.queue))
                        ne.set()

    def Status(self, request, context):
        with self.lock:
            return pb2.StatusResponse(
                my_id=self.my_id, coordinator_id=self.coordinator_id,
                is_coordinator=(self.coordinator_id == self.my_id))

    # Eleição: Algoritmo Bully

    def Ping(self, request, context):
        with self.lock:
            return pb2.PingResponse(alive=True, coordinator_id=self.coordinator_id)

    def Election(self, request, context):
        self.log.info("Recebido ELECTION de nó %d", request.from_id)
        threading.Thread(target=self.start_election, daemon=True).start()
        return pb2.ElectionResponse(alive=True)

    def Victory(self, request, context):
        with self.lock:
            old = self.coordinator_id
            self.coordinator_id       = request.coordinator_id
            self.election_in_progress = False
        if old != request.coordinator_id:
            self.log.info(">>> Novo coordenador anunciado: nó %d <<<", request.coordinator_id)
        return pb2.VictoryResponse(ack=True)

    def start_election(self):
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True

        self.log.info(" INICIANDO ELEIÇÃO (Bully) ")
        higher_alive = False
        for pid in sorted(self.peers):
            if pid > self.my_id:
                try:
                    resp = self._stub(pid).Election(
                        pb2.ElectionRequest(from_id=self.my_id), timeout=RPC_TIMEOUT)
                    if resp.alive:
                        higher_alive = True
                        self.log.info("  nó %d respondeu (está vivo)", pid)
                except grpc.RpcError:
                    self.log.info("  nó %d não respondeu (fora do ar)", pid)

        if higher_alive:
            with self.lock:
                self.election_in_progress = False
            self.log.info("Há nó de maior prioridade vivo — aguardando anúncio de vitória")
            return

        with self.lock:
            self.coordinator_id       = self.my_id
            self.election_in_progress = False
        self.log.info(">>> TORNEI-ME O NOVO COORDENADOR (nó %d) <<<", self.my_id)

        for pid in sorted(self.peers):
            if pid != self.my_id:
                try:
                    self._stub(pid).Victory(
                        pb2.VictoryRequest(coordinator_id=self.my_id), timeout=RPC_TIMEOUT)
                except grpc.RpcError:
                    pass

    def heartbeat_loop(self):
        """Roda em background: nós não-coordenadores monitoram o coordenador."""
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                coord = self.coordinator_id
            if coord == self.my_id:
                continue 
            try:
                self._stub(coord).Ping(pb2.PingRequest(from_id=self.my_id), timeout=RPC_TIMEOUT)
            except grpc.RpcError:
                self.log.warning("!!! Coordenador (nó %d) NÃO RESPONDE — possível falha !!!", coord)
                self.start_election()


# Jogador

def player_loop(node: NodeServicer, player_id: str, sb_stub, game_id: str,
                rounds: int, min_pts: int, max_pts: int, think_time: float):
    log = logging.getLogger(player_id)
    for r in range(1, rounds + 1):
        pts = random.randint(min_pts, max_pts)

        token = None
        while token is None:
            with node.lock:
                coord_id = node.coordinator_id
            try:
                stub = node._stub(coord_id)
                resp = stub.Acquire(pb2.AcquireRequest(client_id=player_id), timeout=15)
                if resp.granted:
                    token = resp.token
                else:
                    time.sleep(0.3)
            except grpc.RpcError:
                log.warning("Coordenador (nó %d) inacessível — disparando eleição...", coord_id)
                node.start_election()
                time.sleep(1.0)

        # SEÇÃO CRÍTICA: GetScore -> calcula -> UpdateScore
        try:
            current   = sb_stub.GetScore(pb2_sb.GetScoreRequest(game_id=game_id))
            new_score = current.score + pts
            time.sleep(random.uniform(0.05, 0.15))
            resp = sb_stub.UpdateScore(pb2_sb.UpdateScoreRequest(
                game_id=game_id, new_score=new_score,
                base_version=current.version, player_id=player_id))
            log.info("Rodada %d/%d  +%d pts  score=%d  version=%d  (coord=nó%d)",
                     r, rounds, pts, resp.score, resp.version, coord_id)
        finally:
            try:
                stub.Release(pb2.ReleaseRequest(client_id=player_id, token=token))
            except grpc.RpcError:
                log.warning("Falha ao liberar lock (coordenador caiu logo após conceder)")

        if r < rounds:
            time.sleep(random.uniform(0, think_time))

    log.info(" Sessão concluída: %d rodadas ", rounds)


# Main

def parse_peers(s: str) -> dict[int, str]:
    peers = {}
    for part in s.split(","):
        pid, addr = part.split("=")
        peers[int(pid)] = addr
    return peers


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id",     type=int, required=True, help="ID numérico deste nó (único)")
    parser.add_argument("--peers",  required=True,
                         help='Mapa de todos os nós, ex: "1=ip1:6000,2=ip2:6000,3=ip3:6000"')
    parser.add_argument("--port",   type=int, default=6000)
    parser.add_argument("--scoreboard", default="localhost:5678")
    parser.add_argument("--players",  type=int, default=2)
    parser.add_argument("--rounds",   type=int, default=30)
    parser.add_argument("--min-pts",  type=int, default=10)
    parser.add_argument("--max-pts",  type=int, default=100)
    parser.add_argument("--think",    type=float, default=1.0)
    parser.add_argument("--game",     default="game1")
    parser.add_argument("--startup-delay", type=float, default=5.0,
                         help="tempo de espera antes da 1ª eleição (para todos os nós subirem)")
    args = parser.parse_args()

    peers = parse_peers(args.peers)
    if args.id not in peers:
        raise SystemExit(f"--id {args.id} precisa estar presente em --peers")

    servicer = NodeServicer(args.id, peers)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb2_grpc.add_NodeServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"0.0.0.0:{args.port}")
    server.start()

    servicer.log.info("Nó %d no ar em 0.0.0.0:%d  |  peers=%s", args.id, args.port, peers)
    servicer.log.info("Coordenador inicial assumido: nó %d", servicer.coordinator_id)

    threading.Thread(target=servicer.heartbeat_loop, daemon=True).start()

    servicer.log.info("Aguardando %.0fs para os demais nós subirem...", args.startup_delay)
    time.sleep(args.startup_delay)
    servicer.start_election()

    sb_stub = pb2_grpc_sb.ScoreboardServiceStub(grpc.insecure_channel(args.scoreboard))

    threads = []
    for i in range(1, args.players + 1):
        pid = f"N{args.id}-P{i:02d}"
        t = threading.Thread(target=player_loop, args=(
            servicer, pid, sb_stub, args.game, args.rounds,
            args.min_pts, args.max_pts, args.think), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    servicer.log.info("Todos os jogadores terminaram. Nó continua no ar (eleição/coordenação).")
    while True:
        time.sleep(60)
