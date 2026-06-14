
import grpc, threading, logging, argparse, time
from concurrent import futures
from datetime import datetime, timezone

import scoreboard_pb2 as pb2
import scoreboard_pb2_grpc as pb2_grpc

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [SERVER] %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


class GameState:
    def __init__(self, game_id: str):
        self.game_id    = game_id
        self.score      = 0
        self.version    = 0
        self.updated_by = "system"
        self.timestamp  = datetime.now(timezone.utc).isoformat()
        self.history: list[dict] = []


class ScoreboardServicer(pb2_grpc.ScoreboardServiceServicer):
    def __init__(self):
        self._games: dict[str, GameState] = {}
        self._lock  = threading.Lock()
        self._stats = {"total_reads": 0, "total_writes": 0,
                       "conflicts": 0, "rejections_low": 0}

    def _get_or_create(self, game_id: str) -> GameState:
        if game_id not in self._games:
            self._games[game_id] = GameState(game_id)
            log.info("Novo jogo criado: %s", game_id)
        return self._games[game_id]

    def GetScore(self, request, context):
        with self._lock:
            game = self._get_or_create(request.game_id)
            self._stats["total_reads"] += 1
            log.info("GetScore  game=%-10s score=%-6d version=%d",
                     game.game_id, game.score, game.version)
            return pb2.GetScoreResponse(
                game_id=game.game_id, score=game.score,
                version=game.version, updated_by=game.updated_by,
                timestamp=game.timestamp)

    def UpdateScore(self, request, context):
        with self._lock:
            game = self._get_or_create(request.game_id)
            self._stats["total_writes"] += 1

            if request.base_version != game.version:
                self._stats["conflicts"] += 1
                msg = (f"Conflito de versão: cliente tem v{request.base_version}, "
                       f"servidor está em v{game.version}")
                log.warning("CONFLITO   player=%-10s %s", request.player_id, msg)
                return pb2.UpdateScoreResponse(success=False, message=msg,
                    score=game.score, version=game.version)

            if request.new_score <= game.score:
                self._stats["rejections_low"] += 1
                msg = (f"Valor inválido: novo escore {request.new_score} "
                       f"não é maior que o atual {game.score}")
                log.warning("REJEITADO  player=%-10s %s", request.player_id, msg)
                return pb2.UpdateScoreResponse(success=False, message=msg,
                    score=game.score, version=game.version)

            old_score       = game.score
            game.score      = request.new_score
            game.version   += 1
            game.updated_by = request.player_id
            game.timestamp  = datetime.now(timezone.utc).isoformat()
            game.history.append({"player": request.player_id, "old_score": old_score,
                "new_score": game.score, "version": game.version,
                "timestamp": game.timestamp})

            log.info("ATUALIZADO player=%-10s %d -> %d  (v%d)",
                     request.player_id, old_score, game.score, game.version)
            return pb2.UpdateScoreResponse(success=True, message="OK",
                score=game.score, version=game.version)

    def print_stats(self):
        log.info("=== ESTATÍSTICAS ===")
        for k, v in self._stats.items():
            log.info("  %-20s %d", k, v)
        for gid, g in self._games.items():
            log.info("  Jogo %-10s  escore=%d  versão=%d  updates=%d",
                     gid, g.score, g.version, len(g.history))


def serve(host: str, port: int):
    servicer = ScoreboardServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32),
        options=[("grpc.max_concurrent_rpcs", 100)])
    pb2_grpc.add_ScoreboardServiceServicer_to_server(servicer, server)
    addr = f"{host}:{port}"
    server.add_insecure_port(addr)
    server.start()
    log.info("Servidor iniciado em %s", addr)
    try:
        while True:
            time.sleep(30)
            servicer.print_stats()
    except KeyboardInterrupt:
        servicer.print_stats()
        server.stop(grace=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5678)
    args = parser.parse_args()
    serve(args.host, args.port)
