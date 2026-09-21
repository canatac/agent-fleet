#!/usr/bin/env python3
"""
fleet_launcher.py — Lanceur non-interactif d'Hermes avec exclusivité garantie.

Invariants :
- extend() ne recrée JAMAIS un claim (retourne False si absent/expiré/volé)
- Toute publication Redis est conditionnée à la vérification atomique du claim
- Perte du claim → kill immédiat du subprocess + descendants
- Aucune relance automatique sans réconciliation explicite
- Checkpoints/results séparés par attempt_id (jamais écrasés)
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import traceback
import threading
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from protocol.fleet_protocol import (
    PROTOCOL_VERSION, BusinessState, TransportState,
    claim_key, make_result, result_key, task_key,
)

REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
REDIS_PASS = os.environ["REDIS_PASS"]
HERMES_BIN = os.environ.get("HERMES_BIN", "/usr/local/bin/hermes")
LOG_DIR = Path(os.environ.get("LOG_DIR", "/var/log/fleet"))
CLAIM_TTL = int(os.environ.get("CLAIM_TTL", "300"))
HEARTBEAT_INTERVAL = 10  # < CLAIM_TTL/2 pour marge


def log(msg: str, *, agent: str = "", level: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{ts} [{level}] launcher agent={agent} {msg}"
    print(line, flush=True)
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with (LOG_DIR / f"{agent}.log").open("a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def get_redis():
    import redis
    pool = redis.ConnectionPool(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS or None,
        decode_responses=True, socket_connect_timeout=5, socket_timeout=10,
        retry_on_timeout=True,
    )
    return redis.Redis(connection_pool=pool)


class TaskClaim:
    """Claim exclusif avec renouvellement contrôlé."""

    def __init__(self, r, agent: str, task_id: str, ttl: int = CLAIM_TTL):
        self.r = r
        self.agent = agent
        self.task_id = task_id
        self.ttl = ttl
        self.attempt_id = f"attempt_{os.getpid()}_{int(time.time())}"
        self._key = claim_key(task_id)

    def acquire(self) -> bool:
        acquired = self.r.set(self._key, self.attempt_id, nx=True, ex=self.ttl)
        if acquired:
            log(f"CLAIM_ACQUIRED key={self._key} attempt={self.attempt_id}", agent=self.agent)
        return bool(acquired)

    def release(self) -> None:
        """Supprime le claim seulement si on est propriétaire."""
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        try:
            self.r.eval(script, 1, self._key, self.attempt_id)
        except Exception:
            pass

    def extend(self) -> bool:
        """
        Renouvelle UNIQUEMENT si propriétaire.
        Retourne False si absent, expiré ou détenu. Ne recrée jamais.
        """
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        end
        return 0
        """
        try:
            return bool(self.r.eval(script, 1, self._key, self.attempt_id, str(self.ttl)))
        except Exception:
            return False

    @property
    def is_owner(self) -> bool:
        try:
            return self.r.get(self._key) == self.attempt_id
        except Exception:
            return False

    def has_effects(self) -> dict:
        """Détecte les effets persistés (result, msg_done)."""
        effects = {}
        if self.r.exists(f"result:{self.task_id}"):
            effects['result'] = True
        if self.r.exists(f"msg:done:{self.task_id}"):
            effects['msg_done'] = True
        return effects

    def try_publish_result(self, result_data: dict) -> bool:
        """
        Publication atomique : écrit result:* UNIQUEMENT si propriétaire.
        Retourne False si le claim est perdu (résultat rejeté).
        """
        # KEYS[1] = claim_key, KEYS[2] = result_key, KEYS[3] = msg_done_key
        # ARGV[1] = attempt_id (ownership check), ARGV[2] = json result data
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            redis.call("set", KEYS[2], ARGV[2], "ex", "86400")
            redis.call("set", KEYS[3], "1", "ex", "86400")
            return 1
        end
        return 0
        """
        try:
            ok = self.r.eval(script, 3,
                self._key,
                f"result:{self.task_id}",
                f"msg:done:{self.task_id}",
                self.attempt_id,
                json.dumps(result_data)
            )
            return bool(ok)
        except Exception:
            return False

    def publish_checkpoint(self, data: dict) -> None:
        """
        Checkpoint local + Redis par attempt_id (jamais écrasé).
        Utilise attempt_id comme suffixe pour séparer les tentatives.
        """
        # Ajouter l'attempt_id aux données
        data['attempt_id'] = self.attempt_id
        data['checkpoint_ts'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        checkpoint_dir = LOG_DIR / "checkpoints" / self.task_id
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        # Fichier unique par attempt_id (jamais écrasé)
        checkpoint_file = checkpoint_dir / f"{self.attempt_id}.json"
        checkpoint_file.write_text(json.dumps(data, indent=2))
        # Référence Redis (liste des attempts)
        try:
            self.r.rpush(f"checkpoints:{self.task_id}", self.attempt_id)
            self.r.expire(f"checkpoints:{self.task_id}", 86400)
        except Exception:
            pass


def build_hermes_cmd(args: argparse.Namespace) -> list[str]:
    cmd = [HERMES_BIN, "chat", "--query-file", args.query_file,
           "--oneshot", "-Q", "--format", "stream-json"]
    if args.profile:
        cmd += ["--profile", args.profile]
    if args.provider:
        cmd += ["--provider", args.provider]
    if args.model:
        cmd += ["--model", args.model]
    if args.worktree:
        cmd.append("--worktree")
    if args.accept_hooks:
        cmd.append("--accept-hooks")
    if args.max_turns:
        cmd += ["--max-turns", str(args.max_turns)]
    if args.run_budget:
        cmd += ["--run-budget", str(args.run_budget)]
    if args.workspace_dir:
        cmd += ["--in", args.workspace_dir, "--no-restore-cwd"]
    if args.session_id:
        cmd += ["--resume", args.session_id]
    cmd += ["--source", "fleet"]
    return cmd


def kill_process_tree(proc: subprocess.Popen) -> None:
    """Kill le processus et tous ses descendants."""
    try:
        pid = proc.pid
        # Tuer le groupe de processus (descendants inclus)
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            proc.terminate()
        # Attendre courtoisement puis forcer
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
    except Exception:
        pass


def run_task(r, agent: str, task_id: str, cmd: list[str], *,
             workdir: str | None = None, env_file: str | None = None,
             claim: TaskClaim, msg_run_budget: int | None = None) -> dict:
    """Exécution avec exclusivité garantie."""

    start = time.monotonic()
    log(f"RUN_START task_id={task_id} attempt={claim.attempt_id}", agent=agent)

    env = os.environ.copy()
    if env_file and Path(env_file).exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip()

    # Créer un nouveau groupe de processus pour kill arbre
    def preexec_fn():
        os.setsid()  # Nouveau groupe de processus

    lost_claim = threading.Event()
    proc_completed = threading.Event()

    # Heartbeat : vérifie et renouvelle toutes les 10s
    def heartbeat_worker():
        while not proc_completed.is_set():
            proc_completed.wait(HEARTBEAT_INTERVAL)
            if not proc_completed.is_set():
                if claim.is_owner:
                    if not claim.extend():
                        log(f"CLAIM_LOST task_id={task_id} attempt={claim.attempt_id}",
                            agent=agent, level="WARN")
                        lost_claim.set()
                        break
                else:
                    log(f"CLAIM_NOT_OWNER task_id={task_id} attempt={claim.attempt_id}",
                        agent=agent, level="WARN")
                    lost_claim.set()
                    break

    heartbeat_thread = threading.Thread(target=heartbeat_worker, daemon=True)
    heartbeat_thread.start()

    proc = None
    exit_code = -1
    stdout_data: list[str] = []
    stderr_data: list[str] = []
    status = BusinessState.FAILED
    error_msg = ""

    try:
        timeout = (msg_run_budget + 30) if msg_run_budget else 600
        proc = subprocess.Popen(
            cmd, cwd=workdir, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            preexec_fn=preexec_fn,
        )

        # Thread pour surveiller la perte de claim et tuer le subprocess
        def claim_guard():
            lost_claim.wait()  # Attend la perte
            if proc and proc.poll() is None:
                log(f"KILLING task_id={task_id} subprocess after claim loss", agent=agent, level="WARN")
                kill_process_tree(proc)

        guard_thread = threading.Thread(target=claim_guard, daemon=True)
        guard_thread.start()

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
            exit_code = proc.returncode
            stdout_data = stdout.splitlines()
            stderr_data = stderr.splitlines()
        except subprocess.TimeoutExpired:
            kill_process_tree(proc)
            error_msg = f"TIMEOUT after {timeout}s"
            log(f"RUN_TIMEOUT task_id={task_id}", agent=agent, level="ERROR")
        except Exception as e:
            if proc and proc.poll() is None:
                kill_process_tree(proc)
            error_msg = f"LAUNCH_ERROR: {e}"
            log(f"RUN_ERROR task_id={task_id} error={e}", agent=agent, level="ERROR")
    finally:
        proc_completed.set()
        lost_claim.set()  # Déclenche le guard si pas déjà fait

    duration = time.monotonic() - start

    # Si le claim est perdu → NE PAS publier de résultat
    if lost_claim.is_set():
        status = BusinessState.FAILED
        error_msg = f"CLAIM_LOST attempt={claim.attempt_id}"
        result = make_result(
            task_id=task_id, agent=agent, success=False,
            output="", error=error_msg,
            artifacts={"claim_lost": True, "duration_seconds": round(duration, 2)},
            attempt_id=claim.attempt_id, duration_seconds=round(duration, 2),
        )
        # Checkpoint local seulement (pas de publication Redis)
        claim.publish_checkpoint(result)
        log(f"RESULT_REJECTED task_id={task_id} (claim lost)", agent=agent, level="WARN")
        return result

    # Extraction du résultat depuis stream-json
    hermes_output = ""
    if exit_code == 0 and stdout_data:
        for line in reversed(stdout_data):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                if event.get("type") == "result":
                    hermes_output = event.get("text", "")
                    break
            except json.JSONDecodeError:
                continue
        if not hermes_output:
            text_lines = []
            for line in stdout_data:
                try:
                    event = json.loads(line)
                    if event.get("type") == "text":
                        text_lines.append(event.get("text", ""))
                except json.JSONDecodeError:
                    pass
            hermes_output = "".join(text_lines)

    if exit_code == 0 and hermes_output.strip():
        status = BusinessState.COMPLETED
    elif exit_code == 0 and not hermes_output.strip():
        status = BusinessState.FAILED
        error_msg = "RESULT_INVALID: empty output"
    else:
        status = BusinessState.FAILED
        if not error_msg:
            error_msg = f"EXIT_CODE_{exit_code}"

    result = make_result(
        task_id=task_id, agent=agent,
        success=(status == BusinessState.COMPLETED),
        output=hermes_output, error=error_msg,
        artifacts={
            "exit_code": exit_code,
            "stdout_lines": len(stdout_data),
            "stderr_lines": len(stderr_data),
            "duration_seconds": round(duration, 2),
        },
        attempt_id=claim.attempt_id,
        duration_seconds=round(duration, 2),
    )

    # Publication atomique (vérifie is_owner dans Redis)
    if claim.try_publish_result(result):
        log(f"RESULT_PUBLISHED task_id={task_id} attempt={claim.attempt_id} state={status.value}", agent=agent)
    else:
        log(f"RESULT_REJECTED task_id={task_id} (lost during execution)", agent=agent, level="WARN")
        claim.publish_checkpoint(result)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Fleet Hermes Launcher (non-interactif)")
    parser.add_argument("--agent", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--query-file", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--provider", default=os.environ.get("HERMES_PROVIDER", "nous"))
    parser.add_argument("--model", default=os.environ.get("HERMES_MODEL", "meituan/longcat-2.0:free"))
    parser.add_argument("--worktree", action="store_true")
    parser.add_argument("--accept-hooks", action="store_true")
    parser.add_argument("--max-turns", type=int, default=150)
    parser.add_argument("--run-budget", type=int, default=600)
    parser.add_argument("--session-id")
    parser.add_argument("--workspace-dir")
    parser.add_argument("--env-file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    try:
        r = get_redis()
        r.ping()
    except Exception as e:
        print(f"REDIS_CONNECT_FAILED: {e}", file=sys.stderr)
        return 2

    claim = TaskClaim(r, args.agent, args.task_id)
    if not claim.acquire():
        log(f"ABORT task already claimed task_id={args.task_id}", agent=args.agent)
        return 3

    try:
        cmd = build_hermes_cmd(args)
        if args.dry_run:
            print(" ".join(cmd))
            claim.release()
            return 0

        result = run_task(r, args.agent, args.task_id, cmd,
            workdir=args.workspace_dir, env_file=args.env_file,
            claim=claim, msg_run_budget=args.run_budget)

        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["success"] else 1

    finally:
        claim.release()


if __name__ == "__main__":
    args = None
    sys.exit(main())
