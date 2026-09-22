#!/usr/bin/env python3
"""
agent_inbox_consumer.py — Consommateur générique d'inbox pour un agent Hermes.

Prend le nom de l'agent en argument.
Parcours :
1. BRPOP inbox:<agent>
2. Validation + claim anti-doublon
3. Écriture registre (RESULT_RECORDED)
4. Lancement launcher pour l'agent avec contexte
5. Traitement événements post-exécution
6. ACK final

Usage :
    python3 agent_inbox_consumer.py <agent_id>
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REDIS_HOST = os.environ.get("REDIS_HOST", "172.16.12.2")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASS = os.environ.get("REDIS_PASS", "")
LOG_DIR = Path(os.environ.get("LOG_DIR", "/var/log/fleet"))
REGISTRY_DIR = Path(os.environ.get("REGISTRY_DIR", "/opt/fleet/registry"))
QUERY_DIR = Path(os.environ.get("QUERY_DIR", "/tmp/fleet/queries"))
LAUNCHER_BIN = os.environ.get("LAUNCHER_BIN", "/opt/fleet/launcher/fleet_launcher.py")
BRPOP_TIMEOUT = 5

# Import du protocole
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from protocol.fleet_protocol import make_message


def log(msg: str, agent: str, level: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"{ts} [{level}] inbox_consumer({agent}) {msg}"
    print(line, flush=True)
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with (LOG_DIR / f"inbox_{agent}.log").open("a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def get_redis():
    import redis
    pool = redis.ConnectionPool(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS or None,
        decode_responses=True, socket_connect_timeout=10,
        socket_timeout=BRPOP_TIMEOUT + 5, retry_on_timeout=True,
    )
    return redis.Redis(connection_pool=pool)


def consume_message(r, agent: str, payload_str: str) -> str:
    try:
        msg = json.loads(payload_str)
    except json.JSONDecodeError as e:
        log(f"JSON_ERROR: {e}", agent, "ERROR")
        return "JSON_ERROR"

    msg_id = msg.get("id", "?")
    task_id = msg.get("task_id", "?")
    from_ = msg.get("from", "?")
    msg_type = msg.get("type", "?")

    log(f"CONSUME task_id={task_id} from={from_} type={msg_type} msg_id={msg_id}", agent)

    # Claim anti-doublon par msg_id
    claim_key = f"claim:inbox:{agent}:{msg_id}"
    claim_id = f"{agent}_{os.getpid()}_{int(time.time())}"
    if not r.set(claim_key, claim_id, nx=True, ex=3600):
        log(f"DEDUP msg_id={msg_id} already consumed", agent)
        return "DEDUP"

    # Écriture registre
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    registry_path = REGISTRY_DIR / f"{agent}_{task_id}_{msg_id}.json"
    registry_entry = {
        "task_id": task_id,
        "msg_id": msg_id,
        "from": from_,
        "type": msg_type,
        "consumed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "message": msg,
        "ack_type": "RESULT_RECORDED",
    }
    registry_path.write_text(json.dumps(registry_entry, indent=2))
    log(f"REGISTRY_WRITTEN path={registry_path}", agent)

    # Publication événement
    event = {
        "type": "RESULT_RECORDED",
        "task_id": task_id,
        "msg_id": msg_id,
        "from": from_,
        "consumed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "registry_key": str(registry_path),
    }
    r.rpush(f"events:{agent}", json.dumps(event))
    r.expire(f"events:{agent}", 604800)
    log(f"EVENT_PUBLISHED type=RESULT_RECORDED", agent)

    # Construction du prompt pour l'agent
    query = build_agent_query(agent, msg)
    query_path = QUERY_DIR / f"{agent}_{task_id}_{msg_id}.txt"
    QUERY_DIR.mkdir(parents=True, exist_ok=True)
    query_path.write_text(query)

    # Lancement launcher
    cmd = [
        sys.executable, LAUNCHER_BIN,
        "--agent", agent,
        "--task-id", f"process_{task_id}",
        "--query-file", str(query_path),
        "--profile", agent,
        "--provider", "nous",
        "--model", "meituan/longcat-2.0:free",
        "--max-turns", "50",
        "--run-budget", "600",
        "--accept-hooks",
        "--workspace-dir", "/root/misfits-web",
        "--env-file", os.path.expanduser("~/.hermes/.env"),
    ]

    env = os.environ.copy()
    env["HERMES_HOME"] = os.path.expanduser(f"~/.hermes/profiles/{agent}")
    if REDIS_PASS:
        env["REDIS_PASS"] = REDIS_PASS

    log(f"LAUNCH {agent} cmd={' '.join(cmd[:8])}...", agent)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=660, env=env)
        log(f"LAUNCH_DONE exit={proc.returncode}", agent)

        if proc.returncode == 0 and proc.stdout.strip():
            for line in reversed(proc.stdout.strip().split("\n")):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        result = json.loads(line)
                        decision = {
                            "type": f"{agent.upper()}_DECISION",
                            "task_id": task_id,
                            "msg_id": msg_id,
                            "decided_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "success": result.get("success"),
                            "output": result.get("output", ""),
                        }
                        r.rpush(f"events:{agent}", json.dumps(decision))
                        log(f"EVENT_PUBLISHED type={agent.upper()}_DECISION success={result.get('success')}", agent)
                        break
                    except json.JSONDecodeError:
                        continue
            else:
                log("OUTPUT_NO_JSON", agent, "WARN")
        else:
            log(f"LAUNCH_FAILED exit={proc.returncode} stderr={proc.stderr.strip()[-200:]}", agent, "ERROR")

    except subprocess.TimeoutExpired:
        log("LAUNCH_TIMEOUT", agent, "ERROR")
    except Exception as e:
        log(f"LAUNCH_ERROR {e}", agent, "ERROR")

    # ACK final
    r.set(f"msg:ack:{msg_id}", "1", ex=86400)
    log(f"ACK msg:ack:{msg_id}", agent)

    return "CONSUMED_AND_PROCESSED"


def build_agent_query(agent: str, msg: dict) -> str:
    task_id = msg.get("task_id", "?")
    from_ = msg.get("from", "?")
    msg_type = msg.get("type", "?")
    body = msg.get("body", "")

    if agent == "product-owner":
        return f"""## Revue PO — Tâche {task_id}

### Demande reçue
- **De** : {from_}
- **Type** : {msg_type}
- **Tâche** : {task_id}

### Message
{body}

### Actions requises
1. Examiner la livraison (issue/PR mentionnée)
2. Vérifier les critères d'acceptation
3. Prendre une décision : APPROVED / NEEDS_ITERATION / REJECTED
4. Si APPROVED : fusionner la PR (gh pr merge)
5. Enregistrer la décision dans events:product-owner

### Contraintes
- Aucun cargo build/check/test local (CI uniquement)
- Revue indépendante obligatoire
- Respecter la chaîne conductor-ops → PO → Scrum Master → développeur
"""
    else:
        return f"""## Traitement {agent} — Tâche {task_id}

### Message reçu
- **De** : {from_}
- **Type** : {msg_type}
- **Tâche** : {task_id}

### Contenu
{body}

### Actions requises
1. Traiter le message selon votre rôle
2. Enregistrer la décision dans events:{agent}
3. Publier les actions nécessaires (attribution, revue, etc.)
"""


def process_agent_events(r, agent: str) -> None:
    """Traite les événements post-exécution de l'agent.

    Idempotence (fix boucle de republication 2026-09-21) :
    chaque transition événement → cible est publiée exactement une fois,
    identifiable par un marqueur durable (SADD sans expiration) et non plus
    par un claim NX EX temporaire. Parcours paginé de TOUS les événements
    (l'ancienne fenêtre lrange(-10, -1) pouvait ignorer des événements non
    traités).
    """
    from protocol.event_transitions import event_source_id, publish_transition_once

    events_key = f"events:{agent}"
    total = r.llen(events_key)
    page = 50
    for start in range(0, total, page):
        # Reprendre LLEN à chaque page : la liste peut croître pendant le cycle.
        batch = r.lrange(events_key, start, start + page - 1)
        for evt_str in batch:
            try:
                evt = json.loads(evt_str)
            except json.JSONDecodeError:
                continue

            evt_type = evt.get("type")
            task_id = evt.get("task_id")

            # Événements de type TASK_ASSIGNED (Scrum Master → dev)
            if evt_type == "TASK_ASSIGNED":
                assigned_to = evt.get("assigned_to")
                if not assigned_to:
                    continue

                def build_assign(tid, _assigned=assigned_to, _task=task_id, _agent=agent):
                    msg = make_message(
                        type="TICKET_ASSIGN",
                        to=_assigned,
                        from_=_agent,
                        body=f"Tâche assignée par {_agent} : {_task}. Veuillez exécuter.",
                        task_id=_task,
                        extra={"reply_to": _agent, "workspace_dir": "/root/misfits-web",
                               "transition_id": tid},
                    )
                    return json.dumps(msg)

                try:
                    tid, outcome = publish_transition_once(
                        r, agent, evt,
                        target=f"assign:{assigned_to}",
                        target_list_key=f"queue:{assigned_to}",
                        build_payload=build_assign,
                    )
                except ValueError as e:
                    # Événement invalide : journalisé, JAMAIS publié ni marqué.
                    log(f"EVENT_INVALID type=TASK_ASSIGNED task_id={task_id} error={e}", agent, "WARN")
                    continue

            # Événements de type REVIEW_REQUEST (Scrum Master → PO)
            elif evt_type == "SCRUM_REVIEW_COMPLETED":

                def build_review(tid, _task=task_id, _agent=agent):
                    msg = make_message(
                        type="REVIEW_REQUEST",
                        to="product-owner",
                        from_=_agent,
                        body=f"Demande de revue PO pour tâche {_task}. Veuillez examiner la livraison.",
                        task_id=_task,
                        extra={"reply_to": _agent, "transition_id": tid},
                    )
                    return json.dumps(msg)

                try:
                    tid, outcome = publish_transition_once(
                        r, agent, evt,
                        target="po",
                        target_list_key="inbox:product-owner",
                        build_payload=build_review,
                    )
                except ValueError as e:
                    # Événement invalide : journalisé, JAMAIS publié ni marqué.
                    log(f"EVENT_INVALID type=SCRUM_REVIEW_COMPLETED task_id={task_id} error={e}", agent, "WARN")
                    continue
                if outcome == "published":
                    log(f"EVENT_EXECUTED type=REVIEW_REQUEST target=product-owner task_id={task_id} transition={tid}", agent)


def main():
    if len(sys.argv) < 2:
        print("Usage: agent_inbox_consumer.py <agent_id>")
        sys.exit(1)

    agent = sys.argv[1]
    log("INBOX_CONSUMER_START", agent)
    r = get_redis()
    r.ping()

    # Traiter les événements en attente au démarrage
    log("PROCESSING_PENDING_EVENTS", agent)
    process_agent_events(r, agent)

    while True:
        try:
            result = r.brpop(f"inbox:{agent}", timeout=BRPOP_TIMEOUT)
            if result:
                _, payload_str = result
                consume_message(r, agent, payload_str)
            # Toujours traiter les événements en attente (même sans nouveau message)
            process_agent_events(r, agent)
        except Exception as e:
            log(f"ERROR {e}", agent, "ERROR")
            time.sleep(1)


if __name__ == "__main__":
    main()
