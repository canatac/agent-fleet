#!/usr/bin/env python3
"""
migration_transitions_inventory.py — Inventaire LECTURE SEULE des transitions
historiques TASK_ASSIGNED et SCRUM_REVIEW_COMPLETED.

But : préparer la migration vers les marqueurs durables fleet:transitions:*
SANS exécuter la moindre transition. Pour chaque événement historique :
  - DONE      : transition déjà réalisée (preuves Redis : inbox/queue/registre)
  - NEEDED    : transition encore nécessaire (aucune preuve de publication)
  - UNKNOWN   : indéterminée — NE DOIT PAS être exécutée automatiquement
                à la reprise ; décision humaine requise.

Les marqueurs sont calculés avec les fonctions du code (event_source_id /
transition_id), jamais saisis approximativement.

Usage (lecture seule, aucune écriture Redis) :
    python3 migration/migration_transitions_inventory.py \
        [--redis-host H] [--redis-port P] [--redis-pass P] [--agent scrum-master]

Sortie : JSON sur stdout + fichier migration_inventory_<agent>.json.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from protocol.event_transitions import event_source_id, transition_id


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_all_events(r, agent: str) -> list[dict]:
    """Lecture paginée de TOUS les événements de events:<agent>."""
    events = []
    key = f"events:{agent}"
    total = r.llen(key)
    page = 100
    for start in range(0, total, page):
        for raw in r.lrange(key, start, start + page - 1):
            try:
                events.append(json.loads(raw))
            except json.JSONDecodeError:
                events.append({"_invalid_json": raw[:200]})
    return events


def find_published_evidence(r, evt: dict, agent: str) -> dict:
    """Recherche des preuves QUE LA TRANSITION A ÉTÉ PUBLIÉE (lecture seule).

    Preuves acceptées :
    - inbox:product-owner contient un message avec ce task_id ET un
      état/body cohérent avec une REVIEW_REQUEST (pour SCRUM_REVIEW_COMPLETED)
    - queue:<assigned_to> contient un TICKET_ASSIGN pour ce task_id
      (pour TASK_ASSIGNED)
    - le registre /opt/fleet/registry contient une entrée pour ce task_id
      côté cible (preuve de consommation, plus forte que la publication)
    """
    evt_type = evt.get("type")
    task_id = evt.get("task_id")
    evidence = {"inbox_hit": False, "queue_hit": False, "registry_hit": False}

    if evt_type == "SCRUM_REVIEW_COMPLETED" and task_id:
        for raw in r.lrange("inbox:product-owner", 0, -1):
            try:
                m = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if m.get("task_id") == task_id and m.get("type") == "REVIEW_REQUEST":
                evidence["inbox_hit"] = True
                break
        # Registre PO : preuve de consommation (fichiers locaux).
        registry_dir = "/opt/fleet/registry"
        if os.path.isdir(registry_dir) and task_id:
            for fname in os.listdir(registry_dir):
                if task_id in fname and fname.startswith("product-owner"):
                    evidence["registry_hit"] = True
                    break

    elif evt_type == "TASK_ASSIGNED" and task_id:
        assigned_to = evt.get("assigned_to", "")
        if assigned_to:
            for raw in r.lrange(f"queue:{assigned_to}", 0, -1):
                try:
                    m = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if m.get("task_id") == task_id and m.get("type") == "TICKET_ASSIGN":
                    evidence["queue_hit"] = True
                    break
            registry_dir = "/opt/fleet/registry"
            if os.path.isdir(registry_dir):
                for fname in os.listdir(registry_dir):
                    if task_id in fname:
                        evidence["registry_hit"] = True
                        break
    return evidence


def classify(evt: dict, evidence: dict) -> str:
    """DONE / NEEDED / UNKNOWN."""
    if evt.get("_invalid_json"):
        return "UNKNOWN"
    # Publication prouvée (message encore présent) OU consommation prouvée
    # (registre) → la transition a eu lieu.
    if evidence["inbox_hit"] or evidence["queue_hit"] or evidence["registry_hit"]:
        return "DONE"
    # Pas de preuve → indéterminé si l'événement est ancien (le message
    # publié a pu être consommé et supprimé de l'inbox : BRPOP l'enlève).
    # On ne peut PAS distinguer "jamais publié" de "publié puis consommé"
    # sans registre → UNKNOWN par défaut, SAUF si le claim temporaire
    # historique existe encore (preuve que le consumer l'a traité).
    return "UNKNOWN"


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventaire lecture seule des transitions historiques")
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "172.16.12.2"))
    parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")))
    parser.add_argument("--redis-pass", default=os.environ.get("REDIS_PASS", ""))
    parser.add_argument("--agent", default="scrum-master")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    import redis
    r = redis.Redis(
        host=args.redis_host, port=args.redis_port,
        password=args.redis_pass or None,
        decode_responses=True, socket_connect_timeout=5,
    )
    r.ping()

    events = read_all_events(r, args.agent)
    inventory = []
    counts = {"DONE": 0, "NEEDED": 0, "UNKNOWN": 0}

    for evt in events:
        evt_type = evt.get("type")
        if evt_type not in ("TASK_ASSIGNED", "SCRUM_REVIEW_COMPLETED"):
            continue
        source_id = event_source_id(evt)
        if source_id is None:
            inventory.append({
                "event": evt, "source_id": None,
                "transition_id": None,
                "status": "UNKNOWN",
                "reason": "missing_identity_fields",
                "evidence": None,
            })
            counts["UNKNOWN"] += 1
            continue
        evidence = find_published_evidence(r, evt, args.agent)
        status = classify(evt, evidence)
        target = "po" if evt_type == "SCRUM_REVIEW_COMPLETED" else f"assign:{evt.get('assigned_to')}"
        tid = transition_id(args.agent, source_id, target)
        inventory.append({
            "event_type": evt_type,
            "task_id": evt.get("task_id"),
            "msg_id": evt.get("msg_id"),
            "reviewed_at": evt.get("reviewed_at"),
            "source_id": source_id,
            "transition_id": tid,
            "status": status,
            "evidence": evidence,
        })
        counts[status] += 1

    report = {
        "generated_at": now_iso(),
        "agent": args.agent,
        "read_only": True,
        "total_events_scanned": len(events),
        "counts": counts,
        "transitions": inventory,
        "notes": [
            "UNKNOWN = aucune preuve de publication ni de consommation ; ces transitions",
            "NE DOIVENT PAS être exécutées automatiquement à la reprise. Décision humaine",
            "requise (voir procédure de réconciliation des messages PO avant tout ACK).",
            "Les marqueurs transition_id sont calculés avec les fonctions du code",
            "(event_source_id / transition_id), jamais saisis manuellement.",
        ],
    }

    out = args.output or f"migration_inventory_{args.agent}.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(json.dumps({"output_file": out, "counts": counts, "total": len(inventory)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
