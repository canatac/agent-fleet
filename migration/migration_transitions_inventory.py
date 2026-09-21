#!/usr/bin/env python3
"""
migration_transitions_inventory.py — Inventaire LECTURE SEULE des transitions
historiques TASK_ASSIGNED et SCRUM_REVIEW_COMPLETED, avec vérification
préalable de reprise.

But : préparer la migration vers les marqueurs durables fleet:transitions:*
SANS exécuter la moindre transition. Pour chaque événement historique :
  - DONE    : transition prouvée par rapprochement EXACT (transition_id, ou
              liaison source msg_id + task_id + cible) dans le CONTENU des
              preuves — jamais par simple nom de fichier ou task_id partagé.
  - UNKNOWN : rapprochement incertain ou absent, avec RAISON EXPLICITE.
              (Aucun statut NEEDED n'est émis : sans preuve certaine, une
              transition historique est indéterminée, pas « à faire ».)

IMPORTANT — UNKNOWN bloque la reprise :
  Le consumer NE LIT PAS cet inventaire. Sans vérification préalable, un
  consumer démarré avec des transitions UNKNOWN non marquées les publierait.
  `verify_resume_ready()` (et le mode --check-resume) REFUSE la reprise tant
  qu'il reste des UNKNOWN. Ne jamais marquer UNKNOWN comme traité pour
  contourner ce blocage : la réconciliation humaine est requise
  (voir docs/KNOWN-ISSUES.md, réconciliation des messages PO).

Une preuve de consommation (registre) ne constitue PAS une acceptation
métier : elle prouve que la transition a été publiée et consommée, pas que
la décision métier a été validée.

Usage :
    # Inventaire (lecture seule)
    python3 migration/migration_transitions_inventory.py \
        [--redis-host H] [--redis-port P] [--redis-pass P] \
        [--agent scrum-master] [--registry-dir DIR] [--output FILE]

    # Vérification préalable de reprise (échoue si UNKNOWN restant)
    python3 migration/migration_transitions_inventory.py \
        --check-resume [--inventory FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from protocol.event_transitions import event_source_id, transition_id

DEFAULT_REGISTRY_DIR = os.environ.get("FLEET_REGISTRY_DIR", "/opt/fleet/registry")


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


# ── Preuves : rapprochement sur le CONTENU ───────────────────────────────────

def _target_of(evt: dict) -> tuple[str | None, str | None]:
    """(target_id, target_list_key) selon le type d'événement."""
    if evt.get("type") == "SCRUM_REVIEW_COMPLETED":
        return ("po", "inbox:product-owner")
    if evt.get("type") == "TASK_ASSIGNED":
        assigned = evt.get("assigned_to")
        if assigned:
            return (f"assign:{assigned}", f"queue:{assigned}")
    return (None, None)


def _expected_msg_type(evt: dict) -> str:
    return "REVIEW_REQUEST" if evt.get("type") == "SCRUM_REVIEW_COMPLETED" else "TICKET_ASSIGN"


def _scan_list_evidence(r, list_key: str, evt: dict, tid: str) -> dict | None:
    """Cherche dans une liste Redis une preuve EXACTE de la transition.

    Retourne la preuve (dict) ou None. Le rapprochement se fait sur le
    CONTENU du message :
      1. transition_id == tid (preuve des publications du nouveau code) ;
      2. liaison au msg_id SOURCE de l'événement (id du message historique
         au format <...>_<source_msg_id>, ou champ in_reply_to)
         + task_id + type de message attendu ;
      3. sinon : rien (un task_id partagé seul NE PROUVE PAS la transition —
         deux révisions de la même tâche partagent le task_id).
    """
    evt_task = evt.get("task_id")
    evt_msg_id = evt.get("msg_id")
    expected_type = _expected_msg_type(evt)
    for raw in r.lrange(list_key, 0, -1):
        try:
            m = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if m.get("task_id") != evt_task or m.get("type") != expected_type:
            continue
        # 1. transition_id exact (nouveau code).
        if m.get("transition_id") == tid:
            return {"kind": "list", "key": list_key, "match": "transition_id"}
        # 2. Liaison au msg_id source (messages historiques).
        if evt_msg_id:
            m_id = m.get("id", "")
            if evt_msg_id in m_id or m.get("in_reply_to") == evt_msg_id:
                return {"kind": "list", "key": list_key, "match": "source_msg_id_linked"}
    return None


def _scan_registry_evidence(registry_dir: str, evt: dict, tid: str) -> dict | None:
    """Cherche dans le registre (fichiers JSON) une preuve EXACTE.

    Le NOM de fichier ne compte PAS : seul le CONTENU est lu. Un fichier dont
    le nom contient le task_id mais dont le contenu est sans rapport n'est
    PAS une preuve. Le destinataire doit correspondre à la cible de la
    transition (une preuve pour un autre destinataire n'est pas retenue).
    """
    evt_task = evt.get("task_id")
    evt_msg_id = evt.get("msg_id")
    if not evt_task or not os.path.isdir(registry_dir):
        return None
    target_prefix = "product-owner" if evt.get("type") == "SCRUM_REVIEW_COMPLETED" else None
    expected_type = _expected_msg_type(evt)
    for fname in sorted(os.listdir(registry_dir)):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(registry_dir, fname)
        try:
            with open(path) as f:
                entry = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        # Destinataire : l'entrée doit concerner la cible de la transition.
        if target_prefix and not fname.startswith(target_prefix + "_"):
            continue
        msg = entry.get("message", entry)
        if msg.get("task_id") != evt_task or msg.get("type") != expected_type:
            continue
        # 1. transition_id exact.
        if msg.get("transition_id") == tid:
            return {"kind": "registry", "file": fname, "match": "transition_id"}
        # 2. Liaison au msg_id source.
        if evt_msg_id:
            m_id = msg.get("id", "")
            if (evt_msg_id in m_id or msg.get("in_reply_to") == evt_msg_id
                    or entry.get("msg_id") == evt_msg_id):
                return {"kind": "registry", "file": fname, "match": "source_msg_id_linked"}
    return None


def find_evidence(r, evt: dict, agent: str, registry_dir: str, tid: str) -> tuple[dict | None, str | None]:
    """(preuve, raison_si_unknown). Preuve EXACTE uniquement."""
    target, list_key = _target_of(evt)
    if target is None or list_key is None:
        return None, "unhandled_event_type"
    if evt.get("_invalid_json"):
        return None, "invalid_json_event"
    if event_source_id(evt) is None:
        return None, "missing_identity_fields"

    proof = _scan_list_evidence(r, list_key, evt, tid)
    if proof:
        return proof, None
    proof = _scan_registry_evidence(registry_dir, evt, tid)
    if proof:
        return proof, None
    # Rien d'exact. Un task_id partagé seul ne prouve rien : deux révisions
    # de la même tâche partagent le task_id sans partager la transition.
    return None, "no_exact_evidence_task_id_or_filename_not_sufficient"


# ── Vérification préalable de reprise ────────────────────────────────────────

def verify_resume_ready(report: dict) -> tuple[bool, list[dict]]:
    """Refuse la reprise tant qu'il reste des transitions UNKNOWN.

    Le consumer ne lit pas l'inventaire : cette vérification DOIT être
    exécutée par l'opérateur AVANT tout redémarrage de consumer. Elle ne
    démarre aucun service et n'écrit rien.
    """
    unknowns = [t for t in report.get("transitions", []) if t.get("status") == "UNKNOWN"]
    return (len(unknowns) == 0, unknowns)


# ── Inventaire ────────────────────────────────────────────────────────────────

def build_inventory(r, agent: str, registry_dir: str) -> dict:
    events = read_all_events(r, agent)
    inventory = []
    counts = {"DONE": 0, "UNKNOWN": 0}

    for evt in events:
        evt_type = evt.get("type")
        if evt_type not in ("TASK_ASSIGNED", "SCRUM_REVIEW_COMPLETED"):
            continue
        source_id = event_source_id(evt)
        if source_id is None:
            inventory.append({
                "event_type": evt_type,
                "task_id": evt.get("task_id"),
                "msg_id": evt.get("msg_id"),
                "source_id": None,
                "transition_id": None,
                "status": "UNKNOWN",
                "reason": "missing_identity_fields",
                "evidence": None,
            })
            counts["UNKNOWN"] += 1
            continue
        target, _ = _target_of(evt)
        assert target is not None  # garanti par le filtre evt_type ci-dessus
        tid = transition_id(agent, source_id, target)
        proof, reason = find_evidence(r, evt, agent, registry_dir, tid)
        entry = {
            "event_type": evt_type,
            "task_id": evt.get("task_id"),
            "msg_id": evt.get("msg_id"),
            "reviewed_at": evt.get("reviewed_at"),
            "source_id": source_id,
            "transition_id": tid,
            "status": "DONE" if proof else "UNKNOWN",
            "reason": None if proof else reason,
            "evidence": proof,
        }
        # NOTE : DONE = transition publiée/consommée prouvée par rapprochement
        # exact. Cela NE vaut PAS acceptation métier.
        inventory.append(entry)
        counts[entry["status"]] += 1

    return {
        "generated_at": now_iso(),
        "agent": agent,
        "read_only": True,
        "registry_dir": registry_dir,
        "total_events_scanned": len(events),
        "counts": counts,
        "transitions": inventory,
        "notes": [
            "DONE = transition prouvée par rapprochement EXACT (transition_id ou",
            "liaison source msg_id + task_id + cible) dans le CONTENU des preuves.",
            "Une preuve de consommation n'est PAS une acceptation métier.",
            "UNKNOWN = rapprochement incertain : la reprise est REFUSÉE par",
            "verify_resume_ready() tant que chaque UNKNOWN n'est pas réconcilié",
            "par décision humaine. Ne jamais marquer UNKNOWN comme traité pour",
            "contourner ce blocage. Les marqueurs transition_id sont calculés avec",
            "les fonctions du code (event_source_id / transition_id).",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventaire lecture seule + vérification préalable de reprise")
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "172.16.12.2"))
    parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")))
    parser.add_argument("--redis-pass", default=os.environ.get("REDIS_PASS", ""))
    parser.add_argument("--agent", default="scrum-master")
    parser.add_argument("--registry-dir", default=DEFAULT_REGISTRY_DIR,
                        help="Répertoire du registre (configurable pour tests/fixtures)")
    parser.add_argument("--output", default=None)
    parser.add_argument("--check-resume", action="store_true",
                        help="Vérifie un inventaire existant : refuse la reprise si UNKNOWN restant")
    parser.add_argument("--inventory", default=None,
                        help="Fichier d'inventaire pour --check-resume")
    args = parser.parse_args()

    if args.check_resume:
        inv_path = args.inventory or f"migration_inventory_{args.agent}.json"
        if not os.path.exists(inv_path):
            print(json.dumps({"resume_ready": False, "error": f"inventory_not_found:{inv_path}"}))
            return 2
        with open(inv_path) as f:
            report = json.load(f)
        ok, unknowns = verify_resume_ready(report)
        result = {
            "resume_ready": ok,
            "unknown_count": len(unknowns),
            "unknowns": [
                {"task_id": u.get("task_id"), "msg_id": u.get("msg_id"), "reason": u.get("reason")}
                for u in unknowns
            ],
        }
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if ok else 1

    import redis
    r = redis.Redis(
        host=args.redis_host, port=args.redis_port,
        password=args.redis_pass or None,
        decode_responses=True, socket_connect_timeout=5,
    )
    r.ping()

    report = build_inventory(r, args.agent, args.registry_dir)
    out = args.output or f"migration_inventory_{args.agent}.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    ok, unknowns = verify_resume_ready(report)
    print(json.dumps({
        "output_file": out,
        "counts": report["counts"],
        "total": len(report["transitions"]),
        "resume_ready": ok,
        "unknown_count": len(unknowns),
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
