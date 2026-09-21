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


# ── Liaison source exacte ────────────────────────────────────────────────────

# Formats historiques documentés (observés en production, cf. inbox:po et
# queue:dev-web) :
#   REVIEW_REQUEST (Scrum → PO) :
#     id = "scrum_to_po_<task_id>_<source_msg_id>"
#     (ex. scrum_to_po_task_x_res_rev1)
#   TICKET_ASSIGN (Scrum → dev) :
#     id = "scrum_to_<assigned_to>_<task_id>_<source_msg_id>"
#     (ex. scrum_to_dev-web_task_fix_694_res_asg1)
# Ces formats sont reconstruits INTÉGRALEMENT et comparés par ÉGALITÉ
# stricte. Aucun rapprochement par sous-chaîne (evt_msg_id in m_id) :
# "res_rev1" matcherait "res_rev10" — interdit.
def _historic_message_id(evt: dict) -> str | None:
    """Format historique documenté, reconstruit intégralement selon le type."""
    task_id = evt.get("task_id", "")
    source_msg_id = evt.get("msg_id", "")
    if evt.get("type") == "SCRUM_REVIEW_COMPLETED":
        return f"scrum_to_po_{task_id}_{source_msg_id}"
    if evt.get("type") == "TASK_ASSIGNED":
        assigned = evt.get("assigned_to", "")
        return f"scrum_to_{assigned}_{task_id}_{source_msg_id}"
    return None


def _source_link_exact(evt: dict, m: dict) -> bool:
    """Liaison source EXPLICITE et EXACTE uniquement.

    Accepte uniquement :
      1. m["in_reply_to"] == evt["msg_id"] (liaison protocolaire exacte) ;
      2. m["id"] == format historique reconstruit intégralement
         (égalité stricte, jamais une sous-chaîne).
    Tout le reste (sous-chaîne, préfixe, ressemblance) → False.
    """
    evt_msg_id = evt.get("msg_id")
    if not evt_msg_id:
        return False
    if m.get("in_reply_to") == evt_msg_id:
        return True
    expected = _historic_message_id(evt)
    return expected is not None and m.get("id") == expected


def _recipient_matches(evt: dict, m: dict) -> bool:
    """Vérifie le destinataire DANS LE CONTENU du message.

    - REVIEW_REQUEST → m["to"] == "product-owner"
    - TICKET_ASSIGN  → m["to"] == evt["assigned_to"]
    Destinataire absent ou contradictoire → False. Un nom de fichier ne
    remplace JAMAIS cette vérification.
    """
    expected_to = "product-owner" if evt.get("type") == "SCRUM_REVIEW_COMPLETED" else evt.get("assigned_to")
    return m.get("to") == expected_to


def _scan_list_evidence(r, list_key: str, evt: dict, tid: str) -> dict | None:
    """Cherche dans une liste Redis une preuve EXACTE de la transition.

    Retourne la preuve (dict) ou None. Le rapprochement se fait sur le
    CONTENU du message :
      1. transition_id == tid (égalité exacte, preuve du nouveau code) ;
      2. liaison source EXACTE (in_reply_to ou format historique reconstruit
         et comparé par égalité) + task_id + type attendu + destinataire
         vérifié dans le contenu ;
      3. sinon : rien (un task_id partagé seul NE PROUVE PAS la transition —
         deux révisions de la même tâche partagent le task_id).
    """
    evt_task = evt.get("task_id")
    expected_type = _expected_msg_type(evt)
    for raw in r.lrange(list_key, 0, -1):
        try:
            m = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if m.get("task_id") != evt_task or m.get("type") != expected_type:
            continue
        # Destinataire vérifié dans le CONTENU (jamais le nom de fichier).
        if not _recipient_matches(evt, m):
            continue
        # 1. transition_id exact (nouveau code).
        if m.get("transition_id") == tid:
            return {"kind": "list", "key": list_key, "match": "transition_id"}
        # 2. Liaison source exacte (historique).
        if _source_link_exact(evt, m):
            return {"kind": "list", "key": list_key, "match": "source_msg_id_linked"}
    return None


def _scan_registry_evidence(registry_dir: str, evt: dict, tid: str) -> dict | None:
    """Cherche dans le registre (fichiers JSON) une preuve EXACTE.

    Le NOM de fichier ne compte PAS : seul le CONTENU est lu. Un fichier dont
    le nom contient le task_id mais dont le contenu est sans rapport n'est
    PAS une preuve. Le destinataire est vérifié DANS LE CONTENU du message
    (to == product-owner / to == assigned_to) — le préfixe du nom de fichier
    n'est qu'un indice de parcours, jamais une preuve.
    """
    evt_task = evt.get("task_id")
    if not evt_task or not os.path.isdir(registry_dir):
        return None
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
        msg = entry.get("message", entry)
        if msg.get("task_id") != evt_task or msg.get("type") != expected_type:
            continue
        # Destinataire vérifié dans le CONTENU du message.
        if not _recipient_matches(evt, msg):
            continue
        # 1. transition_id exact.
        if msg.get("transition_id") == tid:
            return {"kind": "registry", "file": fname, "match": "transition_id"}
        # 2. Liaison source exacte (in_reply_to, format historique reconstruit,
        #    ou champ msg_id du registre — égalité stricte).
        evt_msg_id = evt.get("msg_id")
        if evt_msg_id:
            if (msg.get("in_reply_to") == evt_msg_id
                    or entry.get("msg_id") == evt_msg_id
                    or _source_link_exact(evt, msg)):
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

_VALID_STATUSES = ("DONE", "UNKNOWN")


def _validate_report_structure(report: dict | None, expected_agent: str | None = None) -> list[str]:
    """Valide la STRUCTURE du rapport avant d'examiner les statuts.

    Retourne la liste des erreurs (vide = structure valide) :
      - rapport absent/vide ({} doit être refusé) ;
      - agent attendu (si fourni) ;
      - liste 'transitions' présente et itérable ;
      - compteurs 'counts' présents et COHÉRENTS avec la liste ;
      - chaque DONE : transition_id, source_id et evidence présents ;
      - statut absent ou non reconnu → refus.
    Un inventaire réellement vide (transitions == [], counts == 0) reste
    acceptable s'il est complet et cohérent.
    """
    errors: list[str] = []
    if not isinstance(report, dict) or not report:
        return ["report_missing_or_empty"]
    if expected_agent is not None and report.get("agent") != expected_agent:
        errors.append(f"agent_mismatch:expected={expected_agent},got={report.get('agent')!r}")
    transitions = report.get("transitions")
    if not isinstance(transitions, list):
        errors.append("transitions_missing_or_not_a_list")
        return errors
    counts = report.get("counts")
    if not isinstance(counts, dict):
        errors.append("counts_missing")
        counts = {}
    seen = {"DONE": 0, "UNKNOWN": 0}
    for i, t in enumerate(transitions):
        if not isinstance(t, dict):
            errors.append(f"transition[{i}]:not_an_object")
            continue
        status = t.get("status")
        if status not in _VALID_STATUSES:
            errors.append(f"transition[{i}]:invalid_status:{status!r}")
            continue
        seen[status] = seen.get(status, 0) + 1
        if status == "DONE":
            for field in ("transition_id", "source_id", "evidence"):
                if not t.get(field):
                    errors.append(f"transition[{i}]:done_missing_{field}")
    for key in _VALID_STATUSES:
        if counts.get(key) != seen[key]:
            errors.append(f"counts_mismatch:{key}:reported={counts.get(key)!r},actual={seen[key]}")
    return errors


def verify_resume_ready(report: dict | None, expected_agent: str | None = None) -> tuple[bool, list[dict]]:
    """Refuse la reprise tant que la structure est invalide ou qu'il reste
    des transitions UNKNOWN.

    Le consumer ne lit pas l'inventaire : cette vérification DOIT être
    exécutée par l'opérateur AVANT tout redémarrage de consumer. Elle ne
    démarre aucun service et n'écrit rien.

    PORTÉE — --check-resume valide l'INVENTAIRE uniquement :
      il NE vérifie NI la présence des marqueurs fleet:transitions:* dans
      Redis, NI l'état des services (consumers arrêtés, watchdog, etc.).
      Ces vérifications sont à la charge de l'opérateur.

    En cas d'erreur de structure, retourne (False, [{"reason": "..."}]).
    """
    errors = _validate_report_structure(report, expected_agent)
    if errors:
        return False, [{"reason": e} for e in errors]
    assert report is not None  # garanti par _validate_report_structure
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
        ok, unknowns = verify_resume_ready(report, expected_agent=args.agent)
        result = {
            "resume_ready": ok,
            "unknown_count": len(unknowns),
            "unknowns": [
                {"task_id": u.get("task_id"), "msg_id": u.get("msg_id"), "reason": u.get("reason")}
                for u in unknowns
            ],
            "scope_note": "check-resume validates the INVENTORY only: it does NOT verify "
                          "fleet:transitions:* markers in Redis, nor service state.",
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
