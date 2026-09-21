#!/usr/bin/env python3
"""
event_transitions.py — Transitions d'événements idempotentes (Scrum → PO).

Problème corrigé (2026-09-21) :
  process_agent_events() relisait les 10 derniers événements de events:<agent>
  à chaque itération. Le verrou claim:<agent>_event:<task_id>:<suffix> était
  un SET NX EX 3600 : mémoire TEMPORAIRE. À l'expiration, un ancien
  SCRUM_REVIEW_COMPLETED était re-traité → nouvelle REVIEW_REQUEST → nouvelle
  exécution PO (boucle horaire observée 66+ RESULT_REJECTED en production).

Solution :
  - Identité de transition déterministe fondée sur les champs immuables de
    l'événement source (type + task_id + msg_id de l'événement) et la cible.
    Une NOUVELLE revue d'une NOUVELLE révision (nouveau msg_id source) garde
    une identité distincte → publication autorisée.
  - Marqueur durable SADD (sans expiration) : fleet:transitions:<agent>.
  - Publication + marquage atomiques via script Lua quand le marqueur et
    l'inbox cible sont dans la même instance Redis.
  - Le claim NX EX reste utilisé uniquement comme garde anti-concurrence
    immédiate, jamais comme mémoire des événements traités.

Limites documentées :
  - Exactement-une-exécution n'est PAS garanti pour les effets externes ;
    cette correction garantit l'unicité de la PUBLICATION de la transition.
  - La durabilité dépend de la persistance Redis (AOF/RDB) et de la
    conservation du set fleet:transitions:<agent>. Une perte de données Redis
    peut autoriser une republication.
  - Si marqueur et inbox sont sur des instances différentes, fallback
    non-atomique (SADD puis RPUSH) avec risque documenté de doublon en cas
    de crash entre les deux opérations.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Optional

# ── Clés Redis ────────────────────────────────────────────────────────────────

def transitions_key(agent: str) -> str:
    """Set durable des transitions déjà publiées pour cet agent."""
    return f"fleet:transitions:{agent}"


def transition_claim_key(agent: str, transition_id: str) -> str:
    """Claim temporaire anti-concurrence (jamais mémoire)."""
    return f"claim:transition:{agent}:{transition_id}"


# ── Identité déterministe ─────────────────────────────────────────────────────

_SAFE_ID = re.compile(r"[^a-zA-Z0-9_.:-]")


def event_source_id(evt: dict[str, Any]) -> Optional[str]:
    """
    Identité de l'événement SOURCE à partir de ses champs immuables.

    Règle déterministe (les événements existants n'ont pas d'UUID) :
      type + task_id + msg_id (ou reviewed_at en fallback).

    - msg_id est unique par résultat consommé → une nouvelle revue d'une
      nouvelle révision produit un nouvel identifiant.
    - Le même événement relu (mêmes champs) produit TOUJOURS le même
      identifiant, quel que soit le moment.
    - Retourne None si les champs nécessaires manquent (événement ignoré,
      aucune publication, aucun marquage).
    """
    evt_type = evt.get("type")
    task_id = evt.get("task_id")
    if not evt_type or not task_id:
        return None
    # msg_id identifie le résultat source ; reviewed_at est le fallback
    # chronologique documenté pour les anciens événements sans msg_id.
    discriminator = evt.get("msg_id") or evt.get("reviewed_at") or evt.get("ts")
    if not discriminator:
        return None
    raw = f"{evt_type}|{task_id}|{discriminator}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def transition_id(agent: str, source_id: str, target: str) -> str:
    """
    Identité de la TRANSITION (événement source → cible).

    Déduplique par événement source ET cible : le même événement vers deux
    cibles différentes reste deux transitions distinctes.
    """
    safe_agent = _SAFE_ID.sub("_", agent)[:64]
    safe_target = _SAFE_ID.sub("_", target)[:64]
    return f"{safe_agent}:{source_id}:{safe_target}"


# ── Script Lua : publication + marquage atomiques ─────────────────────────────

# KEYS[1] = set des transitions (marqueur durable)
# KEYS[2] = liste cible (inbox)
# ARGV[1] = transition_id
# ARGV[2] = payload du message à publier
#
# Validation AVANT toute écriture (un script Lua Redis n'annule pas les
# écritures déjà effectuées en cas d'erreur) :
#   - SISMEMBER sur KEYS[1] échoue avec WRONGTYPE si ce n'est pas un set ;
#   - LLEN sur KEYS[2] échoue avec WRONGTYPE si ce n'est pas une liste.
# Les deux lectures précèdent la première écriture : sur mauvais type,
# le script s'arrête avec une erreur explicite SANS avoir rien marqué.
PUBLISH_TRANSITION_LUA = """
local already = redis.call('SISMEMBER', KEYS[1], ARGV[1])
if already == 1 then
  return 0
end
-- Validation de type : lève WRONGTYPE avant toute écriture si les clés
-- n'ont pas les types attendus.
redis.call('LLEN', KEYS[2])
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('SADD', KEYS[1], ARGV[1])
return 1
"""


def publish_transition_once(
    r,
    agent: str,
    evt: dict[str, Any],
    *,
    target: str,
    target_list_key: str,
    build_payload,
) -> tuple[str, str]:
    """
    Publie la transition événement → cible exactement une fois.

    Retourne (transition_id, outcome) où outcome ∈
      {"published", "already_published"}.

    Args:
      r: client Redis (marqueur et inbox doivent être sur la même instance).
      agent: agent source (ex. "scrum-master").
      evt: événement source (dict décodé de events:<agent>).
      target: identifiant de cible (ex. "product-owner") — participe à
        l'identité de transition.
      target_list_key: clé de liste Redis cible (ex. "inbox:product-owner").
      build_payload: callable(transition_id) -> str ; construit le message
        JSON à publier. Appelé AVANT le script Lua ; l'identité de
        transition est injectée dans le message pour la traçabilité.

    Erreurs :
      ValueError si l'événement n'a pas les champs nécessaires.
      redis.ResponseError (WRONGTYPE) si les clés n'ont pas les types
      attendus — dans ce cas RIEN n'a été marqué ni publié.
    """
    source_id = event_source_id(evt)
    if source_id is None:
        raise ValueError(
            f"event missing immutable identity fields: "
            f"type={evt.get('type')!r} task_id={evt.get('task_id')!r} "
            f"msg_id={evt.get('msg_id')!r} reviewed_at={evt.get('reviewed_at')!r} ts={evt.get('ts')!r}"
        )
    tid = transition_id(agent, source_id, target)
    payload = build_payload(tid)

    result = r.eval(
        PUBLISH_TRANSITION_LUA, 2,
        transitions_key(agent), target_list_key,
        tid, payload,
    )
    if result == 1:
        return tid, "published"
    return tid, "already_published"
