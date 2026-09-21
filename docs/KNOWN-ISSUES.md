# Défauts préexistants — NON résolus par la PR #1

> Documenté le : 2026-09-21
> La PR #1 (fix/scrum-event-idempotency) ne corrige QUE la republication des
> transitions Scrum → PO. Les défauts suivants existaient avant, existent
> toujours après, et nécessitent des corrections séparées.

## 1. BRPOP sans réservation récupérable

`launcher/agent_inbox_consumer.py` — boucle principale :

```python
result = r.brpop(f"inbox:{agent}", timeout=BRPOP_TIMEOUT)
```

`BRPOP` retire le message de l'inbox AVANT traitement. Si le consumer crash
entre le BRPOP et le traitement (ou pendant le launcher), le message est
perdu : aucun `processing:<agent>` intermédiaire, contrairement au relay v4.1
qui utilise RPOPLPUSH + réconciliation au démarrage.

Impact : perte de message silencieuse sur crash du consumer inbox.
Non résolu par la PR #1 (qui concerne uniquement la republication des
événements, pas la consommation des inbox).

## 2. ACK inconditionnel après échec du launcher

`launcher/agent_inbox_consumer.py` — `consume_message()` :

```python
# ACK final
r.set(f"msg:ack:{msg_id}", "1", ex=86400)
```

Cet ACK s'exécute QUEL QUE SOIT le résultat du launcher — y compris
`LAUNCH_FAILED exit=1` (observé en production : chaque exécution en échec
de la boucle horaire a été ACKée, cf. `/var/log/fleet/inbox_product-owner.log`,
66+ ACK après échec). Le message est considéré traité alors que le travail
a échoué : aucune rétention, aucun retry, pas de DLQ.

Impact : échec de traitement définitif silencieux.
Non résolu par la PR #1.

## 3. Cause de CLAIM_LOST encore inconnue

`launcher/fleet_launcher.py` — les exécutions PO de
`process_task_v41_validation_scenario_1` se sont toutes terminées par
`RESULT_REJECTED (claim lost)` (durées 124s→268s, TTL 300s, heartbeat 10s),
mais :

- le heartbeat ne journalise NI les renouvellements réussis NI le moment
  précis de la perte (`CLAIM_LOST`/`CLAIM_NOT_OWNER` absents des journaux
  pour ces attempts) ;
- aucune preuve de contention Redis (pas de mesure de latence) ;
- aucune preuve d'expiration naturelle (durées < TTL) ni de vol de claim
  (aucun autre writer identifié).

Statut : inconnu. Nécessite un ticket d'instrumentation séparé : journaliser
acquisition, chaque renouvellement (attempt_id, âge du claim, TTL restant),
expiration/changement de propriétaire, avec task_id, attempt_id et horodatage
— sans exposer de secrets. La PR #1 ne prétend pas résoudre ce point.

## 4. Réconciliation des 2 messages PO historiques (en attente)

`inbox:product-owner` contient toujours :
- `scrum_to_po_699_review` (REVIEW_REQUEST, task_pilote_v41_001c, 2026-09-18T09:20:00Z)
- `scrum_to_po_698` (BLOCKED_APPROVAL, task_pilote_v41_001b, 2026-09-18T09:15:00Z)

La PR #699 concernée a déjà été mergée. Ces messages ne doivent PAS être
supprimés silencieusement : procédure de réconciliation requise avant tout
ACK (établir l'identité de transition, vérifier la décision métier existante,
enregistrer une décision explicite de traitement sans nouvelle exécution,
puis acquitter de façon fiable). Voir `migration/migration_transitions_inventory.py`
pour l'inventaire lecture seule.
