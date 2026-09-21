#!/usr/bin/env python3
"""
test_consumer_integration.py — Test d'intégration du consumer corrigé.

Simule le consumer scrum-master (aucun appel LLM/GitHub/Honcho) :
- process_agent_events() avec les vraies fonctions du dépôt
- rejoue le scénario de production : événement SCRUM_REVIEW_COMPLETED
  ancien, présenté 3 cycles de suite + après expiration simulée du claim
- 120 événements via le VRAI process_agent_events
- événements invalides journalisés sans publication ni marquage.

Redis jetable démarré/arrêté par le dispositif de test (redis_fixture).
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tests.redis_fixture as fixture
from launcher.agent_inbox_consumer import process_agent_events
from protocol.event_transitions import transitions_key

AGENT = "scrum-master"


class TestConsumerIntegration(unittest.TestCase):
    """Consumer réel (fonctions importées) sur Redis jetable dédié."""

    @classmethod
    def setUpClass(cls):
        addr = fixture.start_test_redis()
        if addr is None:
            raise unittest.SkipTest(
                "Redis de test INDISPONIBLE — tests SKIPÉS (pas PASS). "
                "Installer redis-server pour les exécuter."
            )
        cls.redis_addr = addr
        import redis
        cls.r = redis.Redis(
            host=addr[0], port=addr[1],
            decode_responses=True, socket_connect_timeout=5,
        )
        cls.r.ping()

    @classmethod
    def tearDownClass(cls):
        fixture.stop_test_redis()

    def setUp(self):
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    def _cleanup(self):
        # Instance démarrée par la fixture uniquement : aucun risque de
        # nettoyer une instance externe fournie par variable.
        for pattern in ("fleet:transitions:*", "claim:transition:*",
                        "inbox:product-owner", "events:scrum-master",
                        "queue:dev-web"):
            keys = list(self.r.scan_iter(match=pattern, count=100))
            if keys:
                self.r.delete(*keys)

    def _old_scrum_event(self):
        """Événement historique de production (sans UUID dédié)."""
        return {
            "type": "SCRUM_REVIEW_COMPLETED",
            "task_id": "task_v41_validation_scenario_1",
            "msg_id": "res_c1c96f401f094c2e",
            "reviewed_at": "2026-09-18T09:52:32Z",
            "success": True,
        }

    def test_replay_loop_fixed(self):
        """Scénario production : événement ancien, 3 cycles + claim expiré."""
        evt = self._old_scrum_event()
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))

        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 1)

        process_agent_events(self.r, AGENT)
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 1)

        for key in list(self.r.scan_iter(match="claim:transition:*", count=100)):
            self.r.delete(key)
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 1,
                         "BUG: republication après expiration du claim")

        self.assertEqual(self.r.scard(transitions_key(AGENT)), 1)

    def test_120_events_via_real_process_agent_events(self):
        """120 événements via le VRAI process_agent_events : 120 publications,
        et un second cycle n'en republie AUCUNE."""
        for i in range(120):
            evt = {
                "type": "SCRUM_REVIEW_COMPLETED",
                "task_id": f"task_bulk_{i}",
                "msg_id": f"res_bulk_{i}",
                "reviewed_at": "2026-09-18T10:00:00Z",
                "success": True,
            }
            self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 120)
        # Second cycle complet : idempotence sur les 120.
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 120)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 120)

    def test_invalid_events_logged_not_published_not_marked(self):
        """Événements invalides : journalisés, sans publication ni marquage."""
        invalid_events = [
            {"type": "SCRUM_REVIEW_COMPLETED", "task_id": "t"},           # pas de msg_id/ts
            {"type": "SCRUM_REVIEW_COMPLETED"},                            # pas de task_id
            {"type": "TASK_ASSIGNED", "task_id": "t"},                     # pas d'identité
            {"type": "UNKNOWN_TYPE", "task_id": "t", "msg_id": "m"},       # type ignoré
            "not-json",                                                    # JSON invalide
        ]
        for e in invalid_events:
            self.r.rpush(f"events:{AGENT}", e if isinstance(e, str) else json.dumps(e))
        process_agent_events(self.r, AGENT)
        # Aucune publication, aucun marquage.
        self.assertEqual(self.r.llen("inbox:product-owner"), 0)
        self.assertEqual(self.r.llen("queue:dev-web"), 0)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 0)

    def test_new_revision_after_old_one(self):
        """Nouvelle révision (nouveau msg_id) : nouvelle publication autorisée."""
        old = self._old_scrum_event()
        self.r.rpush(f"events:{AGENT}", json.dumps(old))
        process_agent_events(self.r, AGENT)

        new = {
            "type": "SCRUM_REVIEW_COMPLETED",
            "task_id": "task_v41_validation_scenario_1",
            "msg_id": "res_NEW_revision_2",
            "reviewed_at": "2026-09-21T12:00:00Z",
            "success": True,
        }
        self.r.rpush(f"events:{AGENT}", json.dumps(new))
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 2)

    def test_task_assigned_idempotent(self):
        """Transition TASK_ASSIGNED : même idempotence."""
        evt = {
            "type": "TASK_ASSIGNED",
            "task_id": "task_fix_694",
            "msg_id": "res_assign_1",
            "assigned_to": "dev-web",
        }
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        process_agent_events(self.r, AGENT)
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("queue:dev-web"), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
