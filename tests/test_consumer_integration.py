#!/usr/bin/env python3
"""
test_consumer_integration.py — Test d'intégration du consumer corrigé.

Simule le consumer scrum-master (aucun appel LLM/GitHub/Honcho) :
- process_agent_events() avec les vraies fonctions du dépôt
- rejoue le scénario de production : événement SCRUM_REVIEW_COMPLETED
  ancien, présenté 3 cycles de suite + après expiration simulée du claim.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from launcher.agent_inbox_consumer import process_agent_events
from protocol.event_transitions import transitions_key

REDIS_HOST = os.environ.get("FLEET_TEST_REDIS_URL", "127.0.0.1")
REDIS_PORT = int(os.environ.get("FLEET_TEST_REDIS_PORT", "6399"))
AGENT = "scrum-master"


class TestConsumerIntegration(unittest.TestCase):
    """Consumer réel (fonctions importées) sur Redis jetable dédié."""

    @classmethod
    def setUpClass(cls):
        try:
            import redis
        except ImportError:
            raise unittest.SkipTest("redis-py non installé")
        cls.r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            decode_responses=True, socket_connect_timeout=5,
        )
        try:
            cls.r.ping()
        except Exception:
            raise unittest.SkipTest("Redis de test indisponible sur 6399")

    def setUp(self):
        for pattern in ("fleet:transitions:*", "claim:transition:*",
                        "inbox:product-owner", "events:scrum-master",
                        "queue:*"):
            keys = list(self.r.scan_iter(match=pattern, count=100))
            if keys:
                self.r.delete(*keys)

    def tearDown(self):
        self.setUp()

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

        # Cycle 1 : publication (l'événement n'a jamais été marqué durablement).
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 1)

        # Cycles 2 et 3 : aucune republication (marqueur durable).
        process_agent_events(self.r, AGENT)
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 1)

        # Expiration simulée de TOUS les claims temporaires : toujours rien.
        for key in list(self.r.scan_iter(match="claim:transition:*", count=100)):
            self.r.delete(key)
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 1,
                         "BUG: republication après expiration du claim")

        # Le marqueur durable contient exactement une transition.
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 1)

    def test_more_than_10_events_all_processed(self):
        """15 événements : la fenêtre paginée n'en ignore aucun."""
        for i in range(15):
            evt = {
                "type": "SCRUM_REVIEW_COMPLETED",
                "task_id": f"task_bulk_{i}",
                "msg_id": f"res_bulk_{i}",
                "reviewed_at": "2026-09-18T10:00:00Z",
                "success": True,
            }
            self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        process_agent_events(self.r, AGENT)
        self.assertEqual(self.r.llen("inbox:product-owner"), 15)

    def test_new_revision_after_old_one(self):
        """Nouvelle révision (nouveau msg_id) : nouvelle publication autorisée."""
        old = self._old_scrum_event()
        self.r.rpush(f"events:{AGENT}", json.dumps(old))
        process_agent_events(self.r, AGENT)

        new = {
            "type": "SCRUM_REVIEW_COMPLETED",
            "task_id": "task_v41_validation_scenario_1",
            "msg_id": "res_NEW_revision_2",  # nouvelle révision
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
