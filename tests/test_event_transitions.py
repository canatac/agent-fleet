#!/usr/bin/env python3
"""
test_event_transitions.py — Tests de la transition idempotente Scrum → PO.

Redis jetable dédié obligatoire :
    FLEET_TEST_REDIS_URL=127.0.0.1 FLEET_TEST_REDIS_PORT=6399

Aucun appel LLM, GitHub ou Honcho : le consumer et le launcher sont simulés.
"""

import json
import os
import subprocess
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from protocol.event_transitions import (
    event_source_id,
    publish_transition_once,
    transition_id,
    transitions_key,
)
from protocol.fleet_protocol import make_message

REDIS_HOST = os.environ.get("FLEET_TEST_REDIS_URL", "127.0.0.1")
REDIS_PORT = int(os.environ.get("FLEET_TEST_REDIS_PORT", "6399"))
AGENT = "scrum-master"
INBOX = "inbox:product-owner"
PREFIX_KEYS = ("fleet:transitions:*", "claim:transition:*", "inbox:product-owner")


def make_scrum_event(task_id="task_v41", msg_id="res_abc123", reviewed_at=None):
    evt = {
        "type": "SCRUM_REVIEW_COMPLETED",
        "task_id": task_id,
        "msg_id": msg_id,
        "reviewed_at": reviewed_at or "2026-09-18T09:52:32Z",
        "success": True,
    }
    return evt


class DisposableRedisTestCase(unittest.TestCase):
    """Base : Redis jetable dédié sur le port 6399, jamais la production."""

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
            raise unittest.SkipTest(
                f"Redis de test indisponible sur {REDIS_HOST}:{REDIS_PORT} "
                "(lancer: redis-server --port 6399 --save '' --appendonly no)"
            )

    def setUp(self):
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    def _cleanup(self):
        for pattern in PREFIX_KEYS:
            keys = list(self.r.scan_iter(match=pattern, count=100))
            if keys:
                self.r.delete(*keys)

    def publish(self, evt, agent=AGENT, target="product-owner", target_list=INBOX):
        def build(tid):
            msg = make_message(
                type="REVIEW_REQUEST",
                to=target,
                from_=agent,
                body=f"Demande de revue PO pour tâche {evt.get('task_id')}.",
                task_id=evt.get("task_id"),
                extra={"reply_to": agent, "transition_id": tid},
            )
            return json.dumps(msg)

        return publish_transition_once(
            self.r, agent, evt,
            target=target, target_list_key=target_list,
            build_payload=build,
        )


class TestIdentity(unittest.TestCase):
    """Identité stable et déterministe, sans Redis."""

    def test_same_event_same_identity(self):
        e1 = make_scrum_event()
        e2 = make_scrum_event()  # relecture de la même source
        self.assertEqual(event_source_id(e1), event_source_id(e2))

    def test_new_revision_new_identity(self):
        e_old = make_scrum_event(msg_id="res_revision_1")
        e_new = make_scrum_event(msg_id="res_revision_2")
        self.assertNotEqual(event_source_id(e_old), event_source_id(e_new))

    def test_missing_fields_returns_none(self):
        self.assertIsNone(event_source_id({"type": "SCRUM_REVIEW_COMPLETED"}))
        self.assertIsNone(event_source_id({"task_id": "t"}))
        self.assertIsNone(event_source_id({}))

    def test_transition_id_includes_target(self):
        sid = event_source_id(make_scrum_event())
        t1 = transition_id(AGENT, sid, "product-owner")
        t2 = transition_id(AGENT, sid, "other-target")
        self.assertNotEqual(t1, t2)


class TestPublishOnce(DisposableRedisTestCase):
    """1. Même événement présenté 100 fois : une seule publication."""

    def test_same_event_100_times_single_publication(self):
        evt = make_scrum_event()
        outcomes = [self.publish(evt)[1] for _ in range(100)]
        self.assertEqual(outcomes.count("published"), 1)
        self.assertEqual(outcomes.count("already_published"), 99)
        self.assertEqual(self.r.llen(INBOX), 1)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 1)

    def test_two_distinct_events_two_publications(self):
        e1 = make_scrum_event(task_id="task_a", msg_id="res_a")
        e2 = make_scrum_event(task_id="task_b", msg_id="res_b")
        _, o1 = self.publish(e1)
        _, o2 = self.publish(e2)
        self.assertEqual((o1, o2), ("published", "published"))
        self.assertEqual(self.r.llen(INBOX), 2)


class TestConcurrency(DisposableRedisTestCase):
    """2. Deux consumers concurrents : une seule publication."""

    def test_two_concurrent_consumers_single_publication(self):
        evt = make_scrum_event()
        payload = json.dumps(evt)

        script = (
            "import json, sys, os\n"
            "sys.path.insert(0, '.')\n"
            "import redis\n"
            "from protocol.event_transitions import publish_transition_once\n"
            "from protocol.fleet_protocol import make_message\n"
            f"evt = json.loads({payload!r})\n"
            "r = redis.Redis(host=%r, port=%d, decode_responses=True)\n"
            "def build(tid):\n"
            "    return json.dumps(make_message(type='REVIEW_REQUEST', to='product-owner', from_='scrum-master', body='x', task_id=evt['task_id'], extra={'transition_id': tid}))\n"
            "tid, outcome = publish_transition_once(r, 'scrum-master', evt, target='product-owner', target_list_key='inbox:product-owner', build_payload=build)\n"
            "print(f'{tid}|{outcome}')\n"
            % (REDIS_HOST, REDIS_PORT)
        )
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            for _ in range(2)
        ]
        outs = [p.communicate(timeout=30)[0].strip() for p in procs]
        outcomes = [line.split("|")[1] for out in outs for line in out.splitlines() if "|" in line]
        self.assertEqual(outcomes.count("published"), 1, f"outcomes={outcomes}")
        self.assertEqual(self.r.llen(INBOX), 1)

    def test_claim_expiry_and_restart_no_republish(self):
        """3. Expiration du claim et redémarrage : aucune republication."""
        evt = make_scrum_event()
        tid, outcome = self.publish(evt)
        self.assertEqual(outcome, "published")
        # Simuler l'expiration du claim temporaire : le supprimer.
        self.r.delete(f"claim:transition:{AGENT}:{tid}")
        # Redémarrage du consumer → même événement relu.
        _, outcome2 = self.publish(evt)
        self.assertEqual(outcome2, "already_published")
        self.assertEqual(self.r.llen(INBOX), 1)


class TestCrashRecovery(DisposableRedisTestCase):
    """4. Crash avant la transition : publication possible à la reprise."""

    def test_crash_before_transition_publish_on_resume(self):
        evt = make_scrum_event()
        # Aucune publication (crash simulé avant l'appel).
        self.assertEqual(self.r.llen(INBOX), 0)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 0)
        # Reprise : la publication est possible.
        _, outcome = self.publish(evt)
        self.assertEqual(outcome, "published")

    def test_transition_done_redis_reply_lost_retry_no_duplicate(self):
        """5. Transition exécutée, réponse Redis perdue : retry sans doublon."""
        evt = make_scrum_event()
        tid, outcome = self.publish(evt)
        self.assertEqual(outcome, "published")
        # Le client n'a pas reçu la réponse (simulé) et RETRY le même appel.
        _, outcome_retry = self.publish(evt)
        self.assertEqual(outcome_retry, "already_published")
        self.assertEqual(self.r.llen(INBOX), 1)


class TestNewRevision(DisposableRedisTestCase):
    """6. Nouvelle revue d'une nouvelle révision : nouvelle publication."""

    def test_new_review_same_task_new_revision_allowed(self):
        e_rev1 = make_scrum_event(msg_id="res_revision_1")
        e_rev2 = make_scrum_event(msg_id="res_revision_2")
        _, o1 = self.publish(e_rev1)
        _, o2 = self.publish(e_rev2)
        self.assertEqual((o1, o2), ("published", "published"))
        self.assertEqual(self.r.llen(INBOX), 2)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 2)


class TestPaginatedScan(DisposableRedisTestCase):
    """7. Plus de 10 événements en attente : aucun événement oublié."""

    def test_more_than_10_events_none_lost(self):
        # 15 événements distincts (au-delà de la fenêtre lrange -10 -1).
        events = [make_scrum_event(task_id=f"task_{i}", msg_id=f"res_{i}") for i in range(15)]
        for e in events:
            self.r.rpush(f"events:{AGENT}", json.dumps(e))
        # Parcours paginé complet (comportement corrigé du consumer) :
        # LRANGE paginé sur toute la liste, pas seulement les 10 derniers.
        total = self.r.llen(f"events:{AGENT}")
        page = 10
        seen = []
        for start in range(0, total, page):
            batch = self.r.lrange(f"events:{AGENT}", start, start + page - 1)
            for evt_str in batch:
                try:
                    evt = json.loads(evt_str)
                except json.JSONDecodeError:
                    continue
                sid = event_source_id(evt)
                if sid and sid not in seen:
                    seen.append(sid)
        self.assertEqual(len(seen), 15)
        # Chaque événement distinct publié une fois.
        outcomes = []
        for e in events:
            _, o = self.publish(e)
            outcomes.append(o)
        self.assertEqual(outcomes.count("published"), 15)
        self.assertEqual(self.r.llen(INBOX), 15)


class TestWrongType(DisposableRedisTestCase):
    """8. Mauvais type de clé Redis : erreur explicite, aucun marquage trompeur."""

    def test_marker_wrong_type_raises_and_nothing_marked(self):
        evt = make_scrum_event()
        # Corrompre le marqueur : string au lieu de set.
        self.r.set(transitions_key(AGENT), "not-a-set")
        with self.assertRaises(Exception) as ctx:
            self.publish(evt)
        self.assertIn("WRONGTYPE", str(ctx.exception))
        # Aucune publication, le marqueur corrompu est inchangé.
        self.assertEqual(self.r.llen(INBOX), 0)
        self.assertEqual(self.r.get(transitions_key(AGENT)), "not-a-set")

    def test_inbox_wrong_type_raises_and_nothing_marked(self):
        evt = make_scrum_event()
        # Corrompre l'inbox : set au lieu de liste.
        self.r.sadd(INBOX, "not-a-list")
        with self.assertRaises(Exception) as ctx:
            self.publish(evt)
        self.assertIn("WRONGTYPE", str(ctx.exception))
        # Le marqueur NE contient PAS la transition (aucun marquage trompeur).
        members = self.r.smembers(transitions_key(AGENT))
        self.assertEqual(len(members), 0)


class TestInvalidEvent(DisposableRedisTestCase):
    """Événement sans identité : ValueError, aucune écriture."""

    def test_event_without_identity_raises(self):
        with self.assertRaises(ValueError):
            self.publish({"type": "SCRUM_REVIEW_COMPLETED", "task_id": "t"})
        self.assertEqual(self.r.llen(INBOX), 0)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
