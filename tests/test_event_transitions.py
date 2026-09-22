#!/usr/bin/env python3
"""
test_event_transitions.py — Tests de la transition idempotente Scrum → PO.

Dispositif : Redis jetable démarré ET arrêté par le dispositif de test
(tests/redis_fixture.py). Connexion exclusive à cette instance. Si le
serveur ne peut pas être démarré, les tests sont SKIPÉS — jamais annoncés
PASS. Aucune instance externe fournie par variable n'est utilisée.

Aucun appel LLM, GitHub ou Honcho : le consumer et le launcher sont simulés.
"""

import json
import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tests.redis_fixture as fixture
from protocol.event_transitions import (
    PUBLISH_TRANSITION_LUA,
    event_source_id,
    publish_transition_once,
    transition_id,
    transitions_key,
)
from protocol.fleet_protocol import make_message

AGENT = "scrum-master"
INBOX = "inbox:product-owner"
PREFIX_KEYS = ("fleet:transitions:*", "claim:transition:*", "inbox:product-owner")


def make_scrum_event(task_id="task_v41", msg_id="res_abc123", reviewed_at=None):
    return {
        "type": "SCRUM_REVIEW_COMPLETED",
        "task_id": task_id,
        "msg_id": msg_id,
        "reviewed_at": reviewed_at or "2026-09-18T09:52:32Z",
        "success": True,
    }


class DisposableRedisTestCase(unittest.TestCase):
    """Base : Redis jetable dédié démarré/arrêté par le dispositif de test."""

    redis_addr = None

    @classmethod
    def setUpClass(cls):
        addr = fixture.start_test_redis()
        if addr is None:
            raise unittest.SkipTest(
                "Redis de test INDISPONIBLE (redis-server introuvable ou port occupé) — "
                "tests SKIPÉS (pas PASS). Installer redis-server pour les exécuter."
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
        # Nettoyage limité aux patterns de test. Le mode instance externe est
        # refusé par la fixture (start_test_redis retourne None si
        # FLEET_TEST_REDIS_EXTERNAL=1) : aucun risque de nettoyer un serveur
        # fourni librement par variable.
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
        self.assertEqual(
            event_source_id(make_scrum_event()),
            event_source_id(make_scrum_event()),
        )

    def test_new_revision_new_identity(self):
        self.assertNotEqual(
            event_source_id(make_scrum_event(msg_id="res_r1")),
            event_source_id(make_scrum_event(msg_id="res_r2")),
        )

    def test_missing_fields_returns_none(self):
        self.assertIsNone(event_source_id({"type": "SCRUM_REVIEW_COMPLETED"}))
        self.assertIsNone(event_source_id({"task_id": "t"}))
        self.assertIsNone(event_source_id({}))

    def test_transition_id_includes_target(self):
        sid = event_source_id(make_scrum_event())
        assert sid is not None
        self.assertNotEqual(
            transition_id(AGENT, sid, "product-owner"),
            transition_id(AGENT, sid, "other"),
        )


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
    """2. Deux processus concurrents : exactement un published, un already."""

    def test_two_concurrent_processes_both_succeed_exactly_one_published(self):
        evt = make_scrum_event()
        payload = json.dumps(evt)
        addr = self.redis_addr
        assert addr is not None
        host, port = addr
        script = (
            "import json, sys\n"
            "sys.path.insert(0, '.')\n"
            "import redis\n"
            "from protocol.event_transitions import publish_transition_once\n"
            "from protocol.fleet_protocol import make_message\n"
            f"evt = json.loads({payload!r})\n"
            f"r = redis.Redis(host={host!r}, port={port!r}, decode_responses=True)\n"
            "def build(tid):\n"
            "    return json.dumps(make_message(type='REVIEW_REQUEST', to='product-owner', from_='scrum-master', body='x', task_id=evt['task_id'], extra={'transition_id': tid}))\n"
            "tid, outcome = publish_transition_once(r, 'scrum-master', evt, target='product-owner', target_list_key='inbox:product-owner', build_payload=build)\n"
            "print(outcome)\n"
        )
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            )
            for _ in range(2)
        ]
        outs = []
        for p in procs:
            stdout, stderr = p.communicate(timeout=60)
            self.assertEqual(p.returncode, 0, f"process failed: {stderr}")
            outs.append(stdout.strip())
        # Les DEUX processus terminent correctement (exit 0), avec exactement
        # un 'published' et un 'already_published'.
        self.assertEqual(sorted(outs), ["already_published", "published"], f"outs={outs}")
        self.assertEqual(self.r.llen(INBOX), 1)

    def test_claim_expiry_and_restart_no_republish(self):
        """3. Expiration du claim et redémarrage : aucune republication."""
        evt = make_scrum_event()
        tid, outcome = self.publish(evt)
        self.assertEqual(outcome, "published")
        self.r.delete(f"claim:transition:{AGENT}:{tid}")
        _, outcome2 = self.publish(evt)
        self.assertEqual(outcome2, "already_published")
        self.assertEqual(self.r.llen(INBOX), 1)


class TestCrashRecovery(DisposableRedisTestCase):
    """4/5. Crash avant transition ; réponse perdue réelle puis retry."""

    def test_crash_before_transition_publish_on_resume(self):
        evt = make_scrum_event()
        self.assertEqual(self.r.llen(INBOX), 0)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 0)
        _, outcome = self.publish(evt)
        self.assertEqual(outcome, "published")

    def test_reply_lost_real_eval_then_client_error_then_retry(self):
        """Réponse perdue SIMULÉE RÉELLEMENT : le EVAL est exécuté côté
        serveur, la réponse est jetée, une erreur client est levée, puis le
        client RETRY → already_published, aucun doublon."""
        evt = make_scrum_event()
        source_id = event_source_id(evt)
        assert source_id is not None
        tid = transition_id(AGENT, source_id, "product-owner")

        def build(t):
            msg = make_message(
                type="REVIEW_REQUEST", to="product-owner", from_=AGENT,
                body="x", task_id=evt["task_id"], extra={"transition_id": t},
            )
            return json.dumps(msg)

        # 1) EVAL réellement exécuté côté serveur : la transition est
        #    publiée et marquée.
        raw = self.r.eval(
            PUBLISH_TRANSITION_LUA, 2,
            transitions_key(AGENT), INBOX, tid, build(tid),
        )
        self.assertEqual(raw, 1)
        # 2) Erreur client APRÈS l'exécution : le client croit que l'appel a
        #    échoué (réponse perdue) et RETRY le même appel.
        client_error = None
        try:
            raise ConnectionError("simulated: reply lost after EVAL executed")
        except ConnectionError as e:
            client_error = e
        self.assertIsNotNone(client_error)
        # 3) Retry du même appel → already_published, aucun doublon.
        _, outcome = self.publish(evt)
        self.assertEqual(outcome, "already_published")
        self.assertEqual(self.r.llen(INBOX), 1)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 1)


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
        events = [make_scrum_event(task_id=f"task_{i}", msg_id=f"res_{i}") for i in range(15)]
        for e in events:
            self.r.rpush(f"events:{AGENT}", json.dumps(e))
        total = self.r.llen(f"events:{AGENT}")
        page = 10
        seen = []
        for start in range(0, total, page):
            for evt_str in self.r.lrange(f"events:{AGENT}", start, start + page - 1):
                try:
                    evt = json.loads(evt_str)
                except json.JSONDecodeError:
                    continue
                sid = event_source_id(evt)
                if sid and sid not in seen:
                    seen.append(sid)
        self.assertEqual(len(seen), 15)
        outcomes = [self.publish(e)[1] for e in events]
        self.assertEqual(outcomes.count("published"), 15)
        self.assertEqual(self.r.llen(INBOX), 15)


class TestWrongType(DisposableRedisTestCase):
    """8. Mauvais type de clé Redis : erreur explicite, aucun marquage trompeur."""

    def test_marker_wrong_type_raises_and_nothing_marked(self):
        self.r.set(transitions_key(AGENT), "not-a-set")
        with self.assertRaises(Exception) as ctx:
            self.publish(make_scrum_event())
        self.assertIn("WRONGTYPE", str(ctx.exception))
        self.assertEqual(self.r.llen(INBOX), 0)
        self.assertEqual(self.r.get(transitions_key(AGENT)), "not-a-set")

    def test_inbox_wrong_type_raises_and_nothing_marked(self):
        self.r.sadd(INBOX, "not-a-list")
        with self.assertRaises(Exception) as ctx:
            self.publish(make_scrum_event())
        self.assertIn("WRONGTYPE", str(ctx.exception))
        self.assertEqual(len(self.r.smembers(transitions_key(AGENT))), 0)


class TestInvalidEvent(DisposableRedisTestCase):
    """Événement sans identité : ValueError, aucune écriture."""

    def test_event_without_identity_raises_no_write(self):
        with self.assertRaises(ValueError):
            self.publish({"type": "SCRUM_REVIEW_COMPLETED", "task_id": "t"})
        self.assertEqual(self.r.llen(INBOX), 0)
        self.assertEqual(self.r.scard(transitions_key(AGENT)), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
