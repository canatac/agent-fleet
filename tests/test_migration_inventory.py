#!/usr/bin/env python3
"""
test_migration_inventory.py — Tests ciblés de la migration.

Redis jetable démarré/arrêté par le dispositif de test ; registre en
fixture temporaire (jamais /opt/fleet/registry).

Cas exigés :
- Même tâche, deux révisions, preuve uniquement pour la première :
  la seconde reste UNKNOWN.
- Fichier dont le nom contient task_id mais contenu sans rapport :
  aucune preuve retenue.
- Preuve pour un autre destinataire : aucune preuve retenue.
- Présence d'un UNKNOWN : vérification préalable en échec.
- Toutes les transitions réconciliées : vérification préalable réussie,
  sans démarrer de service.
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tests.redis_fixture as fixture
from migration.migration_transitions_inventory import (
    build_inventory,
    verify_resume_ready,
)

AGENT = "scrum-master"


class TestMigrationInventory(unittest.TestCase):

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
        self.r.flushdb()
        self._tmpdir = tempfile.mkdtemp(prefix="fleet-migration-test-")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _registry_file(self, name: str, content: dict):
        with open(os.path.join(self._tmpdir, name), "w") as f:
            json.dump(content, f)

    def _scrum_event(self, task_id, msg_id):
        return {
            "type": "SCRUM_REVIEW_COMPLETED",
            "task_id": task_id,
            "msg_id": msg_id,
            "reviewed_at": "2026-09-18T09:52:32Z",
            "success": True,
        }

    def _by_task(self, report, task_id, msg_id):
        for t in report["transitions"]:
            if t["task_id"] == task_id and t["msg_id"] == msg_id:
                return t
        raise AssertionError(f"transition not found: {task_id}/{msg_id}")

    def test_two_revisions_evidence_only_for_first(self):
        """Même tâche, deux révisions, preuve uniquement pour la première :
        la seconde reste UNKNOWN."""
        e1 = self._scrum_event("task_x", "res_rev1")
        e2 = self._scrum_event("task_x", "res_rev2")
        self.r.rpush(f"events:{AGENT}", json.dumps(e1))
        self.r.rpush(f"events:{AGENT}", json.dumps(e2))
        # Preuve de consommation pour la révision 1 UNIQUEMENT (liaison
        # msg_id source dans le contenu, pas le nom du fichier).
        self._registry_file("product-owner_task_x_rev1.json", {
            "task_id": "task_x",
            "msg_id": "res_rev1",
            "message": {"task_id": "task_x", "type": "REVIEW_REQUEST",
                        "id": "scrum_to_po_task_x_res_rev1"},
        })
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t1 = self._by_task(report, "task_x", "res_rev1")
        t2 = self._by_task(report, "task_x", "res_rev2")
        self.assertEqual(t1["status"], "DONE")
        self.assertIsNotNone(t1["evidence"])
        self.assertEqual(t2["status"], "UNKNOWN", "revision 2 must stay UNKNOWN")
        self.assertIsNotNone(t2["reason"])

    def test_filename_contains_task_id_but_unrelated_content(self):
        """Fichier dont le nom contient task_id mais contenu sans rapport :
        aucune preuve retenue."""
        evt = self._scrum_event("task_y", "res_y1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        # Le nom contient task_y, mais le contenu parle d'une AUTRE tâche.
        self._registry_file("product-owner_task_y_decoy.json", {
            "task_id": "task_UNRELATED",
            "msg_id": "res_other",
            "message": {"task_id": "task_UNRELATED", "type": "REVIEW_REQUEST",
                        "id": "scrum_to_po_task_UNRELATED"},
        })
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_y", "res_y1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_evidence_for_other_recipient_not_retained(self):
        """Preuve pour un autre destinataire : aucune preuve retenue."""
        evt = self._scrum_event("task_z", "res_z1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        # Contenu exact pour task_z, mais fichier d'un AUTRE destinataire
        # (pas product-owner_) : la cible de la transition est le PO.
        self._registry_file("testeur_task_z_wrong_recipient.json", {
            "task_id": "task_z",
            "msg_id": "res_z1",
            "message": {"task_id": "task_z", "type": "REVIEW_REQUEST",
                        "id": "scrum_to_testeur_task_z_res_z1"},
        })
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_z", "res_z1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_unknown_present_resume_check_fails(self):
        """Présence d'un UNKNOWN : vérification préalable en échec."""
        e1 = self._scrum_event("task_a", "res_a1")
        e2 = self._scrum_event("task_b", "res_b1")  # aucune preuve
        self.r.rpush(f"events:{AGENT}", json.dumps(e1))
        self.r.rpush(f"events:{AGENT}", json.dumps(e2))
        self._registry_file("product-owner_task_a.json", {
            "task_id": "task_a",
            "msg_id": "res_a1",
            "message": {"task_id": "task_a", "type": "REVIEW_REQUEST",
                        "id": "scrum_to_po_task_a_res_a1"},
        })
        report = build_inventory(self.r, AGENT, self._tmpdir)
        ok, unknowns = verify_resume_ready(report)
        self.assertFalse(ok, "resume check must FAIL with an UNKNOWN present")
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0]["task_id"], "task_b")
        self.assertIsNotNone(unknowns[0]["reason"])

    def test_all_reconciled_resume_check_succeeds(self):
        """Toutes les transitions réconciliées : vérification réussie,
        sans démarrer de service (fonction pure sur le rapport)."""
        e1 = self._scrum_event("task_c", "res_c1")
        e2 = self._scrum_event("task_d", "res_d1")
        self.r.rpush(f"events:{AGENT}", json.dumps(e1))
        self.r.rpush(f"events:{AGENT}", json.dumps(e2))
        self._registry_file("product-owner_task_c.json", {
            "task_id": "task_c",
            "msg_id": "res_c1",
            "message": {"task_id": "task_c", "type": "REVIEW_REQUEST",
                        "id": "scrum_to_po_task_c_res_c1"},
        })
        self._registry_file("product-owner_task_d.json", {
            "task_id": "task_d",
            "msg_id": "res_d1",
            "message": {"task_id": "task_d", "type": "REVIEW_REQUEST",
                        "id": "scrum_to_po_task_d_res_d1"},
        })
        report = build_inventory(self.r, AGENT, self._tmpdir)
        self.assertEqual(report["counts"], {"DONE": 2, "UNKNOWN": 0})
        ok, unknowns = verify_resume_ready(report)
        self.assertTrue(ok, "resume check must SUCCEED when all DONE")
        self.assertEqual(unknowns, [])

    def test_transition_id_evidence_preferred(self):
        """transition_id présent dans une preuve : rapprochement direct."""
        evt = self._scrum_event("task_e", "res_e1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        # D'abord calculer le transition_id attendu via build_inventory
        # (preuve absente → UNKNOWN), puis l'injecter dans une preuve inbox.
        report0 = build_inventory(self.r, AGENT, self._tmpdir)
        tid = self._by_task(report0, "task_e", "res_e1")["transition_id"]
        self.r.rpush("inbox:product-owner", json.dumps({
            "task_id": "task_e", "type": "REVIEW_REQUEST",
            "transition_id": tid, "id": "msg_anything",
        }))
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_e", "res_e1")
        self.assertEqual(t["status"], "DONE")
        self.assertEqual(t["evidence"]["match"], "transition_id")


if __name__ == "__main__":
    unittest.main(verbosity=2)
