#!/usr/bin/env python3
"""
test_migration_inventory.py — Tests ciblés de la migration.

Redis jetable démarré/arrêté par le dispositif de test ; registre en
fixture temporaire (jamais /opt/fleet/registry). Les fixtures contiennent
de VRAIS champs de routage (to, type, task_id, id/in_reply_to) : leur
absence n'est jamais une preuve suffisante.

Cas exigés :
- Source res_rev1, preuve liée uniquement à res_rev10 → UNKNOWN (aucune
  preuve retenue — interdiction du rapprochement par sous-chaîne).
- Même tâche, deux révisions, preuve uniquement pour la première :
  la seconde reste UNKNOWN.
- Fichier product-owner_*.json mais message.to == testeur → UNKNOWN.
- Fichier dont le nom contient task_id mais contenu sans rapport → UNKNOWN.
- Attribution dev-web mais preuve destinée à dev-back → UNKNOWN.
- Preuve exacte avec bon destinataire → DONE.
- Présence d'un UNKNOWN : vérification préalable en échec.
- Toutes les transitions réconciliées : vérification réussie, sans service.
- Rapport {} refusé ; statut non reconnu refusé ; compteurs incohérents
  refusés ; inventaire vide cohérent accepté.
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

    # ── Fixtures avec vrais champs de routage ────────────────────────────────

    def _registry_file(self, name: str, msg: dict, entry_extra: dict | None = None):
        """Écrit une entrée de registre avec un message COMPLET (routage inclus)."""
        entry = {"message": msg}
        if entry_extra:
            entry.update(entry_extra)
        with open(os.path.join(self._tmpdir, name), "w") as f:
            json.dump(entry, f)

    def _scrum_event(self, task_id, msg_id):
        return {
            "type": "SCRUM_REVIEW_COMPLETED",
            "task_id": task_id,
            "msg_id": msg_id,
            "reviewed_at": "2026-09-18T09:52:32Z",
            "success": True,
        }

    def _assign_event(self, task_id, msg_id, assigned_to):
        return {
            "type": "TASK_ASSIGNED",
            "task_id": task_id,
            "msg_id": msg_id,
            "assigned_to": assigned_to,
        }

    def _review_msg(self, task_id, source_msg_id, to="product-owner"):
        """Message historique complet : routage + liaison source exacte."""
        return {
            "id": f"scrum_to_po_{task_id}_{source_msg_id}",
            "type": "REVIEW_REQUEST",
            "to": to,
            "from": AGENT,
            "task_id": task_id,
        }

    def _assign_msg(self, task_id, source_msg_id, to):
        return {
            "id": f"scrum_to_{to}_{task_id}_{source_msg_id}",
            "type": "TICKET_ASSIGN",
            "to": to,
            "from": AGENT,
            "task_id": task_id,
        }

    def _by_task(self, report, task_id, msg_id):
        for t in report["transitions"]:
            if t["task_id"] == task_id and t["msg_id"] == msg_id:
                return t
        raise AssertionError(f"transition not found: {task_id}/{msg_id}")

    # ── 1. Identité source : pas de sous-chaîne ──────────────────────────────

    def test_substring_match_rejected_res_rev1_vs_res_rev10(self):
        """Source res_rev1, preuve liée uniquement à res_rev10 → UNKNOWN."""
        evt = self._scrum_event("task_sub", "res_rev1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        # Preuve pour la révision 10 (id contient res_rev10, PAS res_rev1
        # en tant que liaison exacte). L'ancien code aurait matché par
        # sous-chaîne : interdit désormais.
        self._registry_file(
            "product-owner_task_sub.json",
            self._review_msg("task_sub", "res_rev10"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_sub", "res_rev1")
        self.assertEqual(t["status"], "UNKNOWN",
                         "substring match res_rev1 in res_rev10 must NOT be evidence")
        self.assertIsNone(t["evidence"])

    def test_two_revisions_evidence_only_for_first(self):
        """Même tâche, deux révisions, preuve uniquement pour la première :
        la seconde reste UNKNOWN."""
        e1 = self._scrum_event("task_x", "res_rev1")
        e2 = self._scrum_event("task_x", "res_rev2")
        self.r.rpush(f"events:{AGENT}", json.dumps(e1))
        self.r.rpush(f"events:{AGENT}", json.dumps(e2))
        self._registry_file(
            "product-owner_task_x_rev1.json",
            self._review_msg("task_x", "res_rev1"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t1 = self._by_task(report, "task_x", "res_rev1")
        t2 = self._by_task(report, "task_x", "res_rev2")
        self.assertEqual(t1["status"], "DONE")
        self.assertIsNotNone(t1["evidence"])
        self.assertEqual(t2["status"], "UNKNOWN", "revision 2 must stay UNKNOWN")
        self.assertIsNotNone(t2["reason"])

    # ── 2. Destinataire vérifié dans le contenu ──────────────────────────────

    def test_po_filename_but_message_to_testeur_unknown(self):
        """Fichier product-owner_*.json mais message.to == testeur → UNKNOWN."""
        evt = self._scrum_event("task_rcpt", "res_rcpt1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        # Le NOM dit product-owner, le CONTENU dit testeur : contenu gagne.
        self._registry_file(
            "product-owner_task_rcpt.json",
            self._review_msg("task_rcpt", "res_rcpt1", to="testeur"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_rcpt", "res_rcpt1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_recipient_absent_unknown(self):
        """Message sans champ to (destinataire absent) → UNKNOWN."""
        evt = self._scrum_event("task_noto", "res_noto1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        msg = self._review_msg("task_noto", "res_noto1")
        del msg["to"]  # destinataire absent
        self._registry_file("product-owner_task_noto.json", msg)
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_noto", "res_noto1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_filename_contains_task_id_but_unrelated_content(self):
        """Fichier dont le nom contient task_id mais contenu sans rapport :
        aucune preuve retenue."""
        evt = self._scrum_event("task_y", "res_y1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        self._registry_file(
            "product-owner_task_y_decoy.json",
            self._review_msg("task_UNRELATED", "res_other"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_y", "res_y1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_evidence_for_other_recipient_not_retained(self):
        """Preuve pour un autre destinataire : aucune preuve retenue."""
        evt = self._scrum_event("task_z", "res_z1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        self._registry_file(
            "testeur_task_z_wrong_recipient.json",
            self._review_msg("task_z", "res_z1", to="testeur"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_z", "res_z1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_assign_dev_web_but_proof_for_dev_back_unknown(self):
        """Attribution dev-web mais preuve destinée à dev-back → UNKNOWN."""
        evt = self._assign_event("task_assign1", "res_asg1", "dev-web")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        # Preuve d'attribution correcte en apparence mais to == dev-back.
        self._registry_file(
            "dev-back_task_assign1.json",
            self._assign_msg("task_assign1", "res_asg1", to="dev-back"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_assign1", "res_asg1")
        self.assertEqual(t["status"], "UNKNOWN")
        self.assertIsNone(t["evidence"])

    def test_exact_evidence_correct_recipient_done(self):
        """Preuve exacte avec bon destinataire → DONE."""
        evt = self._scrum_event("task_ok", "res_ok1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        self._registry_file(
            "product-owner_task_ok.json",
            self._review_msg("task_ok", "res_ok1", to="product-owner"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_ok", "res_ok1")
        self.assertEqual(t["status"], "DONE")
        self.assertIsNotNone(t["evidence"])

    def test_assign_exact_evidence_done(self):
        """Attribution dev-web avec preuve exacte to == dev-web → DONE."""
        evt = self._assign_event("task_asg_ok", "res_asg_ok1", "dev-web")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        self._registry_file(
            "dev-web_task_asg_ok.json",
            self._assign_msg("task_asg_ok", "res_asg_ok1", to="dev-web"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_asg_ok", "res_asg_ok1")
        self.assertEqual(t["status"], "DONE")
        self.assertIsNotNone(t["evidence"])

    def test_in_reply_to_exact_link_done(self):
        """Liaison protocolaire in_reply_to exacte → DONE."""
        evt = self._scrum_event("task_irt", "res_irt1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        self._registry_file(
            "product-owner_task_irt.json",
            {
                "id": "msg_arbitrary_123",
                "type": "REVIEW_REQUEST",
                "to": "product-owner",
                "from": AGENT,
                "task_id": "task_irt",
                "in_reply_to": "res_irt1",
            },
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_irt", "res_irt1")
        self.assertEqual(t["status"], "DONE")

    # ── 3. Vérification de reprise : structure ───────────────────────────────

    def test_unknown_present_resume_check_fails(self):
        """Présence d'un UNKNOWN : vérification préalable en échec."""
        e1 = self._scrum_event("task_a", "res_a1")
        e2 = self._scrum_event("task_b", "res_b1")  # aucune preuve
        self.r.rpush(f"events:{AGENT}", json.dumps(e1))
        self.r.rpush(f"events:{AGENT}", json.dumps(e2))
        self._registry_file(
            "product-owner_task_a.json",
            self._review_msg("task_a", "res_a1"),
        )
        report = build_inventory(self.r, AGENT, self._tmpdir)
        ok, unknowns = verify_resume_ready(report, expected_agent=AGENT)
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
        self._registry_file("product-owner_task_c.json", self._review_msg("task_c", "res_c1"))
        self._registry_file("product-owner_task_d.json", self._review_msg("task_d", "res_d1"))
        report = build_inventory(self.r, AGENT, self._tmpdir)
        self.assertEqual(report["counts"], {"DONE": 2, "UNKNOWN": 0})
        ok, unknowns = verify_resume_ready(report, expected_agent=AGENT)
        self.assertTrue(ok, "resume check must SUCCEED when all DONE")
        self.assertEqual(unknowns, [])

    def test_empty_report_rejected(self):
        """Un rapport {} doit être refusé."""
        ok, problems = verify_resume_ready({})
        self.assertFalse(ok)
        reasons = [p.get("reason") for p in problems]
        self.assertIn("report_missing_or_empty", reasons)

    def test_none_report_rejected(self):
        ok, problems = verify_resume_ready(None)
        self.assertFalse(ok)
        self.assertIn("report_missing_or_empty", [p.get("reason") for p in problems])

    def test_invalid_status_rejected(self):
        """Statut absent ou non reconnu → refus."""
        report = {
            "agent": AGENT,
            "transitions": [
                {"task_id": "t1", "status": "NEEDED", "transition_id": None,
                 "source_id": None, "evidence": None},
                {"task_id": "t2"},  # statut absent
            ],
            "counts": {"DONE": 0, "UNKNOWN": 0},
        }
        ok, problems = verify_resume_ready(report, expected_agent=AGENT)
        self.assertFalse(ok)
        reasons = [p.get("reason") for p in problems]
        self.assertTrue(any("invalid_status" in r for r in reasons), reasons)

    def test_counts_mismatch_rejected(self):
        """Compteurs incohérents avec la liste → refus."""
        report = {
            "agent": AGENT,
            "transitions": [
                {"task_id": "t1", "status": "DONE", "transition_id": "tid",
                 "source_id": "sid", "evidence": {"kind": "registry"}},
            ],
            "counts": {"DONE": 5, "UNKNOWN": 0},  # 5 ≠ 1 réel
        }
        ok, problems = verify_resume_ready(report, expected_agent=AGENT)
        self.assertFalse(ok)
        self.assertTrue(any("counts_mismatch" in p.get("reason", "") for p in problems))

    def test_done_missing_identity_rejected(self):
        """DONE sans transition_id/source_id/evidence → refus."""
        report = {
            "agent": AGENT,
            "transitions": [
                {"task_id": "t1", "status": "DONE", "transition_id": None,
                 "source_id": None, "evidence": None},
            ],
            "counts": {"DONE": 1, "UNKNOWN": 0},
        }
        ok, problems = verify_resume_ready(report, expected_agent=AGENT)
        self.assertFalse(ok)
        reasons = [p.get("reason") for p in problems]
        self.assertTrue(any("done_missing" in r for r in reasons), reasons)

    def test_agent_mismatch_rejected(self):
        """Agent attendu différent → refus."""
        report = {
            "agent": "other-agent",
            "transitions": [],
            "counts": {"DONE": 0, "UNKNOWN": 0},
        }
        ok, problems = verify_resume_ready(report, expected_agent=AGENT)
        self.assertFalse(ok)
        self.assertTrue(any("agent_mismatch" in p.get("reason", "") for p in problems))

    def test_empty_inventory_coherent_accepted(self):
        """Inventaire réellement vide mais complet et cohérent → accepté."""
        report = {
            "agent": AGENT,
            "transitions": [],
            "counts": {"DONE": 0, "UNKNOWN": 0},
        }
        ok, unknowns = verify_resume_ready(report, expected_agent=AGENT)
        self.assertTrue(ok)
        self.assertEqual(unknowns, [])

    def test_transition_id_evidence_preferred(self):
        """transition_id présent dans une preuve : rapprochement direct."""
        evt = self._scrum_event("task_e", "res_e1")
        self.r.rpush(f"events:{AGENT}", json.dumps(evt))
        report0 = build_inventory(self.r, AGENT, self._tmpdir)
        tid = self._by_task(report0, "task_e", "res_e1")["transition_id"]
        self.r.rpush("inbox:product-owner", json.dumps({
            "task_id": "task_e", "type": "REVIEW_REQUEST", "to": "product-owner",
            "transition_id": tid, "id": "msg_anything",
        }))
        report = build_inventory(self.r, AGENT, self._tmpdir)
        t = self._by_task(report, "task_e", "res_e1")
        self.assertEqual(t["status"], "DONE")
        self.assertEqual(t["evidence"]["match"], "transition_id")


if __name__ == "__main__":
    unittest.main(verbosity=2)
