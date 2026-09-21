#!/usr/bin/env python3
"""
test_heartbeat.py — TDD pour le mécanisme de heartbeat claim.
"""

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from launcher.fleet_launcher import TaskClaim

redis_url = os.environ.get("FLEET_TEST_REDIS_URL")
redis_pass = os.environ.get("FLEET_TEST_REDIS_PASS")
redis_port = os.environ.get("FLEET_TEST_REDIS_PORT")


class TestHeartbeatBasic(unittest.TestCase):

    def setUp(self):
        import redis
        self.pool = redis.ConnectionPool(
            host=redis_url, port=redis_port,
            password=redis_pass,
            decode_responses=True, socket_connect_timeout=5,
        )
        self.r = redis.Redis(connection_pool=self.pool)
        self.r.ping()

    def tearDown(self):
        cursor = 0
        while True:
            cursor, keys = self.r.scan(cursor, match='claim:test_hb_*', count=100)
            if keys:
                self.r.delete(*keys)
            if cursor == 0:
                break

    def test_claim_acquired_and_extendable(self):
        """
        ÉTANT DONNÉ un claim acquis avec TTL 5s
        ET un extend appelé toutes les 2s pendant 10s
        ALORS après 10s, le claim existe toujours
        """
        claim = TaskClaim(self.r, 'test-agent', 'task_hb_001', ttl=5)
        self.assertTrue(claim.acquire())

        for i in range(5):
            time.sleep(2)
            extended = claim.extend()
            self.assertTrue(extended, f"Extend {i+1} a échoué")
            ttl = self.r.ttl(claim._key)
            self.assertGreater(ttl, 0, f"TTL invalide après extend {i+1}: {ttl}")

        self.assertTrue(self.r.exists(claim._key), "Le claim a expiré malgré heartbeat")
        claim.release()

    def test_heartbeat_stops_when_claim_stolen(self):
        """
        ÉTANT DONNÉ un claim acquis
        ET un autre processus vole le claim
        ALORS extend retourne False
        """
        claim = TaskClaim(self.r, 'test-agent', 'task_hb_002', ttl=30)
        self.assertTrue(claim.acquire())

        self.r.set(claim._key, 'stolen_by_other', ex=30)

        extended = claim.extend()
        self.assertFalse(extended, "Extend devrait échouer après vol")
        self.assertFalse(claim.is_owner)

    def test_claim_expires_without_heartbeat(self):
        """
        ÉTANT DONNÉ un claim acquis avec TTL 2s
        ET pas de heartbeat
        ALORS après 3s, le claim n'existe plus
        """
        claim = TaskClaim(self.r, 'test-agent', 'task_hb_003', ttl=2)
        self.assertTrue(claim.acquire())

        time.sleep(3)
        self.assertFalse(self.r.exists(claim._key), "Le claim devrait avoir expiré")


if __name__ == '__main__':
    unittest.main(verbosity=2)
