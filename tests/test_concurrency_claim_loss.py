#!/usr/bin/env python3
import os, sys, time, json, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

redis_url = os.environ.get("FLEET_TEST_REDIS_URL")
redis_pass = os.environ.get("FLEET_TEST_REDIS_PASS")
redis_port = os.environ.get("FLEET_TEST_REDIS_PORT")

class TestClaimLoss(unittest.TestCase):
    def setUp(self):
        import redis
        pool = redis.ConnectionPool(host=redis_url, port=redis_port,
            password=redis_pass,
            decode_responses=True, socket_connect_timeout=5)
        self.r = redis.Redis(connection_pool=pool)
        for p in ['claim:test_*','result:test_*','msg:done:test_*']:
            cur = 0
            while True:
                cur, keys = self.r.scan(cur, match=p, count=100)
                if keys: self.r.delete(*keys)
                if cur == 0: break

    def test_extend_does_not_recreate_expired_claim(self):
        """extend() ne doit PAS recréer un claim expiré."""
        from launcher.fleet_launcher import TaskClaim
        tid = 'test_claim_001'
        claim = TaskClaim(self.r, 'ag', tid, ttl=2)
        self.assertTrue(claim.acquire())
        time.sleep(3)
        self.assertFalse(self.r.exists(f'claim:{tid}'))
        self.assertFalse(claim.extend())
        self.assertFalse(self.r.exists(f'claim:{tid}'))

    def test_claim_loss_detected_by_heartbeat(self):
        """Le heartbeat détecte la perte."""
        from launcher.fleet_launcher import TaskClaim
        tid = 'test_claim_002'
        claim = TaskClaim(self.r, 'ag', tid, ttl=30)
        self.assertTrue(claim.acquire())
        self.r.set(f'claim:{tid}', 'stolen', ex=30)
        self.assertFalse(claim.is_owner)
        self.assertFalse(claim.extend())

if __name__ == '__main__':
    unittest.main(verbosity=2)
