#!/usr/bin/env python3
import os, sys, time, json, tempfile, unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

redis_url = os.environ.get("FLEET_TEST_REDIS_URL")
redis_pass = os.environ.get("FLEET_TEST_REDIS_PASS")
redis_port = os.environ.get("FLEET_TEST_REDIS_PORT")
if not redis_url:
    raise RuntimeError(
        "FLEET_TEST_REDIS_URL requis : utiliser un Redis de test isolé"
    )

class TestClaimLossInvariants(unittest.TestCase):
    def setUp(self):
        import redis
        pool = redis.ConnectionPool(host=redis_url, port=redis_port,
            password=redis_pass,
            decode_responses=True, socket_connect_timeout=5)
        self.r = redis.Redis(connection_pool=pool)
        self.witness_dir = tempfile.mkdtemp(prefix='fleet_test_')
        # Nettoyage TRÈS agressif de toutes les clés de test
        local_ckpt_dir = Path('/var/log/fleet/checkpoints')
        if local_ckpt_dir.exists():
            import shutil
            shutil.rmtree(local_ckpt_dir, ignore_errors=True)
        for pattern in ['claim:t_*', 'result:t_*', 'msg:done:t_*', 'checkpoints:t_*']:
            cur = 0
            while True:
                cur, keys = self.r.scan(cur, match=pattern, count=100)
                if keys: self.r.delete(*keys)
                if cur == 0: break

    def tearDown(self):
        import shutil
        shutil.rmtree(self.witness_dir, ignore_errors=True)
        local_ckpt_dir = Path('/var/log/fleet/checkpoints')
        if local_ckpt_dir.exists():
            shutil.rmtree(local_ckpt_dir, ignore_errors=True)
        for pattern in ['claim:t_*', 'result:t_*', 'msg:done:t_*', 'checkpoints:t_*']:
            cur = 0
            while True:
                cur, keys = self.r.scan(cur, match=pattern, count=100)
                if keys: self.r.delete(*keys)
                if cur == 0: break

    def test_extend_never_recreates_claim(self):
        from launcher.fleet_launcher import TaskClaim
        tid = 't_ext_001'
        claim = TaskClaim(self.r, 'ag', tid, ttl=2)
        self.assertTrue(claim.acquire())
        time.sleep(3)
        self.assertFalse(claim.extend())
        self.assertFalse(self.r.exists(f'claim:{tid}'))

    def test_try_publish_result_rejects_when_not_owner(self):
        from launcher.fleet_launcher import TaskClaim
        tid = 't_pub_001'
        claim = TaskClaim(self.r, 'ag', tid, ttl=30)
        self.assertTrue(claim.acquire())
        self.assertTrue(claim.try_publish_result({'data': 'ok'}))
        self.r.set(f'claim:{tid}', 'stolen', ex=30)
        self.assertFalse(claim.try_publish_result({'data': 'rejected'}))

    def test_publish_checkpoint_does_not_create_result_key(self):
        from launcher.fleet_launcher import TaskClaim
        tid = 't_ckpt_001'
        claim = TaskClaim(self.r, 'ag', tid, ttl=30)
        self.assertTrue(claim.acquire())
        claim.publish_checkpoint({'output': 'checkpointed'})
        self.assertFalse(self.r.exists(f'result:{tid}'))
        self.assertFalse(self.r.exists(f'msg:done:{tid}'))
        ckpt_dir = Path('/var/log/fleet/checkpoints') / tid
        self.assertTrue(ckpt_dir.exists())
        files = list(ckpt_dir.glob('*.json'))
        self.assertEqual(len(files), 1)
        data = json.loads(files[0].read_text())
        self.assertEqual(data.get('attempt_id'), claim.attempt_id)

    def test_separate_traces_per_attempt(self):
        from launcher.fleet_launcher import TaskClaim
        tid = 't_trace_001'
        claim1 = TaskClaim(self.r, 'ag', tid, ttl=30)
        self.assertTrue(claim1.acquire())
        claim1.publish_checkpoint({'output': 'first'})
        time.sleep(1.1)
        claim2 = TaskClaim(self.r, 'ag', tid, ttl=30)
        self.r.set(f'claim:{tid}', claim2.attempt_id, ex=30)
        claim2.publish_checkpoint({'output': 'second'})
        ckpt_dir = Path('/var/log/fleet/checkpoints') / tid
        files = list(ckpt_dir.glob('*.json'))
        self.assertEqual(len(files), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
