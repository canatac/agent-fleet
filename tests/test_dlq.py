#!/usr/bin/env python3
"""
test_dlq.py — TDD pour la Dead Letter Queue et la politique de tentatives.
"""

import os
import sys
import time
import json
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

redis_url = os.environ.get("FLEET_TEST_REDIS_URL")
redis_pass = os.environ.get("FLEET_TEST_REDIS_PASS")
redis_port = os.environ.get("FLEET_TEST_REDIS_PORT")


def mock_subprocess_run_factory():
    """Crée un mock qui retourne toujours exit=1 (échec)."""
    def fn(*args, **kwargs):
        result = MagicMock()
        result.returncode = 1
        result.stdout = ""
        result.stderr = "LAUNCH_ERROR: simulated failure"
        return result
    return fn


class TestDLQ(unittest.TestCase):

    def setUp(self):
        import redis
        self.pool = redis.ConnectionPool(
            host=redis_url, port=redis_port,
            password=redis_pass,
            decode_responses=True, socket_connect_timeout=5,
        )
        self.r = redis.Redis(connection_pool=self.pool)
        self.r.ping()
        self._cleanup_all()

    def _cleanup_all(self):
        for pattern in ['dlq:*', 'queue:*', 'attempt:*', 'msg:done:*', 'processing:*', 'claim:test_*']:
            cursor = 0
            while True:
                cursor, keys = self.r.scan(cursor, match=pattern, count=100)
                if keys:
                    self.r.delete(*keys)
                if cursor == 0:
                    break

    @patch('launcher.fleet_relay_v4_1.subprocess.run')
    def test_message_goes_to_dlq_after_max_retries(self, mock_run):
        """
        ÉTANT DONNÉ un message qui échoue 3 fois
        ALORS il est déplacé vers DLQ
        """
        mock_run.side_effect = mock_subprocess_run_factory()

        from launcher.fleet_relay_v4_1 import process_message_with_retry

        agent = 'test-dl'
        msg = '{"id": "msg_dlq_001", "version": "1.0.0", "type": "TEST", "from": "test", "to": "test-dl", "task_id": "task_dlq_001", "ts": "2026-09-18T10:00:00Z", "body": "test"}'

        # Appeler 3 fois pour épuiser les tentatives (max_retries=2)
        for i in range(3):
            result = process_message_with_retry(self.r, agent, msg, max_retries=2)

        dlq_key = f'dlq:{agent}'
        self.assertGreater(self.r.llen(dlq_key), 0, "Le message devrait être en DLQ")

    def test_dlq_message_not_retried_automatically(self):
        """Un message en DLQ ne doit pas être retraité."""
        from launcher.fleet_relay_v4_1 import should_process_message

        agent = 'test-dl'
        self.r.rpush(f'dlq:{agent}', json.dumps({
            'msg_id': 'msg_dlq_002', 'error': 'test', 'attempts': 3
        }))

        self.assertFalse(should_process_message(self.r, agent, 'msg_dlq_002'))

    def test_explicit_recovery_from_dlq(self):
        """La récupération explicite remet le message dans la queue."""
        from launcher.fleet_relay_v4_1 import recover_from_dlq

        agent = 'test-dl'
        msg_id = 'msg_dlq_003'
        original_msg = '{"id": "msg_dlq_003", "version": "1.0.0", "type": "TEST", "from": "test", "to": "test-dl", "task_id": "task_dlq_003", "ts": "2026-09-18T10:00:00Z", "body": "test"}'

        self.r.rpush(f'dlq:{agent}', json.dumps({
            'msg_id': msg_id, 'error': 'test', 'attempts': 3,
            'original_msg': original_msg
        }))

        recovered = recover_from_dlq(self.r, agent, msg_id)
        self.assertTrue(recovered)
        self.assertGreater(self.r.llen(f'queue:{agent}'), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
