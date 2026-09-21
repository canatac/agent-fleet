#!/usr/bin/env python3
"""
redis_fixture.py — Dispositif de test : Redis jetable démarré/arrêté par les tests.

Sécurité :
- Le serveur est lancé par ce module sur un port dédié (défaut 6399) avec
  --save '' --appendonly no : aucune persistance, aucune écriture disque.
- Les tests se connectent EXCLUSIVEMENT à cette instance.
- Si FLEET_TEST_REDIS_EXTERNAL=1 est défini, on refuse de démarrer/arrêter un
  serveur ET on refuse de nettoyer queue:* (aucun serveur fourni librement
  par variable ne sera nettoyé).
- Redis absent → les tests sont SKIPÉS (jamais annoncés PASS).
"""

import atexit
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

DEFAULT_PORT = int(os.environ.get("FLEET_TEST_REDIS_PORT", "6399"))
DEFAULT_HOST = "127.0.0.1"
EXTERNAL = os.environ.get("FLEET_TEST_REDIS_EXTERNAL", "") == "1"

_server_proc = None
_tmpdir = None


def _port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


def _find_redis_server() -> str | None:
    return shutil.which("redis-server")


def start_test_redis(port: int = DEFAULT_PORT) -> tuple[str, int] | None:
    """Démarre un Redis jetable. Retourne (host, port) ou None si impossible.

    Refuse de démarrer si FLEET_TEST_REDIS_EXTERNAL=1 (mode interdit : les
    tests ne doivent jamais toucher une instance fournie par variable).
    """
    global _server_proc, _tmpdir
    if EXTERNAL:
        return None
    if _port_open(DEFAULT_HOST, port):
        # Port déjà occupé : ne pas le réutiliser (ce pourrait être autre chose).
        return None
    binary = _find_redis_server()
    if not binary:
        return None
    _tmpdir = tempfile.mkdtemp(prefix="fleet-test-redis-")
    _server_proc = subprocess.Popen(
        [
            binary,
            "--port", str(port),
            "--bind", DEFAULT_HOST,
            "--save", "",
            "--appendonly", "no",
            "--dir", _tmpdir,
            "--daemonize", "no",
            "--pidfile", "",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Attendre la disponibilité (max 5s).
    for _ in range(50):
        if _server_proc.poll() is not None:
            _cleanup()
            return None
        if _port_open(DEFAULT_HOST, port):
            atexit.register(stop_test_redis)
            return (DEFAULT_HOST, port)
        time.sleep(0.1)
    _cleanup()
    return None


def stop_test_redis() -> None:
    """Arrête le Redis jetable démarré par start_test_redis (si applicable)."""
    global _server_proc, _tmpdir
    if _server_proc is None:
        return
    if _server_proc.poll() is None:
        _server_proc.send_signal(signal.SIGTERM)
        try:
            _server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _server_proc.kill()
            _server_proc.wait(timeout=5)
    _server_proc = None
    _cleanup()


def _cleanup() -> None:
    global _tmpdir
    if _tmpdir:
        shutil.rmtree(_tmpdir, ignore_errors=True)
        _tmpdir = None


def is_test_redis_available(host: str, port: int) -> bool:
    """Vérifie que l'instance répond au PING."""
    try:
        import redis
        r = redis.Redis(host=host, port=port, socket_connect_timeout=2)
        return r.ping()
    except Exception:
        return False
