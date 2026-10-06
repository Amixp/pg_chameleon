"""Disposable native PG16 fixture; retained test script, no remote credentials."""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time

import psycopg2


def main():
    os.chdir(Path(__file__).resolve().parents[1])
    name = 'betark-cdc-sql-' + secrets.token_hex(6)
    # Trust is confined to this disposable container and a random loopback port.
    subprocess.run(['podman', 'run', '-d', '--name', name, '--label', 'purpose=betark-cdc-tests',
                    '-p', '127.0.0.1::5432', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust',
                    'docker.io/library/postgres:16-alpine'], check=True, stdout=subprocess.DEVNULL)
    try:
        info = json.loads(subprocess.check_output(['podman', 'inspect', name]))[0]
        port = info['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
        dsn = 'host=127.0.0.1 port=' + port + ' user=postgres dbname=postgres'
        for attempt in range(60):
            try:
                conn = psycopg2.connect(dsn)
                break
            except psycopg2.OperationalError:
                time.sleep(0.5)
        else:
            raise RuntimeError('Fixture did not become ready')
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute('CREATE DATABASE betark_cdc_gate_sql')
        conn.close()
        dsn = dsn.replace('dbname=postgres', 'dbname=betark_cdc_gate_sql')
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE public.betark_fixture_marker(purpose text NOT NULL)')
            cur.execute("INSERT INTO public.betark_fixture_marker VALUES ('transactional-cdc-tests')")
        conn.close()
        with tempfile.TemporaryDirectory(prefix='betark-cdc-sql-') as directory:
            path = Path(directory) / 'dsn'
            path.write_text(dsn)
            path.chmod(0o600)
            env = dict(os.environ, CDC_TEST_DSN_FILE=str(path))
            return subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', 'tests',
                                   '-p', 'test_transaction*.py', '-v'], env=env).returncode
    finally:
        subprocess.run(['podman', 'stop', name], check=True, stdout=subprocess.DEVNULL)
        subprocess.run(['podman', 'rm', name], check=True, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    sys.exit(main())
