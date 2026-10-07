#! /usr/bin/env atf-sh

. $(atf_get_srcdir)/test_environment.sh

tests_init sequential parallel progress_tty cached missing checksum truncated \
	interrupted interrupted_group worker_died sighup_ignored dry_run file_repo \
	mirror http_mirrors invalid_workers

parallel_test()
{
	atf_require python3 "Requires python3 to run this test"
	cat > exercise.py <<'PY'
import functools
import hashlib
import http.server
import json
import os
import pty
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

mode = sys.argv[1]
root = Path.cwd()
repo = root / 'repo'
repo.mkdir()
cache = root / 'cache'
config = root / 'pkg.conf'
workers = 1 if mode in ('sequential', 'mirror', 'file_repo') else 3
barrier = threading.Barrier(workers, timeout=10)
lock = threading.Lock()
hold = threading.Event()
requests = []
active = peak = mirror_lists = 0
held = mode in ('interrupted', 'interrupted_group', 'worker_died',
                'sighup_ignored')


def run(*args, ok=True):
    p = subprocess.run(['pkg', *args], capture_output=True, text=True, timeout=30)
    if (p.returncode == 0) != ok:
        raise AssertionError(f'{p.args}: {p.returncode}\n{p.stdout}\n{p.stderr}')
    return p


for i in range(6):
    name = f'pf-{i}'
    subprocess.run(['sh', os.environ['RESOURCEDIR'] + '/test_subr.sh',
                    'new_pkg', name, name, '1'], check=True)
    run('create', '-o', str(repo), '-M', name + '.ucl')
run('repo', str(repo))


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        global active, peak, mirror_lists
        if mode == 'http_mirrors':
            if self.path == '/':
                # The mirror list of the server, see gethttpmirrors().
                with lock:
                    mirror_lists += 1
                body = f'URL: {url}/mirror\n'.encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path.startswith('/mirror/'):
                self.path = self.path[len('/mirror'):]
        package = '/pf-' in self.path
        if not package:
            return super().do_GET()
        with lock:
            requests.append(self.path)
            active += 1
            peak = max(peak, active)
        try:
            if held:
                hold.wait(20)
            elif barrier is not None:
                barrier.wait()
        except threading.BrokenBarrierError:
            self.send_error(503, 'Concurrent package requests did not arrive')
            return
        finally:
            # Decrement before sending the last response bytes, so a new
            # request cannot race the accounting for a completed transfer.
            with lock:
                active -= 1
        if mode == 'missing' and '/pf-0-' in self.path:
            self.send_error(404)
            return
        if mode in ('checksum', 'truncated') and '/pf-0-' in self.path:
            data = Path(self.translate_path(self.path)).read_bytes()
            self.send_response(200)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            if mode == 'checksum':
                data = data[:-1] + bytes([data[-1] ^ 1])
            else:
                data = data[:len(data) // 2]
            self.wfile.write(data)
            self.close_connection = True
            return
        try:
            super().do_GET()
        except (BrokenPipeError, ConnectionResetError):
            pass


server = http.server.ThreadingHTTPServer(
    ('127.0.0.1', 0), functools.partial(Handler, directory=str(repo)))
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
url = f'http://127.0.0.1:{server.server_port}'
if mode == 'file_repo':
    url = repo.as_uri()
mirror_type = ', mirror_type: "http"' if mode == 'http_mirrors' else ''
config.write_text(f'''
PKG_DBDIR = "{root}"
PKG_CACHEDIR = "{cache}"
REPOS_DIR = []
REPO_AUTOUPDATE = false
FETCH_RETRY = 1
FETCH_TIMEOUT = 5
repositories: {{ local: {{ url: "{url}"{mirror_type} }} }}
''')
# Omit FETCH_WORKERS entirely in the sequential test to check its default.
if mode != 'sequential':
    with config.open('a') as f:
        f.write('FETCH_WORKERS = 3\n')
run('-C', str(config), 'update')
mirror_lists = 0
args = ['-C', str(config), 'fetch', '-ay']
proc = None
try:
    if mode == 'invalid_workers':
        for value in ('0', '-1', '17'):
            # Rejected while loading the configuration.
            p = run('-C', str(config), '-o', 'FETCH_WORKERS=' + value,
                    'info', ok=False)
            assert 'FETCH_WORKERS must be between 1 and 16' in p.stderr, p.stderr
        assert not requests
    elif mode == 'dry_run':
        run('-C', str(config), 'shell', 'SELECT 1;')
        p = run('-C', str(config), 'install', '-n', 'pf-0', ok=False)
        assert 'to be downloaded' in p.stdout, p.stdout
        assert not requests
        assert not list(cache.glob('*.pkg'))
    elif mode == 'progress_tty':
        master, slave = pty.openpty()
        proc = subprocess.Popen(['pkg', *args], stdin=subprocess.DEVNULL,
                                stdout=slave, stderr=subprocess.PIPE)
        os.close(slave)
        out = b''
        while True:
            try:
                chunk = os.read(master, 4096)
            except OSError:  # EIO once the last writer is gone on Linux
                break
            if not chunk:
                break
            out += chunk
        os.close(master)
        err = proc.stderr.read()
        assert proc.wait(timeout=30) == 0, (out, err)
        text = out.decode()
        # The batch progress bar is redrawn in place with \r: per package
        # events from the workers must not break it into several lines.
        start, end = text.index('Fetching packages'), text.rindex('100%')
        assert '\n' not in text[start:end], repr(text)
    elif mode == 'sighup_ignored':
        # As under nohup(1): an ignored SIGHUP must not cancel the fetch,
        # neither in libpkg nor in the workers.
        proc = subprocess.Popen(
            ['pkg', *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True,
            preexec_fn=lambda: signal.signal(signal.SIGHUP, signal.SIG_IGN))
        deadline = time.monotonic() + 10
        while len(requests) < 3 and time.monotonic() < deadline:
            time.sleep(.02)
        assert len(requests) == 3, requests
        os.killpg(proc.pid, signal.SIGHUP)
        time.sleep(.5)
        hold.set()
        out, err = proc.communicate(timeout=30)
        assert proc.returncode == 0, (out, err)
        assert len(list(cache.glob('pf-*-1~*.pkg'))) == 6
    elif held:
        proc = subprocess.Popen(['pkg', *args], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, start_new_session=True)
        deadline = time.monotonic() + 10
        while len(requests) < 3 and time.monotonic() < deadline:
            time.sleep(.02)
        assert len(requests) == 3, requests
        # The CLI has a supervisor; signal the process running libpkg only.
        # Its fetch workers must be reaped by the code under test.
        pairs = [tuple(map(int, line.split())) for line in
                 subprocess.check_output(['ps', '-ax', '-o', 'pid=', '-o', 'ppid='],
                                         text=True).splitlines()]
        parent, = [pid for pid, ppid in pairs if ppid == proc.pid]
        children = [pid for pid, ppid in pairs if ppid == parent]
        assert len(children) == 3, children
        if mode == 'interrupted_group':
            # As ^C does: the workers die of the signal as well, which is
            # to be reported as a cancellation, not as failed fetches.
            os.killpg(proc.pid, signal.SIGINT)
        else:
            os.kill(parent if mode == 'interrupted' else children[0],
                    signal.SIGTERM if mode == 'interrupted' else
                    signal.SIGKILL)
        out, err = proc.communicate(timeout=10)
        assert proc.returncode != 0, (out, err)
        if mode == 'worker_died':
            assert b'killed by signal 9' in err, err
        if mode == 'interrupted_group':
            # SIGINT has its default action: libpkg redelivers it once the
            # workers are reaped, and the CLI dies of it, without errors.
            assert proc.returncode == -signal.SIGINT, (proc.returncode, err)
            assert b'Failed to fetch' not in err, err
        for pid in children:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            raise AssertionError(f'Fetch worker {pid} survived cancellation')
        assert not run('-C', str(config), 'info', '-q').stdout
    elif mode in ('missing', 'checksum', 'truncated'):
        p = run('-C', str(config), 'install', '-y',
                *[f'pf-{i}' for i in range(6)], ok=False)
        assert 'Failed to fetch pf-0-1' in p.stderr, p.stderr
        if mode == 'checksum':
            assert 'failed checksum' in p.stderr, p.stderr
        if mode == 'missing':
            assert 'Not Found' in p.stderr, p.stderr
        assert not run('-C', str(config), 'info', '-q').stdout
        assert not list(cache.glob('pf-0-1~*.pkg'))
    else:
        if mode == 'mirror':
            args = ['-C', str(config), 'fetch', '-ay', '-o', str(root / 'mirror')]
        p = run(*args)
        if mode == 'parallel':
            # Without a terminal, the batch progress is printed once.
            assert p.stdout.count('Fetching packages') == 1, p.stdout
        if mode == 'file_repo':
            assert not requests
        else:
            assert len(requests) == 6, requests
            assert peak == workers, (peak, workers)
        if mode == 'http_mirrors':
            # Fetched once by libpkg, rather than once by each worker.
            assert mirror_lists == 1, mirror_lists
        if mode in ('parallel', 'sequential', 'cached'):
            for i in range(6):
                path, = cache.glob(f'pf-{i}-1~*.pkg')
                assert hashlib.sha256(path.read_bytes()).digest() == \
                    hashlib.sha256((repo / f'pf-{i}-1.pkg').read_bytes()).digest()
        if mode == 'cached':
            requests.clear()
            barrier = None
            run(*args)
            assert not requests, requests
            # Force a fetch phase and verify that another, same-size corrupt
            # cache entry is checked and refetched by the ordinary fetch path.
            path, = cache.glob('pf-0-1~*.pkg')
            data = path.read_bytes()
            path.write_bytes(data[:-1] + bytes([data[-1] ^ 1]))
            path, = cache.glob('pf-1-1~*.pkg')
            path.unlink()
            run(*args)
            assert len(requests) == 2, requests
        if mode == 'parallel':
            # Install with half of the packages left to fetch: the actions
            # must be numbered against all 6 of them, not against the 3
            # downloads, whose counter the parallel fetch has to reset.
            for i in range(3, 6):
                path, = cache.glob(f'pf-{i}-1~*.pkg')
                path.unlink()
            requests.clear()
            barrier = None
            p = run('-C', str(config), 'install', '-y',
                    *[f'pf-{i}' for i in range(6)])
            assert len(requests) == 3, requests
            steps = [line.split()[0] for line in p.stdout.splitlines()
                     if 'Installing pf-' in line]
            assert steps == [f'[{i}/6]' for i in range(1, 7)], p.stdout
            p = run('-C', str(config), 'info', '-q')
            assert len(p.stdout.splitlines()) == 6, p.stdout
finally:
    if proc is not None and proc.poll() is None:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
    hold.set()
    server.shutdown()
    server.server_close()
PY
	atf_check -o ignore -e ignore python3 exercise.py "$1"
}

sequential_body() { parallel_test sequential; }
parallel_body() { parallel_test parallel; }
cached_body() { parallel_test cached; }
missing_body() { parallel_test missing; }
checksum_body() { parallel_test checksum; }
truncated_body() { parallel_test truncated; }
progress_tty_body() { parallel_test progress_tty; }
interrupted_body() { parallel_test interrupted; }
interrupted_group_body() { parallel_test interrupted_group; }
worker_died_body() { parallel_test worker_died; }
sighup_ignored_body() { parallel_test sighup_ignored; }
dry_run_body() { parallel_test dry_run; }
file_repo_body() { parallel_test file_repo; }
mirror_body() { parallel_test mirror; }
http_mirrors_body() { parallel_test http_mirrors; }
invalid_workers_body() { parallel_test invalid_workers; }
