"""Check exactly the staged Git blobs; findings never print secret values."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
EXTENSIONS = {'.py', '.js', '.cjs', '.mjs', '.css', '.html', '.sql', '.cpp', '.h', '.sh', '.json', '.toml', '.txt', '.md'}
DENIED_DIRS = {'data', 'incoming', 'evidence', 'reports', 'archives', 'research', 'sources', 'operations', 'node_modules', '__pycache__', '.runtime', '.venv', '.worktree'}
DENIED_SUFFIXES = {'.db', '.sqlite', '.sqlite3', '.csv', '.tsv', '.zip', '.gz', '.zst', '.log', '.png', '.jpg', '.jpeg', '.ogg', '.mp4', '.key', '.pem', '.session'}
PATTERNS = {
    'private-key-block': re.compile(rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'github-token': re.compile(rb'gh[pousr]_[A-Za-z0-9]{20,}'),
    'api-token': re.compile(rb'sk-[A-Za-z0-9_-]{20,}'),
    'jwt': re.compile(rb'eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}'),
    'credential-url': re.compile(rb'https?://[^\s/:]+:[^\s/@]+@'),
    'machine-home-path': re.compile(('/' + 'home/' + r'[A-Za-z0-9_.-]+').encode()),
    'credential-literal': re.compile(rb'''(?i)(?:private[_-]?key|api[_-]?key|password|secret|access[_-]?token)["']?\s*[:=]\s*["']([A-Za-z0-9_+/=-]{24,})["']'''),
}

def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args])

def check_history(owner, ref='HEAD'):
    """Check every reachable author and committer, not just the current tree."""
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]{0,38}', owner):
        raise ValueError('Expected GitHub account name')
    email = re.compile(r'(?:[0-9]+\+)?' + re.escape(owner) + r'@users\.noreply\.github\.com', re.I)
    commits = git('rev-list', ref).decode().splitlines()
    findings = []
    for commit in commits:
        fields = git('show', '-s', '--format=%an%x00%ae%x00%cn%x00%ce', commit).decode().rstrip('\n').split('\0')
        if len(fields) != 4:
            raise ValueError('Unexpected Git identity format')
        for role, name, address in [('author', *fields[:2]), ('committer', *fields[2:])]:
            if name.lower() != owner.lower() or not email.fullmatch(address):
                findings.append({'commit': commit, 'kind': 'unexpected-git-identity', 'role': role})
    return {'commits': len(commits), 'findings': findings}

def check():
    entries = git('ls-files', '--stage', '-z').split(b'\0')
    findings, records = [], []
    for entry in filter(None, entries):
        info, raw_path = entry.split(b'\t', 1)
        mode, oid, stage = info.decode().split()
        name = raw_path.decode()
        path = Path(name)
        if stage != '0' or mode not in {'100644', '100755'}:
            findings.append({'path': name, 'kind': 'non-regular-or-conflicted-file'})
            continue
        if any(part in DENIED_DIRS for part in path.parts) or path.suffix in DENIED_SUFFIXES:
            findings.append({'path': name, 'kind': 'data-or-runtime-path'})
        if path.suffix not in EXTENSIONS and name != '.gitignore':
            findings.append({'path': name, 'kind': 'unapproved-file-type'})
        if name == 'apps/vchera2/launch-config.json' or (path.name.startswith('.env') and path.name != '.env.example'):
            findings.append({'path': name, 'kind': 'private-configuration'})
        data = git('cat-file', 'blob', oid)
        if b'\0' in data or len(data) > 2_000_000:
            findings.append({'path': name, 'kind': 'binary-or-oversized-file'})
        for kind, pattern in PATTERNS.items():
            for match in pattern.finditer(data):
                # Low-entropy literal unit-test placeholders are not credentials.
                if kind == 'credential-literal' and len(set(match.group(1))) < 8:
                    continue
                findings.append({'path': name, 'kind': kind, 'line': data[:match.start()].count(b'\n') + 1})
        records.append({'path': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    if not records:
        findings.append({'kind': 'empty-index'})
    return {'files': len(records), 'bytes': sum(r['bytes'] for r in records), 'findings': findings, 'manifest_sha256': hashlib.sha256(json.dumps(records, sort_keys=True).encode()).hexdigest()}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history-owner', help='Require every reachable author/committer to use this GitHub account and its noreply email')
    args = parser.parse_args()
    result = check()
    if args.history_owner:
        result['history'] = check_history(args.history_owner)
        result['findings'].extend(result['history']['findings'])
    print(json.dumps(result, indent=2))
    raise SystemExit(bool(result['findings']))
