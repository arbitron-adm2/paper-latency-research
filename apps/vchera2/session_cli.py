"""Prepare a dedicated session key; optional one-shot registration, never a trade.
No master secret or signed payload is printed, copied, or persisted. No automatic retries.
"""
import argparse
import datetime
import json
import os
import secrets
import sys
import time
from pathlib import Path

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

APP = Path(__file__).resolve().parent
ROOT = APP / '.runtime'
WALLET = os.environ.get('PAPER_WALLET', '').lower()
EXCHANGE = '0x6cd5661646289fb6e65ea5c032310fded797d0a2'
BASE = 'https://exchange.papertrade.xyz'
DOMAIN = {'name': 'String', 'version': '1', 'chainId': 999, 'verifyingContract': EXCHANGE}
TYPES = {'RegisterSessionKey': [
    {'name': 'user', 'type': 'address'}, {'name': 'key', 'type': 'address'},
    {'name': 'expiry', 'type': 'uint64'}, {'name': 'feeAddress', 'type': 'address'},
    {'name': 'nonce', 'type': 'uint256'}, {'name': 'deadline', 'type': 'uint64'}]}

def save(name, value):
    file = ROOT / name
    tmp = ROOT / (name + '.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f, indent=2); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, file)
    os.chmod(file, 0o600)

def snapshot():
    with requests.get(BASE + '/state/user/live', params={'wallet': WALLET}, headers={'Accept': 'text/event-stream'}, stream=True, timeout=(10, 15)) as r:
        r.raise_for_status()
        event = None
        for line in r.iter_lines(chunk_size=1, decode_unicode=True):
            if line.startswith('event:'): event = line[6:].strip()
            elif line.startswith('data:') and event == 'wallet':
                j = json.loads(line[5:])
                if j.get('address', '').lower() != WALLET: raise ValueError('Wrong provider wallet')
                return j
    raise RuntimeError('No complete snapshot')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'register'])
    parser.add_argument('--master-wallet', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    ROOT.mkdir(mode=0o700, exist_ok=True)
    if ROOT.is_symlink() or ROOT.stat().st_uid != os.getuid(): raise ValueError('Unsafe runtime')
    os.chmod(ROOT, 0o700)
    lock = ROOT / 'session-operation.lock'
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    try:
        master_data = json.loads(Path(args.master_wallet).read_text())
        master = Account.from_key(master_data['private_key'])
        if master.address.lower() != WALLET or master_data['address'].lower() != WALLET: raise ValueError('Signer mismatch')
        del master_data
        keyfile = ROOT / 'session-key.json'
        if keyfile.exists():
            if keyfile.is_symlink() or keyfile.stat().st_mode & 0o077: raise ValueError('Unsafe key permissions')
            session_data = json.loads(keyfile.read_text())
            session = Account.from_key(session_data['private_key'])
            if session_data['master'] != WALLET or session_data['address'].lower() != session.address.lower(): raise ValueError('Session mismatch')
            del session_data
        else:
            session = Account.create()
            save('session-key.json', {'private_key': session.key.hex(), 'address': session.address.lower(), 'master': WALLET, 'created_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()})
        public = {'address': session.address.lower(), 'master': WALLET, 'master_signer_matches': True, 'master_secret_copied': False, 'chainId': 999, 'verifyingContract': EXCHANGE, 'registered_by_this_tool': False}
        if args.command == 'prepare':
            save('session-public.json', public)
            print(json.dumps(public)); return
        # Reconcile before ANY registration, including an attempted/uncertain earlier request.
        snap = snapshot()
        present = next((exp for key, exp in snap['sessionKeys'] if key.lower() == session.address.lower() and int(exp) > time.time() + 3600), None)
        if present:
            public.update(registered_by_this_tool=True, registered_expiry=int(present), reconciled_without_mutation=True)
            save('session-public.json', public); print(json.dumps(public)); return
        if (ROOT / 'registration-attempt.json').exists(): raise RuntimeError('Prior registration attempt unresolved; no blind repeat')
        if snap.get('openPositions') or snap.get('operations'): raise RuntimeError('Wallet has positions or pending operations; registration withheld')
        rpc = 'https://rpc.hyperliquid.xyz/evm'
        def call(method, params):
            r = requests.post(rpc, json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}, timeout=15)
            r.raise_for_status(); data = r.json()
            if 'error' in data: raise RuntimeError('RPC failure')
            return data['result']
        if int(call('eth_chainId', []), 16) != 999 or call('eth_getCode', [EXCHANGE, 'latest']) in ['0x', '0x0']: raise RuntimeError('Chain/contract mismatch')
        now = int(time.time())
        message = {'user': WALLET, 'key': session.address.lower(), 'expiry': now + 27 * 86400, 'feeAddress': '0x' + '00' * 20, 'nonce': secrets.randbits(128), 'deadline': now + 600}
        if DOMAIN['chainId'] != 999 or DOMAIN['verifyingContract'] != EXCHANGE or message['user'] != WALLET or message['key'] == WALLET: raise ValueError('Invalid EIP-712 context')
        if message['expiry'] > now + 28 * 86400 or message['deadline'] > now + 3600 or message['feeAddress'] != '0x' + '00' * 20: raise ValueError('Invalid EIP-712 registration')
        typed = encode_typed_data(domain_data=DOMAIN, message_types=TYPES, message_data=message)
        signature = Account.sign_message(typed, master.key)
        if Account.recover_message(typed, signature=signature.signature).lower() != WALLET: raise ValueError('Recovered signer mismatch')
        digest = keccak(b'\x19' + typed.version + typed.header + typed.body)
        intent_id = '0x' + keccak(keccak(text='papertrade:intent-id:v2') + digest + bytes.fromhex(WALLET[2:])).hex()
        # Persist intent identity BEFORE submitting; never persist signed payload.
        attempt = {'attempted_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'session_address': session.address.lower(), 'intent_id': intent_id, 'outcome': 'unknown', 'expiry': message['expiry']}
        save('registration-attempt.json', attempt)
        signed = {k: (v if k in ['user', 'key', 'feeAddress'] else str(v)) for k, v in message.items()}
        signed.update(v=str(signature.v), r='0x' + signature.r.to_bytes(32, 'big').hex(), s='0x' + signature.s.to_bytes(32, 'big').hex())
        response = requests.post(BASE + '/intents', json={'action': 'registerSessionKey', 'signed': signed}, timeout=15)
        attempt.update(http_status=response.status_code, outcome='accepted_unconfirmed' if response.ok else 'rejected_or_uncertain')
        save('registration-attempt.json', attempt)
        # HTTP acceptance is never treated as active authorization without provider readback.
        after = snapshot()
        present = next((exp for key, exp in after['sessionKeys'] if key.lower() == session.address.lower() and int(exp) > time.time() + 3600), None)
        public.update(registration_http_status=response.status_code, registered_by_this_tool=bool(present), registered_expiry=int(present) if present else None, eip712_master_recovery_verified=True)
        save('session-public.json', public)
        print(json.dumps(public))
    finally:
        lock.unlink(missing_ok=True)

if __name__ == '__main__':
    try: main()
    except Exception as e:
        # No exception data from signing/HTTP libraries, to prevent credentials or signatures leaking.
        print(json.dumps({'ok': False, 'error_type': type(e).__name__, 'message': 'Session operation stopped; no automatic retry. Inspect sanitized local state.'}))
        sys.exit(1)
