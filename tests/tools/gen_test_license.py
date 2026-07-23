#!/usr/bin/env python3
"""Reference / test license signer for the Argus ODBC driver enterprise edition.

Generates an Ed25519 keypair and mints `ARGUSLIC1:` license tokens for tests and
local development. This is the SERVER-SIDE signing tool: the private key stays
here (or on an offline signer / HSM in production) and NEVER ships in the driver.
The enterprise add-on (argus_ee) embeds only the PUBLIC key and verifies tokens
offline — mirror this format there.

Token wire format (see docs/LICENSING.md, plan Part B):
    ARGUSLIC1:<base64url(payload_json)>.<base64url(signature)>
The signature covers the EXACT base64url(payload) string (the bytes between the
"ARGUSLIC1:" prefix and the "."). There is no `alg` field in the token — the
algorithm is pinned by the embedded key, so alg-confusion attacks are impossible.

Requires: python3-cryptography (Ed25519).

Examples:
    # 1. Make a dev keypair (prints the raw 32-byte public key to bake in C)
    python3 gen_test_license.py keygen --out-dir ./devkeys

    # 2. Mint a 30-day enterprise token
    python3 gen_test_license.py sign --key ./devkeys/argus_license_dev.key \
        --org "ACME Corp" --org-id acme --features bigquery,trino --seats 50 \
        --exp-days 30

    # 3. Emit the standard unit-test fixture set (valid/expired/tampered/...)
    python3 gen_test_license.py fixtures --key ./devkeys/argus_license_dev.key \
        --out-dir ./fixtures

    # 4. Self-check a token against a public key (reference for the C verifier)
    python3 gen_test_license.py verify --token "ARGUSLIC1:..." \
        --pub ./devkeys/argus_license_dev.pub
"""
import argparse
import base64
import json
import sys
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey, Ed25519PublicKey,
)
from cryptography.exceptions import InvalidSignature

PREFIX = "ARGUSLIC1:"


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


# ── key handling ──────────────────────────────────────────────────────────

def load_priv(path: str) -> Ed25519PrivateKey:
    with open(path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        sys.exit(f"{path}: not an Ed25519 private key")
    return key


def load_pub(path: str) -> Ed25519PublicKey:
    with open(path, "rb") as f:
        key = serialization.load_pem_public_key(f.read())
    if not isinstance(key, Ed25519PublicKey):
        sys.exit(f"{path}: not an Ed25519 public key")
    return key


def pub_raw_hex(pub: Ed25519PublicKey) -> str:
    raw = pub.public_bytes(serialization.Encoding.Raw,
                           serialization.PublicFormat.Raw)
    return raw.hex()


def pub_spki_hex(pub: Ed25519PublicKey) -> str:
    der = pub.public_bytes(serialization.Encoding.DER,
                           serialization.PublicFormat.SubjectPublicKeyInfo)
    return der.hex()


# ── token minting / verifying ─────────────────────────────────────────────

def sign_token(priv: Ed25519PrivateKey, claims: dict) -> str:
    payload = json.dumps(claims, separators=(",", ":"), sort_keys=True)
    p64 = b64url(payload.encode("utf-8"))
    sig = priv.sign(p64.encode("ascii"))
    return f"{PREFIX}{p64}.{b64url(sig)}"


def verify_token(token: str, pub: Ed25519PublicKey, now: int | None = None):
    """Return (ok, status, claims_or_reason). Mirrors the intended C logic."""
    now = int(time.time()) if now is None else now
    if not token.startswith(PREFIX):
        return False, "MALFORMED", "missing ARGUSLIC1: prefix"
    body = token[len(PREFIX):]
    if body.count(".") != 1:
        return False, "MALFORMED", "expected exactly one '.'"
    p64, s64 = body.split(".")
    try:
        sig = b64url_decode(s64)
    except Exception as e:                       # noqa: BLE001
        return False, "MALFORMED", f"bad base64url signature: {e}"
    try:
        pub.verify(sig, p64.encode("ascii"))     # verify over the exact p64 bytes
    except InvalidSignature:
        return False, "BAD_SIGNATURE", "signature does not verify"
    try:
        claims = json.loads(b64url_decode(p64))
    except Exception as e:                        # noqa: BLE001
        return False, "MALFORMED", f"bad payload JSON: {e}"

    skew = 24 * 3600
    nbf = claims.get("nbf")
    if nbf and now < nbf - skew:
        return False, "NOT_YET_VALID", claims
    exp = claims.get("exp", 0)
    if exp:
        grace = int(claims.get("grace_days", 14)) * 86400
        if now <= exp + skew:
            return True, "VALID", claims
        if now <= exp + grace + skew:
            return True, "GRACE", claims
        return False, "EXPIRED", claims
    return True, "VALID", claims                  # exp==0/absent → perpetual


# ── subcommands ───────────────────────────────────────────────────────────

def cmd_keygen(args):
    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    key_path = f"{args.out_dir}/argus_license_dev.key"
    pub_path = f"{args.out_dir}/argus_license_dev.pub"
    import os
    os.makedirs(args.out_dir, exist_ok=True)
    with open(key_path, "wb") as f:
        f.write(priv.private_bytes(serialization.Encoding.PEM,
                                   serialization.PrivateFormat.PKCS8,
                                   serialization.NoEncryption()))
    os.chmod(key_path, 0o600)
    with open(pub_path, "wb") as f:
        f.write(pub.public_bytes(serialization.Encoding.PEM,
                                 serialization.PublicFormat.SubjectPublicKeyInfo))
    print(f"private key: {key_path}  (DEV/TEST ONLY — never ship, never commit)")
    print(f"public  key: {pub_path}")
    print(f"pubkey raw-32 hex (bake this in argus_ee): {pub_raw_hex(pub)}")
    print(f"pubkey SPKI der hex:                        {pub_spki_hex(pub)}")


def build_claims(args) -> dict:
    now = int(time.time())
    claims = {
        "v": 1,
        "lid": args.lid or f"ARG-TEST-{now}",
        "org": args.org,
        "org_id": args.org_id,
        "edition": "enterprise",
        "features": [f.strip() for f in args.features.split(",") if f.strip()],
        "seats": args.seats,
        "iat": now,
        "nbf": now + args.nbf_days * 86400,
        "grace_days": args.grace_days,
    }
    if args.exp_days is not None:
        claims["exp"] = now + args.exp_days * 86400
    if args.domains:
        claims["bind"] = {"domains": [d.strip() for d in args.domains.split(",")]}
    return claims


def cmd_sign(args):
    priv = load_priv(args.key)
    print(sign_token(priv, build_claims(args)))


def cmd_pubkey(args):
    pub = load_priv(args.key).public_key() if args.key else load_pub(args.pub)
    if args.fmt == "raw-hex":
        print(pub_raw_hex(pub))
    elif args.fmt == "spki-hex":
        print(pub_spki_hex(pub))
    else:
        sys.stdout.buffer.write(pub.public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo))


def cmd_verify(args):
    pub = load_pub(args.pub)
    ok, status, info = verify_token(args.token, pub)
    print(f"status={status} allow={ok}")
    if isinstance(info, dict):
        print(json.dumps(info, indent=2, sort_keys=True))
    else:
        print(f"reason: {info}")
    sys.exit(0 if ok else 1)


def cmd_fixtures(args):
    """Emit the standard unit-test fixture set for the argus_ee verify tests."""
    import os
    os.makedirs(args.out_dir, exist_ok=True)
    priv = load_priv(args.key)
    pub = priv.public_key()
    now = int(time.time())
    base = {"v": 1, "lid": "ARG-FIX", "org": "Fixture", "org_id": "fix",
            "edition": "enterprise", "features": ["*"], "seats": 1, "iat": now,
            "grace_days": 14}

    def emit(name, token):
        with open(f"{args.out_dir}/{name}.tok", "w") as f:
            f.write(token + "\n")
        print(f"  {name}.tok")

    emit("valid", sign_token(priv, {**base, "exp": now + 30 * 86400}))
    emit("perpetual", sign_token(priv, {**base}))
    emit("expired", sign_token(priv, {**base, "exp": now - 60 * 86400}))
    emit("grace", sign_token(priv, {**base, "exp": now - 3 * 86400}))
    emit("not_yet", sign_token(priv, {**base, "nbf": now + 60 * 86400,
                                      "exp": now + 90 * 86400}))
    # tampered: flip a byte of the payload segment, keep the old signature
    good = sign_token(priv, {**base, "exp": now + 30 * 86400})
    p64, s64 = good[len(PREFIX):].split(".")
    flipped = p64[:-2] + ("A" if p64[-2] != "A" else "B") + p64[-1]
    emit("tampered", f"{PREFIX}{flipped}.{s64}")
    # wrong_key: same claims signed by a different key
    other = Ed25519PrivateKey.generate()
    emit("wrong_key", sign_token(other, {**base, "exp": now + 30 * 86400}))

    with open(f"{args.out_dir}/pubkey_raw_hex.txt", "w") as f:
        f.write(pub_raw_hex(pub) + "\n")
    print(f"  pubkey_raw_hex.txt  (bake into the argus_ee verifier)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("keygen", help="generate a dev Ed25519 keypair")
    g.add_argument("--out-dir", default=".")
    g.set_defaults(func=cmd_keygen)

    s = sub.add_parser("sign", help="mint an ARGUSLIC1: token")
    s.add_argument("--key", required=True, help="signer private key (PEM)")
    s.add_argument("--org", default="Test Org")
    s.add_argument("--org-id", default="test")
    s.add_argument("--features", default="*",
                   help="comma-separated entitled backends/features, or '*'")
    s.add_argument("--seats", type=int, default=1)
    s.add_argument("--exp-days", type=int, default=None,
                   help="days until expiry (omit for perpetual)")
    s.add_argument("--nbf-days", type=int, default=0)
    s.add_argument("--grace-days", type=int, default=14)
    s.add_argument("--lid", default=None)
    s.add_argument("--domains", default=None, help="optional org-domain binding")
    s.set_defaults(func=cmd_sign)

    p = sub.add_parser("pubkey", help="print the public key (for baking)")
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--key", help="derive from a private key (PEM)")
    grp.add_argument("--pub", help="a public key (PEM)")
    p.add_argument("--fmt", choices=["raw-hex", "spki-hex", "pem"],
                   default="raw-hex")
    p.set_defaults(func=cmd_pubkey)

    v = sub.add_parser("verify", help="self-check a token (reference logic)")
    v.add_argument("--token", required=True)
    v.add_argument("--pub", required=True, help="public key (PEM)")
    v.set_defaults(func=cmd_verify)

    fx = sub.add_parser("fixtures", help="emit the unit-test fixture set")
    fx.add_argument("--key", required=True, help="signer private key (PEM)")
    fx.add_argument("--out-dir", default="./license_fixtures")
    fx.set_defaults(func=cmd_fixtures)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
