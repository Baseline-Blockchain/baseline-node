"""
Cryptographic primitives for Baseline.
"""

from __future__ import annotations

import hashlib
import hmac

__all__ = [
    "sha256",
    "sha256d",
    "hash160",
    "base58check_encode",
    "base58check_decode",
    "generate_pubkey",
    "public_key_from_bytes",
    "sign",
    "verify",
    "address_from_pubkey",
    "CryptoError",
]


class CryptoError(Exception):
    pass


SECP_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F
SECP_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
SECP_A = 0
SECP_B = 7
G_X = 55066263022277343669578718895168534326250603453777594175500187360389116729240
G_Y = 32670510020758816978083085130507043184471273380659243275938904335757337482424
G = (G_X, G_Y)


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def sha256d(data: bytes) -> bytes:
    return sha256(sha256(data))


def ripemd160(data: bytes) -> bytes:
    try:
        return hashlib.new("ripemd160", data).digest()
    except ValueError:
        # OpenSSL 3.0+ requires legacy provider for RIPEMD160
        import subprocess
        # Fallback: use openssl command
        result = subprocess.run(
            ['openssl', 'dgst', '-ripemd160', '-binary'],
            input=data,
            capture_output=True,
            check=True
        )
        return result.stdout


def hash160(data: bytes) -> bytes:
    return ripemd160(sha256(data))


ALPHABET = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def base58check_encode(payload: bytes) -> str:
    checksum = sha256d(payload)[:4]
    return base58_encode(payload + checksum)


def base58check_decode(value: str) -> bytes:
    data = base58_decode(value)
    payload, checksum = data[:-4], data[-4:]
    if sha256d(payload)[:4] != checksum:
        raise CryptoError("Invalid base58 checksum")
    return payload


def base58_encode(data: bytes) -> str:
    num = int.from_bytes(data, "big")
    encoded = bytearray()
    while num:
        num, rem = divmod(num, 58)
        encoded.insert(0, ALPHABET[rem])
    for byte in data:
        if byte == 0:
            encoded.insert(0, ALPHABET[0])
        else:
            break
    return encoded.decode("ascii")


def base58_decode(value: str) -> bytes:
    num = 0
    for ch in value.encode("ascii"):
        num *= 58
        idx = ALPHABET.find(bytes([ch]))
        if idx == -1:
            raise CryptoError("Invalid base58 character")
        num += idx
    data = num.to_bytes((num.bit_length() + 7) // 8, "big") or b"\x00"
    pad = 0
    for ch in value:
        if ch == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + data


def inverse_mod(k: int) -> int:
    if k == 0:
        raise CryptoError("Division by zero")
    return pow(k, SECP_P - 2, SECP_P)


def is_on_curve(point: tuple[int, int]) -> bool:
    x, y = point
    return (y * y - (x * x * x + SECP_A * x + SECP_B)) % SECP_P == 0


def point_add(p: tuple[int, int], q: tuple[int, int]) -> tuple[int, int]:
    if p is None:
        return q
    if q is None:
        return p
    if p[0] == q[0] and p[1] != q[1]:
        return None
    if p == q:
        m = (3 * p[0] * p[0] + SECP_A) * inverse_mod(2 * p[1]) % SECP_P
    else:
        m = (q[1] - p[1]) * inverse_mod((q[0] - p[0]) % SECP_P) % SECP_P
    x_r = (m * m - p[0] - q[0]) % SECP_P
    y_r = (m * (p[0] - x_r) - p[1]) % SECP_P
    return x_r, y_r


def scalar_mul(k: int, point: tuple[int, int] = G) -> tuple[int, int]:
    if k % SECP_N == 0 or point is None:
        return None
    result = None
    addend = point
    while k:
        if k & 1:
            result = point_add(result, addend)
        addend = point_add(addend, addend)
        k >>= 1
    return result


def generate_pubkey(privkey: int, compressed: bool = True) -> bytes:
    if not (1 <= privkey < SECP_N):
        raise CryptoError("Invalid private key range")
    point = scalar_mul(privkey)
    return encode_public_key(point, compressed)


def encode_public_key(point: tuple[int, int], compressed: bool = True) -> bytes:
    if point is None or not is_on_curve(point):
        raise CryptoError("Point not on curve")
    x, y = point
    if not compressed:
        return b"\x04" + x.to_bytes(32, "big") + y.to_bytes(32, "big")
    prefix = b"\x02" if y % 2 == 0 else b"\x03"
    return prefix + x.to_bytes(32, "big")


def public_key_from_bytes(data: bytes) -> tuple[int, int]:
    if data[0] == 4 and len(data) == 65:
        x = int.from_bytes(data[1:33], "big")
        y = int.from_bytes(data[33:], "big")
        point = (x, y)
        if not is_on_curve(point):
            raise CryptoError("Invalid public key")
        return point
    if data[0] in (2, 3) and len(data) == 33:
        x = int.from_bytes(data[1:], "big")
        y_sq = (pow(x, 3, SECP_P) + SECP_B) % SECP_P
        y = pow(y_sq, (SECP_P + 1) // 4, SECP_P)
        if (y % 2) != (data[0] % 2):
            y = SECP_P - y
        point = (x, y)
        if not is_on_curve(point):
            raise CryptoError("Invalid public key")
        return point
    raise CryptoError("Unsupported public key format")


def _bits2int(b: bytes) -> int:
    return int.from_bytes(b, "big")


def _int2bytes(value: int) -> bytes:
    return value.to_bytes(32, "big")


def deterministic_k(msg_hash: bytes, privkey: int) -> int:
    key = _int2bytes(privkey)
    v = b"\x01" * 32
    k = b"\x00" * 32
    k = hmac.new(k, v + b"\x00" + key + msg_hash, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()
    k = hmac.new(k, v + b"\x01" + key + msg_hash, hashlib.sha256).digest()
    v = hmac.new(k, v, hashlib.sha256).digest()
    while True:
        v = hmac.new(k, v, hashlib.sha256).digest()
        candidate = _bits2int(v)
        if 1 <= candidate < SECP_N:
            return candidate
        k = hmac.new(k, v + b"\x00", hashlib.sha256).digest()
        v = hmac.new(k, v, hashlib.sha256).digest()


def encode_der(r: int, s: int) -> bytes:
    def _encode_int(value: int) -> bytes:
        data = value.to_bytes((value.bit_length() + 7) // 8 or 1, "big")
        if data[0] & 0x80:
            data = b"\x00" + data
        return b"\x02" + bytes([len(data)]) + data

    r_enc = _encode_int(r)
    s_enc = _encode_int(s)
    return b"\x30" + bytes([len(r_enc) + len(s_enc)]) + r_enc + s_enc


def decode_der(signature: bytes) -> tuple[int, int]:
    if len(signature) < 8 or signature[0] != 0x30:
        raise CryptoError("Invalid DER sequence")
    length = signature[1]
    if length + 2 != len(signature):
        raise CryptoError("Invalid DER length")
    idx = 2
    if signature[idx] != 0x02:
        raise CryptoError("Invalid R integer tag")
    r_len = signature[idx + 1]
    r = int.from_bytes(signature[idx + 2 : idx + 2 + r_len], "big")
    idx += 2 + r_len
    if signature[idx] != 0x02:
        raise CryptoError("Invalid S integer tag")
    s_len = signature[idx + 1]
    s = int.from_bytes(signature[idx + 2 : idx + 2 + s_len], "big")
    if not (1 <= r < SECP_N and 1 <= s < SECP_N):
        raise CryptoError("Signature integers out of range")
    return r, s


def sign(msg_hash: bytes, privkey: int) -> bytes:
    if len(msg_hash) != 32:
        raise CryptoError("msg_hash must be 32 bytes")
    while True:
        k = deterministic_k(msg_hash, privkey)
        point = scalar_mul(k, G)
        if point is None:
            continue
        r = point[0] % SECP_N
        if r == 0:
            continue
        k_inv = pow(k, SECP_N - 2, SECP_N)
        s = (k_inv * (int.from_bytes(msg_hash, "big") + r * privkey)) % SECP_N
        if s == 0:
            continue
        if s > SECP_N // 2:
            s = SECP_N - s
        return encode_der(r, s)


def verify(msg_hash: bytes, signature: bytes, pubkey: bytes) -> bool:
    try:
        r, s = decode_der(signature)
        point = public_key_from_bytes(pubkey)
    except CryptoError:
        return False
    w = pow(s, SECP_N - 2, SECP_N)
    u1 = int.from_bytes(msg_hash, "big") * w % SECP_N
    u2 = r * w % SECP_N
    p = point_add(scalar_mul(u1, G), scalar_mul(u2, point))
    if p is None:
        return False
    return (p[0] % SECP_N) == r


def address_from_pubkey(pubkey: bytes, version: int = 0x35) -> str:
    payload = bytes([version]) + hash160(pubkey)
    return base58check_encode(payload)
