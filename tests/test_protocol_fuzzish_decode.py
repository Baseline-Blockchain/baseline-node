import asyncio
import random
import string
import unittest

from baseline.net import protocol


def _frame(length: int, checksum: bytes, payload: bytes) -> bytes:
    return length.to_bytes(protocol.LEN_FIELD, "big") + checksum + payload


class ProtocolFuzzishDecodeTests(unittest.IsolatedAsyncioTestCase):
    async def _decode(self, framed: bytes):
        reader = asyncio.StreamReader()
        reader.feed_data(framed)
        reader.feed_eof()
        return await protocol.read_message(reader, timeout=0.5)

    async def test_garbage_frames_never_crash_always_reject(self):
        """
        Fuzz-ish test: feed random/garbage frames into read_message() and assert:
        - it does not crash with unexpected exceptions
        - it always rejects via ProtocolError
        """
        rng = random.Random(1337)

        # 1) Invalid lengths should reject immediately
        for bad_len in (0, protocol.MAX_PAYLOAD + 1, protocol.MAX_PAYLOAD + 999999):
            framed = _frame(bad_len, b"\x00\x00\x00\x00", b"")
            with self.assertRaises(protocol.ProtocolError):
                await self._decode(framed)

        # 2) Random frames with guaranteed checksum mismatch
        for _ in range(200):
            length = rng.randint(1, 2048)

            # Keep payload valid UTF-8 so we can reach deeper paths reliably when desired
            payload = "".join(rng.choice(string.printable) for _ in range(length)).encode("utf-8")

            # Compute the real checksum and flip a bit to guarantee mismatch
            good = protocol.sha256d(payload)[: protocol.CHECKSUM_FIELD]
            bad = bytes([good[0] ^ 0x01]) + good[1:]

            framed = _frame(len(payload), bad, payload)
            with self.assertRaises(protocol.ProtocolError):
                await self._decode(framed)

        # 3) Correct checksum but invalid JSON (should be ProtocolError, not crash)
        for _ in range(50):
            payload = ("not-json-" + "".join(rng.choice(string.ascii_letters) for _ in range(50))).encode("utf-8")
            checksum = protocol.sha256d(payload)[: protocol.CHECKSUM_FIELD]
            framed = _frame(len(payload), checksum, payload)
            with self.assertRaises(protocol.ProtocolError):
                await self._decode(framed)

        # 4) Correct checksum, valid JSON, but not a dict
        for payload_text in ("[]", '"hello"', "123", "true", "null"):
            payload = payload_text.encode("utf-8")
            checksum = protocol.sha256d(payload)[: protocol.CHECKSUM_FIELD]
            framed = _frame(len(payload), checksum, payload)
            with self.assertRaises(protocol.ProtocolError):
                await self._decode(framed)

        # 5) Correct checksum, dict JSON, but missing "type"
        payload = b'{"foo": 1, "bar": "baz"}'
        checksum = protocol.sha256d(payload)[: protocol.CHECKSUM_FIELD]
        framed = _frame(len(payload), checksum, payload)
        with self.assertRaises(protocol.ProtocolError):
            await self._decode(framed)


if __name__ == "__main__":
    unittest.main()
