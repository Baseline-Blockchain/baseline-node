import asyncio
import unittest

from baseline.net import protocol


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip(self):
        message = {"type": "ping", "nonce": 42}
        framed = protocol.encode_message(message)

        reader = asyncio.StreamReader()

        async def feed():
            reader.feed_data(framed)
            reader.feed_eof()

        await feed()
        decoded = await protocol.read_message(reader, timeout=1)
        self.assertEqual(decoded, message)

    def test_payload_limit(self):
        message = {"type": "payload", "data": "x" * (protocol.MAX_PAYLOAD + 1)}
        with self.assertRaises(protocol.ProtocolError):
            protocol.encode_message(message)
