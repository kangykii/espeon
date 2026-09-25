import unittest

from retrieval.qdrant_local_bridge import (
    LocalQdrantBridge,
    decode_command,
    encode_result,
    text_input,
)


class TextInputBoundaryTests(unittest.TestCase):
    def test_plain_query_remains_plain_text(self) -> None:
        self.assertEqual(text_input("  BTCUSD continuation  ", "query"), "BTCUSD continuation")

    def test_legacy_sequence_is_flattened_before_embedding(self) -> None:
        self.assertEqual(
            text_input(["BTCUSD", "30-minute continuation"], "query"),
            "BTCUSD\n30-minute continuation",
        )

    def test_nested_or_structured_values_are_rejected_at_boundary(self) -> None:
        with self.assertRaisesRegex(TypeError, "received dict"):
            text_input({"question": "BTCUSD"}, "query")
        with self.assertRaisesRegex(TypeError, "received list"):
            text_input([["BTCUSD"]], "query")

    def test_empty_text_is_rejected_at_boundary(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            text_input([" ", ""], "query")

    def test_dense_encoder_receives_one_normalized_string(self) -> None:
        class StringOnlyDense:
            received = None

            def embed(self, value):
                self.received = value
                if not isinstance(value, str):
                    raise TypeError("expected scalar string")
                yield [0.25, 0.75]

        bridge = object.__new__(LocalQdrantBridge)
        bridge.dense = StringOnlyDense()
        self.assertEqual(bridge._dense_vector(["BTCUSD", "continuation"]), [0.25, 0.75])
        self.assertEqual(bridge.dense.received, "BTCUSD\ncontinuation")

    def test_bridge_protocol_preserves_non_ascii_research_text(self) -> None:
        command = {
            "action": "upsert",
            "records": [{"text": "Shen et al., “Bitcoin momentum” — 2026"}],
        }
        decoded = decode_command(encode_result(command))
        text = decoded["records"][0]["text"]
        self.assertEqual(text, command["records"][0]["text"])
        self.assertFalse(any(0xD800 <= ord(character) <= 0xDFFF for character in text))


if __name__ == "__main__":
    unittest.main()
