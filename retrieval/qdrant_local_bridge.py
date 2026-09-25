import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from fastembed import SparseTextEmbedding, TextEmbedding
from qdrant_client import QdrantClient, models

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


def decode_command(raw: bytes) -> dict[str, Any]:
    """Decode the Rust bridge protocol as strict UTF-8 on every platform."""
    command = json.loads(raw.decode("utf-8", errors="strict"))
    if not isinstance(command, dict):
        raise TypeError("bridge command must be a JSON object")
    return command


def encode_result(result: dict[str, Any]) -> bytes:
    """Encode bridge responses as UTF-8 instead of the Windows console code page."""
    return json.dumps(result, ensure_ascii=False).encode("utf-8")


def text_input(value: Any, field: str) -> str:
    """Normalize bridge text at the process boundary before it reaches FastEmbed."""
    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, (list, tuple)) and all(
        isinstance(item, str) for item in value
    ):
        # Compatibility for requests emitted by early UI/retrieval builds.
        text = "\n".join(item.strip() for item in value if item.strip())
    else:
        raise TypeError(
            f"{field} must be a string or a sequence of strings; received "
            f"{type(value).__name__}"
        )
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text


class LocalQdrantBridge:
    def __init__(self, storage: Path, model_cache: Path, collection: str) -> None:
        storage.mkdir(parents=True, exist_ok=True)
        model_cache.mkdir(parents=True, exist_ok=True)
        self.collection = collection
        self.dense_model_name = "sentence-transformers/all-MiniLM-L6-v2"
        self.sparse_model_name = "Qdrant/bm25"
        self.client = QdrantClient(path=str(storage))
        self.dense = TextEmbedding(
            model_name=self.dense_model_name,
            cache_dir=str(model_cache),
        )
        self.sparse = SparseTextEmbedding(
            model_name=self.sparse_model_name,
            cache_dir=str(model_cache),
        )
        probe = self._dense_vector("dimension probe")
        if not self.client.collection_exists(collection):
            self.client.create_collection(
                collection_name=collection,
                vectors_config={
                    "dense": models.VectorParams(
                        size=len(probe),
                        distance=models.Distance.COSINE,
                    )
                },
                sparse_vectors_config={
                    "sparse": models.SparseVectorParams(
                        modifier=models.Modifier.IDF,
                    )
                },
            )

    def _dense_vector(self, text: Any) -> list[float]:
        text = text_input(text, "dense embedding input")
        # FastEmbed accepts either one string or an iterable. Passing the
        # normalized scalar directly lets FastEmbed own batching and avoids
        # an extra sequence layer reaching tokenizers.encode_batch.
        return list(next(self.dense.embed(text)))

    def _sparse_vector(self, text: Any) -> models.SparseVector:
        text = text_input(text, "sparse embedding input")
        embedding = next(self.sparse.embed(text))
        return models.SparseVector(
            indices=embedding.indices.tolist(),
            values=embedding.values.tolist(),
        )

    def upsert(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        points = []
        for record in records:
            text = text_input(record.get("text"), "record.text")
            record_id = record.get("id", "<missing>")
            try:
                points.append(
                    models.PointStruct(
                        id=record["id"],
                        vector={
                            "dense": self._dense_vector(text),
                            "sparse": self._sparse_vector(text),
                        },
                        payload=record,
                    )
                )
            except Exception as error:
                raise RuntimeError(
                    "embedding canonical context record "
                    f"{record_id} failed "
                    f"(event={record.get('canonical_event_id', '<missing>')}, "
                    f"title={record.get('title', '<missing>')!r}, "
                    f"text_type={type(text).__name__}, text_length={len(text)})"
                ) from error
        if points:
            self.client.upsert(
                collection_name=self.collection,
                points=points,
                wait=True,
            )
        return {"upserted": len(points)}

    def _filter(self, filters: dict[str, Any]) -> models.Filter | None:
        conditions = []
        for key in ("run_id", "source_class", "trust_level", "canonical_entity_type"):
            value = filters.get(key)
            if value:
                conditions.append(
                    models.FieldCondition(
                        key=key,
                        match=models.MatchValue(value=value),
                    )
                )
        return models.Filter(must=conditions) if conditions else None

    def search(self, command: dict[str, Any]) -> dict[str, Any]:
        query = text_input(command.get("query"), "search.query")
        mode = command.get("mode", "hybrid")
        limit = int(command.get("limit", 8))
        query_filter = self._filter(command.get("filters", {}))
        if mode == "exact":
            canonical_id = command.get("canonical_entity_id", query)
            exact_filter = models.Filter(
                must=[
                    models.FieldCondition(
                        key="canonical_entity_id",
                        match=models.MatchValue(value=canonical_id),
                    )
                ]
                + (query_filter.must if query_filter else [])
            )
            points, _ = self.client.scroll(
                collection_name=self.collection,
                scroll_filter=exact_filter,
                limit=limit,
                with_payload=True,
                with_vectors=False,
            )
            return {"hits": [self._hit(point, 1.0) for point in points]}

        dense = self._dense_vector(query)
        sparse = self._sparse_vector(query)
        if mode == "semantic":
            response = self.client.query_points(
                collection_name=self.collection,
                query=dense,
                using="dense",
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
            )
        elif mode == "keyword":
            response = self.client.query_points(
                collection_name=self.collection,
                query=sparse,
                using="sparse",
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
            )
        else:
            response = self.client.query_points(
                collection_name=self.collection,
                prefetch=[
                    models.Prefetch(
                        query=dense,
                        using="dense",
                        filter=query_filter,
                        limit=max(limit * 3, 20),
                    ),
                    models.Prefetch(
                        query=sparse,
                        using="sparse",
                        filter=query_filter,
                        limit=max(limit * 3, 20),
                    ),
                ],
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                limit=limit,
                with_payload=True,
            )
        return {
            "hits": [self._hit(point, float(point.score)) for point in response.points]
        }

    def inspect(self, ids: list[str]) -> dict[str, Any]:
        points = self.client.retrieve(
            collection_name=self.collection,
            ids=ids,
            with_payload=True,
            with_vectors=False,
        )
        return {"records": [point.payload for point in points]}

    @staticmethod
    def _hit(point: Any, score: float) -> dict[str, Any]:
        payload = dict(point.payload or {})
        payload["score"] = score
        payload["point_id"] = str(point.id)
        return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", required=True)
    parser.add_argument("--models", required=True)
    parser.add_argument("--collection", required=True)
    args = parser.parse_args()
    command = decode_command(sys.stdin.buffer.read())
    bridge = LocalQdrantBridge(
        Path(args.storage),
        Path(args.models),
        args.collection,
    )
    action = command["action"]
    if action == "init":
        result = {"ready": True, "collection": args.collection}
    elif action == "upsert":
        result = bridge.upsert(command.get("records", []))
    elif action == "search":
        result = bridge.search(command)
    elif action == "inspect":
        result = bridge.inspect(command.get("ids", []))
    else:
        raise ValueError(f"unsupported action: {action}")
    sys.stdout.buffer.write(encode_result(result))


if __name__ == "__main__":
    main()
