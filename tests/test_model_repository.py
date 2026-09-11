from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from tests.helpers import kafka_import_context, load_module

model_upload = load_module(
    "tc35_model_upload",
    "model-upload/files/model_upload.py",
)
with kafka_import_context():
    inference = load_module(
        "tc35_model_repository_inference",
        "ai-inference/files/ai_inference.py",
    )


class FakeResponse:
    status_code = 201

    @staticmethod
    def raise_for_status() -> None:
        return None


class FakeSession:
    def __init__(self) -> None:
        self.calls = []

    def put(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse()


class ModelRepositoryTests(unittest.TestCase):
    def test_document_id_is_artifact_checksum_and_extension_uses_last_dot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "model_t_lim_0.5s.joblib"
            artifact.write_bytes(b"model bytes")
            digest, document = model_upload.build_document(
                artifact,
                {"model": artifact.name},
            )

        self.assertEqual(digest, hashlib.sha256(b"model bytes").hexdigest())
        self.assertEqual(document["file_data"]["sha256"], digest)
        self.assertEqual(
            document["file_data"]["content_type"],
            "application/octet-stream",
        )
        self.assertEqual(document["metadata"]["artifact_sha256"], digest)

    def test_upload_uses_idempotent_put(self) -> None:
        session = FakeSession()
        model_upload.upload_model(
            session,
            "http://catalog:9200",
            "models",
            "abc123",
            {"file_data": {}},
        )
        self.assertEqual(len(session.calls), 1)
        url, kwargs = session.calls[0]
        self.assertEqual(
            url,
            "http://catalog:9200/models/_doc/abc123?refresh=wait_for",
        )
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["timeout"], 30)

    def test_unsafe_filename_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            model_upload.safe_model_path(".", "../model.joblib")

    def test_model_discovery_skips_a_malformed_unrelated_hit(self) -> None:
        class SearchResponse:
            @staticmethod
            def raise_for_status() -> None:
                return None

            @staticmethod
            def json():
                return {
                    "hits": {
                        "hits": [
                            {
                                "_id": "bad",
                                "_source": {
                                    "file_data": {"filename": "../bad.joblib"},
                                    "metadata": {},
                                },
                            },
                            {
                                "_id": "good",
                                "_source": {
                                    "file_data": {
                                        "filename": "good.joblib",
                                        "sha256": "abc",
                                    },
                                    "metadata": {"feature_names": ["feature"]},
                                },
                            },
                        ]
                    }
                }

        class SearchSession:
            @staticmethod
            def post(*_args, **_kwargs):
                return SearchResponse()

        service = inference.AIInference.__new__(inference.AIInference)
        service.catalog_url = "http://catalog:9200"
        service.models_index = "models"
        service.session = SearchSession()
        records = service.list_models()
        self.assertEqual([record.document_id for record in records], ["good"])


if __name__ == "__main__":
    unittest.main()
