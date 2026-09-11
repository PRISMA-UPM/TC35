from __future__ import annotations

import unittest

import pandas as pd

from tc35_contract import FEATURE_NAMES_V1
from tests.helpers import load_module

training = load_module("tc35_ai_training", "ai-training/files/ai_training.py")


def training_frame() -> pd.DataFrame:
    rows = []
    for category, protocol in (
        ("Benign_traffic", 6),
        ("Benign_HH", 17),
        ("Malign_HH", 6),
    ):
        row = dict.fromkeys(FEATURE_NAMES_V1, 1.0)
        row["udps.protocol"] = protocol
        row["category"] = category
        row["src_ip"] = "192.0.2.1"
        rows.append(row)
    return pd.DataFrame(rows)


class TrainingTests(unittest.TestCase):
    def test_training_normalizes_schema_and_canonical_output_labels(self) -> None:
        features, labels = training.AITraining.preprocess_features(training_frame())
        training.AITraining.validate_training_classes(labels)
        self.assertEqual(tuple(features.columns), FEATURE_NAMES_V1)
        self.assertEqual(tuple(labels), (0, 1, 2))
        self.assertEqual(
            training.LABEL_CORRESPONDENCE,
            {
                "0": "normal_traffic",
                "1": "benign_heavy_hitter",
                "2": "malign_heavy_hitter",
            },
        )

    def test_training_rejects_nonfinite_features(self) -> None:
        frame = training_frame()
        frame.loc[0, "src2dst_bytes"] = float("inf")
        with self.assertRaisesRegex(ValueError, "finite"):
            training.AITraining.preprocess_features(frame)

    def test_training_normalizes_float_class_ids_to_integers(self) -> None:
        frame = training_frame()
        frame["category"] = [0.0, 1.0, 2.0]
        _features, labels = training.AITraining.preprocess_features(frame)
        self.assertTrue(pd.api.types.is_integer_dtype(labels.dtype))
        self.assertEqual(tuple(labels), (0, 1, 2))

    def test_training_route_rejects_non_string_dataset_id(self) -> None:
        app = training.create_app(object())
        response = app.test_client().post(
            "/train",
            json={"dataset_id": 123, "n_estimators": 20},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("non-empty string", response.get_json()["error"])


if __name__ == "__main__":
    unittest.main()
