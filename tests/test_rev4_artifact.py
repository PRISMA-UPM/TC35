from __future__ import annotations

import hashlib
import json
import unittest
import warnings
from pathlib import Path

import joblib
import pandas as pd

from tc35_contract import FEATURE_NAMES_V1

ROOT = Path(__file__).resolve().parents[1]
MODEL_DIRECTORY = ROOT / "model-upload" / "files" / "models" / "rev4"
MODEL_PATH = (
    MODEL_DIRECTORY
    / "random_forest_train_ceos2_eth3_rev4_ronda1_t_lim_0.5s_20estimators_all.joblib"
)
METADATA_PATH = MODEL_PATH.with_suffix(".json")


class Rev4ArtifactTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cls.model = joblib.load(MODEL_PATH)

    def test_checksum_and_feature_schema_match_metadata(self) -> None:
        digest = hashlib.sha256(MODEL_PATH.read_bytes()).hexdigest()
        self.assertEqual(digest, self.metadata["artifact_sha256"])
        self.assertEqual(tuple(self.metadata["feature_names"]), FEATURE_NAMES_V1)
        self.assertEqual(tuple(self.model.feature_names_in_), FEATURE_NAMES_V1)
        self.assertEqual(self.model.n_features_in_, 8)
        self.assertEqual(tuple(self.model.classes_), (0, 1, 2))
        self.assertEqual(
            tuple(self.metadata["label_correspondence"]),
            tuple(str(value) for value in self.model.classes_),
        )

    def test_model_accepts_one_nfstream_row(self) -> None:
        frame = pd.DataFrame(
            [[6, 0, 0, 1, 0, 64, 0, 0]],
            columns=FEATURE_NAMES_V1,
        )
        probabilities = self.model.predict_proba(frame)
        self.assertEqual(probabilities.shape, (1, 3))
        self.assertAlmostEqual(float(probabilities[0].sum()), 1.0)


if __name__ == "__main__":
    unittest.main()
