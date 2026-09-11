"""
AI Training Service
====================
Trains, validates, and publishes machine learning models for heavy hitter discrimination.

Interfaces:
    - InitializeModelTraining(dataset_id) -> (status_message, new_model_id)
    - RetrainModel(model_id, new_dataset_id) -> (status_message, retrained_model_id)
    - publish_models(model: ModelDescriptor) -> None
"""

import os
import time
import base64
import hashlib
import logging
import argparse
import re
from datetime import datetime, timezone
from pathlib import Path
from sys import stdout
from typing import Tuple, Optional, Any, Dict
from collections import namedtuple
from urllib.parse import quote

import pandas as pd
import numpy as np
import requests
from flask import Flask, request, jsonify
from joblib import dump
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)
from tc35_contract import FEATURE_NAMES_V1, SNAPSHOT_INTERVAL_SECONDS_V1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------
ModelDescriptor = namedtuple("ModelDescriptor", [
    "model_object",     # the fitted sklearn model
    "filename",         # e.g. "random_forest_20est.joblib"
    "metadata",         # dict with name, params, metrics, label_correspondence
])

# Category mapping (from notebook)
CATEGORY_MAP = {
    'Benign_traffic': 0,
    'Benign_HH': 1,
    'Malign_HH': 2,
}

LABEL_CORRESPONDENCE = {
    "0": "normal_traffic",
    "1": "benign_heavy_hitter",
    "2": "malign_heavy_hitter",
}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SHA256_PATTERN = re.compile(r"^[a-fA-F0-9]{64}$")
REQUEST_TIMEOUT_SECONDS = 30

# Columns to drop when extracting features (from notebook)
NON_FEATURE_COLUMNS = [
    'udps.timestamp',
    'bidirectional_first_seen_ms',
    'src_ip',
    'src_port',
    'dst_ip',
    'dst_port',
    'category',
]


# ===========================================================================
# Core Training Service
# ===========================================================================
class AITraining:
    """Core AI Training service that trains, validates and publishes models."""

    def __init__(
        self,
        catalog_url: str,
        catalog_index: str,
        data_path: str,
        models_out_path: str,
    ):
        LOGGER.info("Initializing AI Training Service...")

        self.catalog_url = (
            catalog_url.rstrip("/")
            if catalog_url.startswith("http")
            else f"http://{catalog_url.rstrip('/')}"
        )
        self.catalog_index = catalog_index
        self.data_path = data_path
        self.models_out_path = models_out_path

        LOGGER.info("AI Catalog URL: %s", self.catalog_url)
        LOGGER.info("Catalog index: %s", self.catalog_index)
        LOGGER.info("Data path: %s", self.data_path)

        os.makedirs(self.models_out_path, exist_ok=True)
        LOGGER.info("Models output path: %s", self.models_out_path)
        LOGGER.info("AI Training Service initialized successfully.")

    # -------------------------------------------------------------------
    # Data Loading & Preprocessing
    # -------------------------------------------------------------------
    @staticmethod
    def preprocess_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Drops non-feature columns and encodes category labels.
        Returns (features, labels).
        """
        cols_to_drop = [c for c in NON_FEATURE_COLUMNS if c in df.columns]
        if "category" not in df.columns:
            raise ValueError("dataset must contain a category column")
        features = df.drop(columns=cols_to_drop)
        missing = [name for name in FEATURE_NAMES_V1 if name not in features.columns]
        extra = sorted(set(features.columns).difference(FEATURE_NAMES_V1))
        if missing or extra:
            raise ValueError(
                f"dataset feature schema mismatch; missing={missing}, extra={extra}"
            )
        features = features.loc[:, list(FEATURE_NAMES_V1)]
        if any(pd.api.types.is_bool_dtype(dtype) for dtype in features.dtypes):
            raise ValueError("dataset features must be numeric, not boolean")
        try:
            features = features.apply(pd.to_numeric, errors="raise")
        except (TypeError, ValueError) as exc:
            raise ValueError("dataset features must all be numeric") from exc
        if not np.isfinite(features.to_numpy(dtype=float)).all():
            raise ValueError("dataset features must all be finite")
        labels = df["category"].apply(
            lambda value: CATEGORY_MAP.get(value, value)
        )
        if labels.isna().any() or not labels.isin(CATEGORY_MAP.values()).all():
            unknown = sorted(
                {
                    repr(value)
                    for value in df.loc[
                        ~labels.isin(CATEGORY_MAP.values()),
                        "category",
                    ]
                }
            )
            raise ValueError(f"dataset contains unknown categories: {unknown}")
        # Numeric CSV labels are commonly inferred as floats (0.0, 1.0, 2.0).
        # Store the canonical integer class IDs so inference sees classes_
        # exactly as declared by the model metadata.
        return features, labels.astype("int64")

    @staticmethod
    def validate_training_classes(labels: pd.Series) -> None:
        expected = set(CATEGORY_MAP.values())
        observed = {int(label) for label in labels.unique()}
        if observed != expected:
            raise ValueError(
                f"training data must contain class IDs {sorted(expected)}; "
                f"found {sorted(observed)}"
            )

    def load_dataset(self, dataset_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Loads training and validation CSVs for a given dataset_id.
        Expects files named:
            {dataset_id}_training.csv
            {dataset_id}_validation.csv
        inside self.data_path.
        """
        if not IDENTIFIER_PATTERN.fullmatch(dataset_id):
            raise ValueError("dataset_id contains unsupported characters")
        train_path = os.path.join(self.data_path, f"{dataset_id}_training.csv")
        val_path = os.path.join(self.data_path, f"{dataset_id}_validation.csv")

        if not os.path.isfile(train_path):
            raise FileNotFoundError(f"Training data not found: {train_path}")
        if not os.path.isfile(val_path):
            raise FileNotFoundError(f"Validation data not found: {val_path}")

        LOGGER.info("Loading training data from %s ...", train_path)
        df_train = pd.read_csv(train_path, delimiter='*')
        LOGGER.info("Training samples: %d", len(df_train))

        LOGGER.info("Loading validation data from %s ...", val_path)
        df_val = pd.read_csv(val_path, delimiter='*')
        LOGGER.info("Validation samples: %d", len(df_val))

        return df_train, df_val

    # -------------------------------------------------------------------
    # Training
    # -------------------------------------------------------------------
    @staticmethod
    def train_model(features: pd.DataFrame, labels: pd.Series,
                    n_estimators: int = 20) -> RandomForestClassifier:
        """Trains a RandomForestClassifier (mirrors notebook configuration)."""
        if isinstance(n_estimators, bool) or not isinstance(n_estimators, int):
            raise ValueError("n_estimators must be an integer")
        if not 1 <= n_estimators <= 1000:
            raise ValueError("n_estimators must be between 1 and 1000")
        LOGGER.info("Training RandomForest with %d estimators...", n_estimators)

        model = RandomForestClassifier(
            n_estimators=n_estimators,
            random_state=42,
            n_jobs=-1
        )

        start = time.time()
        model.fit(features, labels)
        elapsed = int(time.time() - start)

        LOGGER.info("Training completed in %d min %d sec.", elapsed // 60, elapsed % 60)
        return model

    # -------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------
    @staticmethod
    def validate_model(model: RandomForestClassifier,
                       features: pd.DataFrame,
                       labels: pd.Series) -> Dict[str, Any]:
        """
        Validates the model and returns metrics dict.
        Mirrors the notebook's get_report function.
        """
        LOGGER.info("Validating model...")
        y_pred = model.predict(features)

        accuracy = float(accuracy_score(labels, y_pred))
        precision = float(precision_score(labels, y_pred, average='macro'))
        recall = float(recall_score(labels, y_pred, average='macro'))
        f1 = float(f1_score(labels, y_pred, average='macro'))
        conf_matrix = confusion_matrix(labels, y_pred).tolist()
        report = classification_report(labels, y_pred, output_dict=True)

        LOGGER.info("Accuracy:  %.4f", accuracy)
        LOGGER.info("Precision: %.4f", precision)
        LOGGER.info("Recall:    %.4f", recall)
        LOGGER.info("F1 Score:  %.4f", f1)
        LOGGER.info("Confusion Matrix:\n%s", confusion_matrix(labels, y_pred))
        LOGGER.info("Classification Report:\n%s",
                     classification_report(labels, y_pred))

        return {
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "confusion_matrix": conf_matrix,
            "classification_report": report,
        }

    # -------------------------------------------------------------------
    # Publish / Upload
    # -------------------------------------------------------------------
    def publish_models(self, model_desc: ModelDescriptor) -> Optional[str]:
        """
        Serializes a trained model to .joblib, base64-encodes it,
        and POSTs to the AI Catalog (Elasticsearch).

        Mirrors the pattern from model-upload/files/model_upload.py.

        Returns the new model document ID on success, or None on failure.
        """
        # Save model to disk
        local_path = os.path.join(self.models_out_path, model_desc.filename)
        dump(model_desc.model_object, local_path)
        LOGGER.info("Model saved locally: %s", local_path)

        # Read and base64-encode
        with open(local_path, "rb") as f:
            file_content = base64.b64encode(f.read()).decode("ascii")

        artifact = Path(local_path).read_bytes()
        digest = hashlib.sha256(artifact).hexdigest()
        metadata = dict(model_desc.metadata)
        metadata.update({
            "model": model_desc.filename,
            "artifact_sha256": digest,
            "input_schema": "nfstream-v1",
            "feature_names": list(FEATURE_NAMES_V1),
            "snapshot_interval_seconds": SNAPSHOT_INTERVAL_SECONDS_V1,
            "training_library": {"scikit-learn": "1.0.2"},
        })

        payload = {
            "file_data": {
                "filename": model_desc.filename,
                "file_content": file_content,
                "content_type": "application/octet-stream",
                "size_bytes": len(artifact),
                "sha256": digest,
            },
            "metadata": metadata,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }

        api_url = (
            f"{self.catalog_url}/{quote(self.catalog_index, safe='')}/_doc/"
            f"{digest}?refresh=wait_for"
        )

        LOGGER.info("Uploading model to AI Catalog: %s", api_url)

        try:
            response = requests.put(
                api_url,
                json=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

            if response.status_code in (200, 201):
                doc_id = response.json().get("_id", digest)
                LOGGER.info("Model [%s] uploaded successfully. ID: %s",
                            model_desc.filename, doc_id)
                return doc_id
            else:
                LOGGER.error("Upload failed: %s - %s", response.status_code, response.text)
                return None

        except requests.RequestException as e:
            LOGGER.error("Error uploading model: %s", e)
            return None

    # -------------------------------------------------------------------
    # Retrieve existing model metadata from AI Catalog (for retraining)
    # -------------------------------------------------------------------
    def get_model_metadata_from_catalog(self, model_id: str) -> Dict[str, Any]:
        """
        Retrieves metadata for an existing model without deserializing it.
        """
        if not SHA256_PATTERN.fullmatch(model_id):
            raise ValueError("model_id must be a SHA-256 document ID")
        api_url = (
            f"{self.catalog_url}/{quote(self.catalog_index, safe='')}/_doc/"
            f"{quote(model_id, safe='')}?_source_includes=metadata"
        )

        LOGGER.info("Retrieving model %s from AI Catalog...", model_id)

        response = requests.get(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        result = response.json()

        metadata = result["_source"].get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("catalog model metadata must be an object")
        return metadata

    # -------------------------------------------------------------------
    # High-level API Methods
    # -------------------------------------------------------------------
    def initialize_model_training(self, dataset_id: str,
                                  n_estimators: int = 20) -> Tuple[str, Optional[str]]:
        """
        InitializeModelTraining(dataset_id) → (status_message, new_model_id)

        Full pipeline: load data → preprocess → train → validate → publish.
        """
        LOGGER.info("=== InitializeModelTraining(dataset_id=%s) ===", dataset_id)

        try:
            # 1. Load data
            df_train, df_val = self.load_dataset(dataset_id)

            # 2. Preprocess
            features_train, labels_train = self.preprocess_features(df_train)
            features_val, labels_val = self.preprocess_features(df_val)
            self.validate_training_classes(labels_train)

            # 3. Train
            model = self.train_model(features_train, labels_train, n_estimators)

            # 4. Validate
            metrics = self.validate_model(model, features_val, labels_val)

            # 5. Publish
            timestamp = int(time.time())
            filename = f"random_forest_{n_estimators}est_{dataset_id}_{timestamp}.joblib"

            metadata = {
                "name": f"RandomForest {n_estimators} estimators - {dataset_id}",
                "dataset_id": dataset_id,
                "params": {
                    "n_estimators": n_estimators,
                    "random_state": 42,
                    "n_jobs": -1,
                },
                "metrics": metrics,
                "label_correspondence": LABEL_CORRESPONDENCE,
                "trained_at": timestamp,
            }

            model_desc = ModelDescriptor(
                model_object=model,
                filename=filename,
                metadata=metadata,
            )

            new_model_id = self.publish_models(model_desc)

            if new_model_id:
                msg = (f"Training completed successfully. "
                       f"F1={metrics['f1_score']:.4f}, Accuracy={metrics['accuracy']:.4f}. "
                       f"Model ID: {new_model_id}")
                LOGGER.info(msg)
                return msg, new_model_id
            else:
                msg = "Training succeeded but model upload to AI Catalog failed."
                LOGGER.error(msg)
                return msg, None

        except Exception as e:
            msg = f"Training failed: {e}"
            LOGGER.exception(msg)
            return msg, None

    def retrain_model(
        self,
        model_id: str,
        new_dataset_id: str,
        n_estimators: int | None = None,
    ) -> Tuple[str, Optional[str]]:
        """
        RetrainModel(model_id, new_dataset_id) → (status_message, retrained_model_id)

        Downloads existing model metadata, retrains on new data, validates, publishes.
        """
        LOGGER.info("=== RetrainModel(model_id=%s, new_dataset_id=%s) ===",
                     model_id, new_dataset_id)

        try:
            # 1. Retrieve existing model metadata from catalog
            old_metadata = self.get_model_metadata_from_catalog(model_id)
            old_params = old_metadata.get("params", {})
            estimator_source = "request override"
            if n_estimators is None:
                n_estimators = old_params.get("n_estimators", 20)
                estimator_source = "original model metadata"
            LOGGER.info(
                "Retraining with n_estimators=%s (%s)",
                n_estimators,
                estimator_source,
            )

            # 2. Load new data
            df_train, df_val = self.load_dataset(new_dataset_id)

            # 3. Preprocess
            features_train, labels_train = self.preprocess_features(df_train)
            features_val, labels_val = self.preprocess_features(df_val)
            self.validate_training_classes(labels_train)

            # 4. Train fresh model with same hyperparameters
            model = self.train_model(features_train, labels_train, n_estimators)

            # 5. Validate
            metrics = self.validate_model(model, features_val, labels_val)

            # 6. Publish
            timestamp = int(time.time())
            filename = f"random_forest_{n_estimators}est_{new_dataset_id}_retrained_{timestamp}.joblib"

            metadata = {
                "name": f"RandomForest {n_estimators} est (retrained) - {new_dataset_id}",
                "dataset_id": new_dataset_id,
                "retrained_from": model_id,
                "params": {
                    "n_estimators": n_estimators,
                    "random_state": 42,
                    "n_jobs": -1,
                },
                "metrics": metrics,
                "label_correspondence": LABEL_CORRESPONDENCE,
                "trained_at": timestamp,
            }

            model_desc = ModelDescriptor(
                model_object=model,
                filename=filename,
                metadata=metadata,
            )

            retrained_id = self.publish_models(model_desc)

            if retrained_id:
                msg = (f"Retraining completed successfully. "
                       f"F1={metrics['f1_score']:.4f}, Accuracy={metrics['accuracy']:.4f}. "
                       f"Retrained model ID: {retrained_id}")
                LOGGER.info(msg)
                return msg, retrained_id
            else:
                msg = "Retraining succeeded but model upload to AI Catalog failed."
                LOGGER.error(msg)
                return msg, None

        except Exception as e:
            msg = f"Retraining failed: {e}"
            LOGGER.exception(msg)
            return msg, None


# ===========================================================================
# Flask API
# ===========================================================================
def create_app(training_service: AITraining) -> Flask:
    """Creates the Flask application with training API routes."""
    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "service": "ai-training"}), 200

    @app.route("/train", methods=["POST"])
    def train():
        """
        InitializeModelTraining(dataset_id) → (status_message, new_model_id)

        Request body:
            { "dataset_id": "ceos2_eth3_rev4", "n_estimators": 20 }
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "request body must be a JSON object"}), 400
        dataset_id = body.get("dataset_id")

        if not isinstance(dataset_id, str) or not dataset_id:
            return jsonify({"error": "dataset_id must be a non-empty string"}), 400
        if not IDENTIFIER_PATTERN.fullmatch(dataset_id):
            return jsonify({"error": "dataset_id contains unsupported characters"}), 400

        n_estimators = body.get("n_estimators", 20)
        if (
            isinstance(n_estimators, bool)
            or not isinstance(n_estimators, int)
            or not 1 <= n_estimators <= 1000
        ):
            return jsonify({"error": "n_estimators must be an integer from 1 to 1000"}), 400

        status_message, new_model_id = training_service.initialize_model_training(
            dataset_id=dataset_id,
            n_estimators=n_estimators,
        )

        status_code = 201 if new_model_id else 500
        return jsonify({
            "status_message": status_message,
            "new_model_id": new_model_id,
        }), status_code

    @app.route("/retrain", methods=["POST"])
    def retrain():
        """
        RetrainModel(model_id, new_dataset_id) → (status_message, retrained_model_id)

        Request body:
            { "model_id": "abc123", "new_dataset_id": "ceos2_eth3_rev5", "n_estimators": 20 }
        """
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "request body must be a JSON object"}), 400
        model_id = body.get("model_id")
        new_dataset_id = body.get("new_dataset_id")

        if not isinstance(model_id, str) or not model_id:
            return jsonify({"error": "model_id must be a non-empty string"}), 400
        if not SHA256_PATTERN.fullmatch(model_id):
            return jsonify({"error": "model_id must be a SHA-256 document ID"}), 400
        if not isinstance(new_dataset_id, str) or not new_dataset_id:
            return jsonify(
                {"error": "new_dataset_id must be a non-empty string"}
            ), 400
        if not IDENTIFIER_PATTERN.fullmatch(new_dataset_id):
            return jsonify(
                {"error": "new_dataset_id contains unsupported characters"}
            ), 400

        n_estimators = body.get("n_estimators")
        if (
            n_estimators is not None
            and (
                isinstance(n_estimators, bool)
                or not isinstance(n_estimators, int)
                or not 1 <= n_estimators <= 1000
            )
        ):
            return jsonify({"error": "n_estimators must be an integer from 1 to 1000"}), 400

        status_message, retrained_model_id = training_service.retrain_model(
            model_id=model_id,
            new_dataset_id=new_dataset_id,
            n_estimators=n_estimators,
        )

        status_code = 201 if retrained_model_id else 500
        return jsonify({
            "status_message": status_message,
            "retrained_model_id": retrained_model_id,
        }), status_code

    return app


# ===========================================================================
# Entrypoint
# ===========================================================================
def main(args):
    training_service = AITraining(
        catalog_url=args.catalog_url,
        catalog_index=args.catalog_index,
        data_path=args.data_path,
        models_out_path=args.models_out_path,
    )

    app = create_app(training_service)

    LOGGER.info("Starting AI Training API on port %s", args.port)
    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AI Training Service — train, validate and publish ML models."
    )
    parser.add_argument(
        "--catalog_url", type=str, default=os.getenv("CATALOG_URL", "http://ai-repository:9200"),
        help="URL of the AI Catalog (Elasticsearch)."
    )
    parser.add_argument(
        "--catalog_index", type=str, default=os.getenv("CATALOG_INDEX", "models"),
        help="Elasticsearch index for storing models. Default: models"
    )
    parser.add_argument(
        "--data_path", type=str, default=os.getenv("DATA_PATH", "./data/"),
        help="Path to directory containing training/validation CSV files. Default: ./data/"
    )
    parser.add_argument(
        "--models_out_path",
        type=str,
        default=os.getenv("MODELS_OUT_PATH", "./models/"),
        help="Writable path for generated model artifacts. Default: ./models/",
    )
    parser.add_argument(
        "--port", type=int, default=int(os.getenv("PORT", "5050")),
        help="Port for the Flask API server. Default: 5050"
    )

    args = parser.parse_args()
    main(args)
