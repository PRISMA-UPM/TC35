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
import json
import time
import base64
import signal
import logging
import argparse
from sys import stdout
from typing import Tuple, Optional, Any, Dict
from collections import namedtuple

import numpy as np
import pandas as pd
import requests
from flask import Flask, request, jsonify
from joblib import load, dump
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)

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

LABEL_CORRESPONDENCE = {str(v): k for k, v in CATEGORY_MAP.items()}

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

    def __init__(self, catalog_url: str, catalog_index: str, data_path: str):
        LOGGER.info("Initializing AI Training Service...")

        self.catalog_url = catalog_url if catalog_url.startswith("http") else f"http://{catalog_url}"
        self.catalog_index = catalog_index
        self.data_path = data_path
        self.models_out_path = os.path.join(data_path, "models")

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
        features = df.drop(columns=cols_to_drop)
        labels = df["category"].replace(CATEGORY_MAP)
        return features, labels

    def load_dataset(self, dataset_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Loads training and validation CSVs for a given dataset_id.
        Expects files named:
            {dataset_id}_training.csv
            {dataset_id}_validation.csv
        inside self.data_path.
        """
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

        extension = model_desc.filename.rsplit(".", 1)[-1]

        payload = {
            "file_data": {
                "filename": model_desc.filename,
                "file_content": file_content,
                "content_type": f"file/{extension}",
            },
            "metadata": model_desc.metadata,
        }

        api_url = f"{self.catalog_url}/{self.catalog_index}/_doc"
        headers = {"Content-Type": "application/json"}

        LOGGER.info("Uploading model to AI Catalog: %s", api_url)

        try:
            response = requests.post(api_url, data=json.dumps(payload), headers=headers)

            if response.status_code == 201:
                doc_id = response.json().get("_id")
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
    # Retrieve existing model from AI Catalog (for retraining)
    # -------------------------------------------------------------------
    def get_model_from_catalog(self, model_id: str) -> Tuple[Any, Dict]:
        """
        Downloads an existing model from the AI Catalog by its document ID.
        Returns (sklearn_model, metadata_dict).
        """
        api_url = f"{self.catalog_url}/{self.catalog_index}/_doc/{model_id}"
        headers = {"Content-Type": "application/json"}

        LOGGER.info("Retrieving model %s from AI Catalog...", model_id)

        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        result = response.json()

        filename = result["_source"]["file_data"]["filename"]
        file_content = result["_source"]["file_data"]["file_content"]
        metadata = result["_source"].get("metadata", {})

        file_bytes = base64.b64decode(file_content.encode("utf-8"))
        local_path = os.path.join(self.models_out_path, filename)

        with open(local_path, "wb") as f:
            f.write(file_bytes)

        LOGGER.info("Model %s downloaded to %s", filename, local_path)

        model = load(local_path)
        return model, metadata

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

    def retrain_model(self, model_id: str, new_dataset_id: str,
                      n_estimators: int = 20) -> Tuple[str, Optional[str]]:
        """
        RetrainModel(model_id, new_dataset_id) → (status_message, retrained_model_id)

        Downloads existing model metadata, retrains on new data, validates, publishes.
        """
        LOGGER.info("=== RetrainModel(model_id=%s, new_dataset_id=%s) ===",
                     model_id, new_dataset_id)

        try:
            # 1. Retrieve existing model metadata from catalog
            _, old_metadata = self.get_model_from_catalog(model_id)
            old_params = old_metadata.get("params", {})
            n_estimators = old_params.get("n_estimators", n_estimators)
            LOGGER.info("Retraining with n_estimators=%d (from original model)", n_estimators)

            # 2. Load new data
            df_train, df_val = self.load_dataset(new_dataset_id)

            # 3. Preprocess
            features_train, labels_train = self.preprocess_features(df_train)
            features_val, labels_val = self.preprocess_features(df_val)

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
        body = request.get_json(force=True)
        dataset_id = body.get("dataset_id")

        if not dataset_id:
            return jsonify({"error": "dataset_id is required"}), 400

        n_estimators = body.get("n_estimators", 20)

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
        body = request.get_json(force=True)
        model_id = body.get("model_id")
        new_dataset_id = body.get("new_dataset_id")

        if not model_id:
            return jsonify({"error": "model_id is required"}), 400
        if not new_dataset_id:
            return jsonify({"error": "new_dataset_id is required"}), 400

        n_estimators = body.get("n_estimators", 20)

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
    )

    app = create_app(training_service)

    LOGGER.info("Starting AI Training API on port %s", args.port)
    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AI Training Service — train, validate and publish ML models."
    )
    parser.add_argument(
        "--catalog_url", type=str, default="http://localhost:9200",
        help="URL of the AI Catalog (Elasticsearch). Default: http://localhost:9200"
    )
    parser.add_argument(
        "--catalog_index", type=str, default="models",
        help="Elasticsearch index for storing models. Default: models"
    )
    parser.add_argument(
        "--data_path", type=str, default="./data/",
        help="Path to directory containing training/validation CSV files. Default: ./data/"
    )
    parser.add_argument(
        "--port", type=int, default=5050,
        help="Port for the Flask API server. Default: 5050"
    )

    args = parser.parse_args()
    main(args)
