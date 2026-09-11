from __future__ import annotations

import importlib.util
import sys
from collections import namedtuple
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str) -> ModuleType:
    path = REPOSITORY_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@contextmanager
def temporary_modules(modules: dict[str, ModuleType]):
    """Temporarily replace selected imports without purging unrelated modules."""
    missing = object()
    previous = {name: sys.modules.get(name, missing) for name in modules}
    sys.modules.update(modules)
    try:
        yield
    finally:
        for name, old_value in previous.items():
            if old_value is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_value


@contextmanager
def kafka_import_context():
    """Use kafka-python when installed, otherwise provide import-only test fakes."""
    try:
        from kafka import KafkaConsumer, KafkaProducer  # noqa: F401
        from kafka.structs import OffsetAndMetadata, TopicPartition  # noqa: F401
    except (ImportError, ModuleNotFoundError):
        fake_kafka = ModuleType("kafka")

        class KafkaConsumer:
            pass

        class KafkaProducer:
            pass

        fake_kafka.KafkaConsumer = KafkaConsumer
        fake_kafka.KafkaProducer = KafkaProducer
        fake_structs = ModuleType("kafka.structs")
        fake_structs.OffsetAndMetadata = namedtuple(
            "OffsetAndMetadata",
            ("offset", "metadata", "leader_epoch"),
        )
        fake_structs.TopicPartition = namedtuple(
            "TopicPartition",
            ("topic", "partition"),
        )
        with temporary_modules(
            {"kafka": fake_kafka, "kafka.structs": fake_structs}
        ):
            yield
    else:
        with nullcontext():
            yield
