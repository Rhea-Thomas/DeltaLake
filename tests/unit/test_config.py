import pytest

from lakehouse import config


def test_kafka_options_default_empty(monkeypatch):
    monkeypatch.delenv("KAFKA_OPTIONS_JSON", raising=False)
    assert config.kafka_options() == {}


def test_kafka_options_parsed(monkeypatch):
    monkeypatch.setenv("KAFKA_OPTIONS_JSON", '{"kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN"}')
    assert config.kafka_options() == {"kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN"}


def test_kafka_options_must_be_object(monkeypatch):
    monkeypatch.setenv("KAFKA_OPTIONS_JSON", '["x"]')
    with pytest.raises(ValueError):
        config.kafka_options()
