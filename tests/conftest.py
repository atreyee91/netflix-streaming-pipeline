"""Shared test fixtures for Netflix Streaming Pipeline."""

import json
import os
import sys
from pathlib import Path

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def sample_events():
    """Load sample events from the data_generator fixtures."""
    sample_path = PROJECT_ROOT / "data_generator" / "sample_data.json"
    with open(sample_path) as f:
        return json.load(f)


@pytest.fixture
def single_event(sample_events):
    return sample_events[0]


@pytest.fixture
def env_vars(monkeypatch):
    """Set required environment variables for tests."""
    monkeypatch.setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==")
    monkeypatch.setenv("EVENTHUB_NAME", "netflix-events")
    monkeypatch.setenv("COSMOS_CONNECTION_STRING", "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=dGVzdA==;")
    monkeypatch.setenv("COSMOS_DATABASE", "netflix-streaming")
    monkeypatch.setenv("COSMOS_CONTAINER_EVENTS", "processed-events")
    monkeypatch.setenv("DATALAKE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net")
    monkeypatch.setenv("DATALAKE_CONTAINER", "raw")
