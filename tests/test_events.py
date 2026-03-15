import pytest
import threading
import time
from queue import Queue
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from app.main import app
from app.queues import email_queue, sms_queue, push_queue
from app.workers import process_event

client = TestClient(app)



# Helpers


def drain_queue(q: Queue):
    """Empty a queue completely (used for test isolation)."""
    while not q.empty():
        try:
            q.get_nowait()
            q.task_done()
        except Exception:
            break


@pytest.fixture(autouse=True)
def clear_queues():
    """Before every test, drain all three queues so tests don't interfere."""
    drain_queue(email_queue)
    drain_queue(sms_queue)
    drain_queue(push_queue)
    yield
    drain_queue(email_queue)
    drain_queue(sms_queue)
    drain_queue(push_queue)



# 1. API Layer Tests


class TestAPILayer:

    def test_home_endpoint(self):
        """GET / should return a running message."""
        response = client.get("/")
        assert response.status_code == 200
        assert "message" in response.json()

    def test_valid_email_event_submission(self):
        """Valid EMAIL event returns 200 with eventId."""
        response = client.post("/api/events", json={
            "eventType": "EMAIL",
            "payload": {"recipient": "test@example.com", "message": "Hello"},
            "callbackUrl": "http://example.com/callback"
        })
        assert response.status_code == 200
        data = response.json()
        assert "eventId" in data
        assert data["message"] == "Event accepted for processing"

    def test_valid_sms_event_submission(self):
        """Valid SMS event returns 200 with eventId."""
        response = client.post("/api/events", json={
            "eventType": "SMS",
            "payload": {"phoneNumber": "+911234567890", "message": "OTP is 123456"},
            "callbackUrl": "http://example.com/callback"
        })
        assert response.status_code == 200
        assert "eventId" in response.json()

    def test_valid_push_event_submission(self):
        """Valid PUSH event returns 200 with eventId."""
        response = client.post("/api/events", json={
            "eventType": "PUSH",
            "payload": {"deviceId": "abc-123", "message": "Order shipped!"},
            "callbackUrl": "http://example.com/callback"
        })
        assert response.status_code == 200
        assert "eventId" in response.json()

    def test_invalid_event_type_returns_422(self):
        """Invalid eventType should return 422 Unprocessable Entity."""
        response = client.post("/api/events", json={
            "eventType": "INVALID",
            "payload": {},
            "callbackUrl": "http://example.com/callback"
        })
        assert response.status_code == 422

    def test_missing_event_type_returns_422(self):
        """Missing eventType field should return 422."""
        response = client.post("/api/events", json={
            "payload": {"message": "Hello"},
            "callbackUrl": "http://example.com/callback"
        })
        assert response.status_code == 422

    def test_missing_callback_url_returns_422(self):
        """Missing callbackUrl field should return 422."""
        response = client.post("/api/events", json={
            "eventType": "EMAIL",
            "payload": {"message": "Hello"}
        })
        assert response.status_code == 422

    def test_missing_payload_returns_422(self):
        """Missing payload field should return 422."""
        response = client.post("/api/events", json={
            "eventType": "EMAIL",
            "callbackUrl": "http://example.com/callback"
        })
        assert response.status_code == 422

    def test_empty_body_returns_422(self):
        """Completely empty body should return 422."""
        response = client.post("/api/events", json={})
        assert response.status_code == 422

    def test_event_id_is_unique(self):
        """Each submitted event should receive a unique eventId."""
        ids = set()
        for _ in range(5):
            response = client.post("/api/events", json={
                "eventType": "PUSH",
                "payload": {"deviceId": "xyz"},
                "callbackUrl": "http://example.com/callback"
            })
            ids.add(response.json()["eventId"])
        assert len(ids) == 5


# 2. Queue Routing Tests

class TestQueueRouting:

    def test_email_event_goes_to_email_queue(self):
        """EMAIL event must be enqueued in email_queue only."""
        before = email_queue.qsize()
        client.post("/api/events", json={
            "eventType": "EMAIL",
            "payload": {"recipient": "a@b.com", "message": "hi"},
            "callbackUrl": "http://example.com/callback"
        })
        assert email_queue.qsize() == before + 1
        assert sms_queue.qsize() == 0
        assert push_queue.qsize() == 0

    def test_sms_event_goes_to_sms_queue(self):
        """SMS event must be enqueued in sms_queue only."""
        client.post("/api/events", json={
            "eventType": "SMS",
            "payload": {"phoneNumber": "+91999", "message": "otp"},
            "callbackUrl": "http://example.com/callback"
        })
        assert sms_queue.qsize() == 1
        assert email_queue.qsize() == 0
        assert push_queue.qsize() == 0

    def test_push_event_goes_to_push_queue(self):
        """PUSH event must be enqueued in push_queue only."""
        client.post("/api/events", json={
            "eventType": "PUSH",
            "payload": {"deviceId": "d1", "message": "msg"},
            "callbackUrl": "http://example.com/callback"
        })
        assert push_queue.qsize() == 1
        assert email_queue.qsize() == 0
        assert sms_queue.qsize() == 0

    def test_fifo_order_in_queue(self):
        """Events must be dequeued in the same order they were enqueued (FIFO)."""
        q = Queue()
        items = ["first", "second", "third"]
        for item in items:
            q.put(item)
        dequeued = [q.get() for _ in range(len(items))]
        assert dequeued == items

    def test_multiple_events_stack_in_queue(self):
        """Submitting 3 EMAIL events should result in queue size 3."""
        for i in range(3):
            client.post("/api/events", json={
                "eventType": "EMAIL",
                "payload": {"recipient": f"user{i}@x.com", "message": "hi"},
                "callbackUrl": "http://example.com/callback"
            })
        assert email_queue.qsize() == 3

    def test_event_data_stored_correctly_in_queue(self):
        """Event data placed in queue must contain all required fields."""
        client.post("/api/events", json={
            "eventType": "SMS",
            "payload": {"phoneNumber": "+910000", "message": "test"},
            "callbackUrl": "http://example.com/cb"
        })
        event = sms_queue.get_nowait()
        sms_queue.task_done()
        assert "eventId" in event
        assert "eventType" in event
        assert "payload" in event
        assert "callbackUrl" in event
        assert event["callbackUrl"] == "http://example.com/cb"


# 3. Worker / Processing Tests

class TestWorkerProcessing:

    def _make_event(self, event_type="EMAIL", callback_url="http://example.com/cb"):
        return {
            "eventId": "test-id-123",
            "eventType": event_type,
            "payload": {"message": "test"},
            "callbackUrl": callback_url
        }

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.5)   # force success (>0.1)
    @patch("app.workers.time.sleep")                         # skip actual delay
    def test_successful_event_sends_completed_callback(self, mock_sleep, mock_random, mock_post):
        """On success the callback payload must have status COMPLETED."""
        q = Queue()
        shutdown = threading.Event()
        event = self._make_event()
        q.put(event)
        shutdown.set()  # stop after draining

        process_event(q, 0, shutdown)

        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert payload["status"] == "COMPLETED"
        assert payload["eventId"] == "test-id-123"
        assert "errorMessage" not in payload

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.05)  # force failure (<0.1)
    @patch("app.workers.time.sleep")
    def test_failed_event_sends_failed_callback(self, mock_sleep, mock_random, mock_post):
        """On simulated failure the callback payload must have status FAILED."""
        q = Queue()
        shutdown = threading.Event()
        q.put(self._make_event())
        shutdown.set()

        process_event(q, 0, shutdown)

        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert payload["status"] == "FAILED"
        assert "errorMessage" in payload

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.5)
    @patch("app.workers.time.sleep")
    def test_callback_contains_event_type(self, mock_sleep, mock_random, mock_post):
        """Callback payload must include the eventType field."""
        q = Queue()
        shutdown = threading.Event()
        q.put(self._make_event(event_type="EMAIL"))
        shutdown.set()

        process_event(q, 0, shutdown)

        payload = mock_post.call_args[1]["json"]
        assert "eventType" in payload

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.5)
    def test_processing_delay_is_respected(self, mock_random, mock_post):
        """Worker must sleep for the configured processing_time per event."""
        q = Queue()
        shutdown = threading.Event()
        q.put(self._make_event())
        shutdown.set()

        with patch("app.workers.time.sleep") as mock_sleep:
            process_event(q, 3, shutdown)
            mock_sleep.assert_called_once_with(3)

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.5)
    @patch("app.workers.time.sleep")
    def test_queue_task_done_called_after_processing(self, mock_sleep, mock_random, mock_post):
        """queue.task_done() must be called so queue.join() can unblock."""
        q = Queue()
        shutdown = threading.Event()
        q.put(self._make_event())
        shutdown.set()

        process_event(q, 0, shutdown)

        # If task_done was called correctly, join() should not block
        q.join()  # will hang and fail the test if task_done was never called

    @patch("app.workers.requests.post", side_effect=Exception("Network error"))
    @patch("app.workers.random.random", return_value=0.5)
    @patch("app.workers.time.sleep")
    def test_callback_failure_does_not_crash_worker(self, mock_sleep, mock_random, mock_post):
        """If the callback HTTP call fails, the worker must continue without crashing."""
        q = Queue()
        shutdown = threading.Event()
        q.put(self._make_event())
        q.put(self._make_event(event_type="SMS"))
        shutdown.set()

        # Should not raise
        process_event(q, 0, shutdown)

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.5)
    @patch("app.workers.time.sleep")
    def test_fifo_processing_order(self, mock_sleep, mock_random, mock_post):
        """Worker processes events in the order they were enqueued."""
        q = Queue()
        shutdown = threading.Event()
        ids = ["id-1", "id-2", "id-3"]
        for eid in ids:
            q.put({
                "eventId": eid,
                "eventType": "PUSH",
                "payload": {},
                "callbackUrl": "http://example.com/cb"
            })
        shutdown.set()

        process_event(q, 0, shutdown)

        processed_ids = [call[1]["json"]["eventId"] for call in mock_post.call_args_list]
        assert processed_ids == ids


# 4. Random Failure Simulation Tests

class TestRandomFailureSimulation:

    @patch("app.workers.requests.post")
    @patch("app.workers.time.sleep")
    def test_roughly_10_percent_of_events_fail(self, mock_sleep, mock_post):
        """Over 200 events, ~10% should be marked FAILED (allow 3–20% window)."""
        q = Queue()
        shutdown = threading.Event()

        for i in range(200):
            q.put({
                "eventId": f"id-{i}",
                "eventType": "PUSH",
                "payload": {},
                "callbackUrl": "http://example.com/cb"
            })
        shutdown.set()

        process_event(q, 0, shutdown)

        statuses = [call[1]["json"]["status"] for call in mock_post.call_args_list]
        failure_rate = statuses.count("FAILED") / len(statuses)
        assert 0.03 <= failure_rate <= 0.20, f"Unexpected failure rate: {failure_rate:.2%}"

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.05)   # always fail
    @patch("app.workers.time.sleep")
    def test_failed_event_includes_error_message(self, mock_sleep, mock_random, mock_post):
        """A failed callback must include a non-empty errorMessage."""
        q = Queue()
        shutdown = threading.Event()
        q.put({
            "eventId": "fail-id",
            "eventType": "EMAIL",
            "payload": {},
            "callbackUrl": "http://example.com/cb"
        })
        shutdown.set()

        process_event(q, 0, shutdown)

        payload = mock_post.call_args[1]["json"]
        assert payload["status"] == "FAILED"
        assert payload.get("errorMessage"), "errorMessage must be non-empty on failure"

    @patch("app.workers.requests.post")
    @patch("app.workers.random.random", return_value=0.5)   # always succeed
    @patch("app.workers.time.sleep")
    def test_successful_event_has_no_error_message(self, mock_sleep, mock_random, mock_post):
        """A successful callback must NOT include errorMessage."""
        q = Queue()
        shutdown = threading.Event()
        q.put({
            "eventId": "ok-id",
            "eventType": "EMAIL",
            "payload": {},
            "callbackUrl": "http://example.com/cb"
        })
        shutdown.set()

        process_event(q, 0, shutdown)

        payload = mock_post.call_args[1]["json"]
        assert payload["status"] == "COMPLETED"
        assert "errorMessage" not in payload


# 5. Graceful Shutdown Tests


class TestGracefulShutdown:

    @patch("app.workers.requests.post")
    @patch("app.workers.time.sleep")
    def test_worker_stops_when_shutdown_set_and_queue_empty(self, mock_sleep, mock_post):
        """Worker loop exits once shutdown is set and queue is empty."""
        q = Queue()
        shutdown = threading.Event()
        shutdown.set()  # signal shutdown immediately with empty queue

        t = threading.Thread(target=process_event, args=(q, 0, shutdown))
        t.start()
        t.join(timeout=3)
        assert not t.is_alive(), "Worker thread should have exited after shutdown"

    @patch("app.workers.requests.post")
    @patch("app.workers.time.sleep")
    def test_in_flight_events_complete_before_shutdown(self, mock_sleep, mock_post):
        """Events already in queue must finish processing even after shutdown is set."""
        q = Queue()
        shutdown = threading.Event()

        for i in range(5):
            q.put({
                "eventId": f"drain-{i}",
                "eventType": "SMS",
                "payload": {},
                "callbackUrl": "http://example.com/cb"
            })

        shutdown.set()  # shutdown before worker even starts

        process_event(q, 0, shutdown)

        # All 5 events must have been processed and callbacks sent
        assert mock_post.call_count == 5

    @patch("app.workers.requests.post")
    @patch("app.workers.time.sleep")
    def test_queue_empty_after_processing(self, mock_sleep, mock_post):
        """Queue must be empty after worker finishes draining it."""
        q = Queue()
        shutdown = threading.Event()

        for i in range(3):
            q.put({
                "eventId": f"e-{i}",
                "eventType": "PUSH",
                "payload": {},
                "callbackUrl": "http://example.com/cb"
            })

        shutdown.set()
        process_event(q, 0, shutdown)

        assert q.empty()

    @patch("app.workers.requests.post")
    @patch("app.workers.time.sleep")
    def test_thread_terminates_cleanly(self, mock_sleep, mock_post):
        """Worker thread must terminate cleanly after shutdown."""
        q = Queue()
        shutdown = threading.Event()

        q.put({
            "eventId": "t-1",
            "eventType": "PUSH",
            "payload": {},
            "callbackUrl": "http://example.com/cb"
        })

        t = threading.Thread(target=process_event, args=(q, 0, shutdown))
        t.start()

        time.sleep(0.2)   # give thread time to pick up the event
        shutdown.set()
        t.join(timeout=5)

        assert not t.is_alive(), "Worker thread did not terminate cleanly"