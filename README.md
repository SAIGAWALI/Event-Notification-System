# Event Notification System

A containerized **FastAPI-based event processing system** that accepts events via an API, queues them based on event type, processes them asynchronously, and sends results to a callback URL (webhook).

The system simulates a real backend notification service where events such as **EMAIL**, **SMS**, and **PUSH notifications** are processed independently with different delays and a simulated failure rate.

---

# System Architecture

The system follows a **queue-based worker architecture**.

```
Client Request
      │
      ▼
POST /api/events
      │
      ▼
Event routed to queue based on type
      │
      ▼
Worker thread processes event
      │
      ▼
Callback sent to webhook URL
```

Three independent queues are used:

| Event Type | Queue | Processing Delay |
|-------------|------|------------------|
| EMAIL | email_queue | 5 seconds |
| SMS | sms_queue | 3 seconds |
| PUSH | push_queue | 2 seconds |

Each queue has a **dedicated worker thread** that processes events in **FIFO order**.

---

# Event Processing Flow

1. Client sends a request to **POST /api/events**
2. The API validates the request.
3. A unique **eventId** is generated.
4. The event is placed into the appropriate queue.
5. A worker thread processes the event with a simulated delay.
6. There is a **10% chance of simulated failure**.
7. The worker sends a **POST callback to the provided webhook URL**.
8. The callback payload includes the event result.

Example callback payload:

```json
{
  "eventId": "12345",
  "eventType": "EMAIL",
  "status": "COMPLETED",
  "processedAt": "2026-03-15T12:00:00Z"
}
```

If the event fails:

```json
{
  "eventId": "12345",
  "eventType": "EMAIL",
  "status": "FAILED",
  "processedAt": "2026-03-15T12:00:00Z",
  "errorMessage": "Simulated failure"
}
```

---

# API Documentation

Once the server is running, FastAPI automatically provides interactive API documentation.

Open in browser:

```
http://localhost:8080/docs
```

This page allows you to:

- View API schema
- Send test requests
- Inspect request/response formats

---

# Using a Webhook for Callbacks

To observe event callbacks, you can use a webhook testing service such as:

```
https://webhook.site
```

Steps:

1. Open webhook.site
2. Copy the generated URL
3. Use it as the **callbackUrl** in your request
4. You will see incoming callback events in real time

---

# Example Event Requests

### EMAIL Event

```
POST /api/events
```

```json
{
  "eventType": "EMAIL",
  "payload": {
    "recipient": "user@example.com",
    "message": "Welcome to our service"
  },
  "callbackUrl": "https://webhook.site/your-url"
}
```

Processing delay: **5 seconds**

---

### SMS Event

```json
{
  "eventType": "SMS",
  "payload": {
    "phoneNumber": "+911234567890",
    "message": "Your OTP is 123456"
  },
  "callbackUrl": "https://webhook.site/your-url"
}
```

Processing delay: **3 seconds**

---

### PUSH Notification Event

```json
{
  "eventType": "PUSH",
  "payload": {
    "deviceId": "device-123",
    "message": "Your order has shipped"
  },
  "callbackUrl": "https://webhook.site/your-url"
}
```

Processing delay: **2 seconds**

---

# Graceful Shutdown

The system supports **graceful shutdown**.

When the server receives a shutdown signal:

1. The API stops accepting new events.
2. Workers continue processing remaining queued events.
3. The system waits until all queues are empty.
4. Workers terminate cleanly.

This ensures **no events are lost during shutdown**.

---

# Running the System

## Option 1: Run with Docker (Recommended)

Build and start the container:

```
docker compose up --build
```

The API will be available at:

```
http://localhost:8080
```

Stop the system:

```
CTRL + C
```

Docker allows up to **900 seconds** for graceful shutdown to complete.

---

## Option 2: Run Locally (Without Docker)

Install dependencies:

```
pip install -r requirements.txt
```

Start the server:

```
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

---

# Running Tests

The project includes **30 automated tests** using **pytest**.

Run tests:

```
pytest
```

Tests cover:

- API validation
- Event routing
- Queue behavior
- Worker processing
- Random failure simulation
- Callback payload correctness
- Graceful shutdown behavior

Example output:

```
=====================
30 passed in 1.20s
=====================
```

---

# Project Structure

```
event_notification_system/

app/
├── __init__.py        # Marks the directory as a Python package
├── main.py            # FastAPI application, API routes, startup and shutdown logic
├── models.py          # Pydantic request models and EventType enum
├── queues.py          # Definitions of the three event queues (EMAIL, SMS, PUSH)
└── workers.py         # Worker thread logic that processes queued events and sends callbacks

tests/
├── __init__.py        # Makes the tests directory a Python package
└── test_events.py     # Pytest test suite (API, queues, workers, failure simulation, shutdown)

conftest.py            # Pytest configuration file for shared fixtures and test setup

Dockerfile             # Builds the Docker image for the application
docker-compose.yml     # Runs the service container and configures graceful shutdown
requirements.txt       # Python dependencies required to run the project

README.md              # Project documentation and usage instructions
```
---

# Key Features

- FastAPI REST API
- Queue-based event processing
- Multi-threaded workers
- FIFO queue processing
- Random failure simulation
- Webhook callback notifications
- Graceful shutdown support
- Docker containerization
- Automated testing with pytest

---

# Summary

This system demonstrates a simplified **asynchronous event processing architecture** commonly used in backend services for notifications, messaging, and background task processing.

It highlights:

- concurrent processing using worker threads
- queue-based workload management
- webhook-based callback communication
- graceful shutdown of background workers
- containerized deployment
- automated testing