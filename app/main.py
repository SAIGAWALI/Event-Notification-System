from fastapi import FastAPI
from app.models import EventRequest
from app.queues import email_queue, sms_queue, push_queue
import uuid
import threading
from app.workers import process_event
from fastapi import HTTPException
app = FastAPI()

shutdown_event = threading.Event() 


@app.get("/")
def home():
    return {"message": "Event Notification System Running"}

@app.post("/api/events")
def create_event(event:EventRequest):
    if shutdown_event.is_set():
        raise HTTPException(status_code=503, detail="Server shutting down")
    event_id=str(uuid.uuid4())
    event_data={
        "eventId":event_id,
        "eventType":event.eventType.value,
        "payload":event.payload,
        "callbackUrl":event.callbackUrl
    }
    
    if event.eventType.value=="EMAIL":
        email_queue.put(event_data)
        print("email_queue_size",email_queue.qsize())
    elif event.eventType.value=="SMS":
        sms_queue.put(event_data)
        print("sms_queue_size",sms_queue.qsize())
    elif event.eventType.value=="PUSH":
        push_queue.put(event_data)
        print("push_queue_size",push_queue.qsize())
    return{
        "eventId":event_id,"message":"Event accepted for processing"
    }
    
@app.on_event("startup")
def start_workers():
    threading.Thread(target=process_event, args=(email_queue, 5, shutdown_event), daemon=True).start()
    threading.Thread(target=process_event, args=(sms_queue, 3, shutdown_event), daemon=True).start()
    threading.Thread(target=process_event, args=(push_queue, 2, shutdown_event), daemon=True).start()

@app.on_event("shutdown")
def shutdown_workers():
    print("Shutdown signal received",flush=True)
    shutdown_event.set()
    email_queue.join()
    sms_queue.join()
    push_queue.join()
    print("All events processed. Workers stopping.",flush=True)

    
