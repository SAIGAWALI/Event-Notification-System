import time
import random
import requests
from datetime import datetime

def process_event(queue,processing_time,shutdown_event):
    
    while not shutdown_event.is_set() or not queue.empty():
        try:
            event=queue.get(timeout=1)
        except:
            continue
        print("processing event",event["eventId"])
        try:
            #Processing time(execution get delay)
            time.sleep(processing_time)
            
            # simulate random failure (10%)
            if random.random() < 0.1:
                raise Exception("Simulated failure")
            
            status="COMPLETED"
            error_message=None
        except Exception as e:
            status="FAILED"
            error_message=str(e)
            print("Simulated failure",flush=True)
        callback_payload ={
            "eventId":event["eventId"],
            "status":status,
            "eventType":str(event["eventType"]),
            "processedAt": datetime.utcnow().isoformat()
        }
        
        if error_message:
            callback_payload["errorMessage"] = error_message
        
        try:
            requests.post(event["callbackUrl"],json=callback_payload)
        except Exception as e:
            print("Callback failed",flush=True)

        queue.task_done()
        if status=="COMPLETED":
            print(f"task_done for {str(event['eventType'])}",flush=True)
        print("queue size:", queue.qsize(),flush=True)
