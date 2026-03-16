from queue import Queue
MAX_QUEUE_SIZE = 1000
email_queue=Queue(maxsize=MAX_QUEUE_SIZE)
sms_queue=Queue(maxsize=MAX_QUEUE_SIZE)
push_queue=Queue(maxsize=MAX_QUEUE_SIZE)
