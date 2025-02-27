from fastapi import FastAPI
from pydantic import BaseModel
from openai import OpenAI
from celery import Celery
import requests, os, random, time, re
from dotenv import load_dotenv  # ✅ Add this to load .env variables locally

# ✅ Fetch Redis URL
REDIS_URL = os.getenv("REDIS_URL")
# ✅ Load environment variables from .env (only needed locally)
load_dotenv()

# ✅ Ensure REDIS_URL is always available
REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    raise ValueError("❌ REDIS_URL is not set! Make sure it's in your environment variables.")

# ✅ Define SSL Options for Celery
CELERY_SSL_OPTIONS = {
    "ssl_cert_reqs": "CERT_OPTIONAL"  # Fixes SSL Error
}

# ✅ Celery configuration with correct SSL handling
celery_app = Celery(
    "tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
    broker_use_ssl=CELERY_SSL_OPTIONS,
    backend_use_ssl=CELERY_SSL_OPTIONS
)

# ✅ Fix Celery 6.0 deprecation warning
celery_app.conf.broker_connection_retry_on_startup = True

# ✅ OpenAI API Key & Assistant ID
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID = os.getenv("ASSISTANT_ID")

# ✅ GHL Webhook URLs (one for each day)
GHL_WEBHOOK_URL_DAY_1 = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/beafa555-9b19-45f9-823b-0a4ee4a6eeb5"
GHL_WEBHOOK_URL_DAY_2 = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/56ec6125-9870-4931-975b-41c4e0014c44"
GHL_WEBHOOK_URL_DAY_3 = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/8ace25bb-5a0b-4cfc-9641-2e97678b0eb8"
GHL_WEBHOOK_URL_DAY_4 = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/9a9cc420-ae3c-4369-bb9e-797a6f225273"
GHL_WEBHOOK_URL_DAY_5 = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/2b6c1401-ef23-4fe8-a507-f7b9965eecf7"

# ✅ FastAPI App
app = FastAPI(debug=True)

# ✅ Request Model
class AssignmentRequest(BaseModel):
    contact_id: str # For matching in the Inbound Webhook automation
    contact_email: str
    day: int
    field1: str
    field2: str

# ✅ Helper Function to remove GPT Assistant auto-generated text
def remove_bracketed_text(text):
    return re.sub(r'【.*?】', '', text)

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3, "countdown": 60})
def process_assignment(self, contact_id: str, contact_email: str, day: int, field1: str, field2: str):
    """
    Celery Background Task:
    1️⃣ Waits 7-15 min before processing.
    2️⃣ Sends request to OpenAI Assistants API.
    3️⃣ Sends feedback back to GHL.
    """

    match day:
        case 1: 
            user_input = f"Användaren lämnar in sin läxa för Dag 1. Användaren har valt marknadsplatsen >{field1}< och tagit fram snittförsäljningen >{field2}<."
            GHL_WEBHOOK_URL = GHL_WEBHOOK_URL_DAY_1

        case 2: 
            user_input = f"Användaren lämnar in sin läxa för Dag 2. Användaren har prisbilden >{field1}< och har en marginal på >{field2}<."
            GHL_WEBHOOK_URL = GHL_WEBHOOK_URL_DAY_2

        case 3: 
            user_input = f"Användaren lämnar in sin läxa för Dag 3. Användaren kommer att sticka ut i sin förstabild genom: >{field1}<. Användarens viktigaste USP är: >{field2}<."
            GHL_WEBHOOK_URL = GHL_WEBHOOK_URL_DAY_3
        
        case 4: 
            user_input = f"Användaren lämnar in sin läxa för Dag 4. Användaren kommer att stimulera A9 på >{field1}<st sätt. Användarens viktigaste målgrupp är >{field2}<."
            GHL_WEBHOOK_URL = GHL_WEBHOOK_URL_DAY_4

        case 5: 
            user_input = f"Användaren lämnar in sin läxa för Dag 5. Användaren kommer att generera reviews på >{field1}<st sätt. Användarens viktigaste taktik för att generera reviews är >{field2}<."
            GHL_WEBHOOK_URL = GHL_WEBHOOK_URL_DAY_5

    minutes = random.randint(7, 15)
    wait_time = minutes * 60  # Convert to minutes

    print(f"🕒 Assignment received from {contact_email}. Will process in {minutes} minutes...")

    time.sleep(wait_time)  # Wait 7-15 min before processing

    try:
        # Send to OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        thread = client.beta.threads.create(messages=[{"role": "user", "content": user_input}])
        run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=ASSISTANT_ID)

        while run.status != "completed":
            time.sleep(10)  # Prevent excessive API calls
            run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)

        # Retrieve OpenAI Response
        message_response = client.beta.threads.messages.list(thread_id=thread.id)
        latest_message = message_response.data[0]
        assistant_output = latest_message.content[0].text.value
        feedback = remove_bracketed_text(assistant_output)
        formatted_feedback = feedback.replace("\n", "<br>")

        # Send Feedback to GHL
        response = requests.post(
            GHL_WEBHOOK_URL,
            json={"contact_id": contact_id, "contact_email": contact_email, "feedback": formatted_feedback}
        )

        print(f"✅ Feedback sent for {contact_email}: {formatted_feedback} (Status: {response.status_code})")

    except Exception as e:
        print(f"❌ Error processing assignment for {contact_email}: {str(e)}")
        raise self.retry(exc=e)  # ✅ Automatically retry if the task fails

@app.post("/receive-assignment/")
def receive_assignment(data: AssignmentRequest):
    """
    1️⃣ Receives assignment from GHL.
    2️⃣ Immediately returns "OK" to avoid timeouts.
    3️⃣ Sends task to Celery queue.
    """
    print(f"✅ Received assignment from {data.contact_email}")
    print(f"Day: {data.day}")
    print(f"Field 1: {data.field1}")
    print(f"Field 2: {data.field2}")

    process_assignment.delay(data.contact_id, data.contact_email, data.day, data.field1, data.field2)  # ✅ Celery Task

    return {"message": "Assignment received! Processing in Celery queue."}