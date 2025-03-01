from fastapi import FastAPI
from pydantic import BaseModel
from openai import OpenAI
from celery import Celery
import requests, os, random, time, re, logging
from dotenv import load_dotenv  # ✅ Add this to load .env variables locally
from datetime import datetime # For precise logging of failed tasks

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ✅ Load environment variables from .env (only needed locally)
load_dotenv()

# ✅ Fetch Redis URL
REDIS_URL = os.getenv("REDIS_URL")

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

# ✅ GHL Webhook URLs (one for each day) and a Slack Webhook URL in the rare case of OpenAI failure
GHL_WEBHOOK_URL_DAY_1 = os.getenv("GHL_WEBHOOK_URL_DAY_1")
GHL_WEBHOOK_URL_DAY_2 = os.getenv("GHL_WEBHOOK_URL_DAY_2")
GHL_WEBHOOK_URL_DAY_3 = os.getenv("GHL_WEBHOOK_URL_DAY_3")
GHL_WEBHOOK_URL_DAY_4 = os.getenv("GHL_WEBHOOK_URL_DAY_4")
GHL_WEBHOOK_URL_DAY_5 = os.getenv("GHL_WEBHOOK_URL_DAY_5")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
GHL_WEBHOOK_FAILSAFE = os.getenv("GHL_WEBHOOK_FAILSAFE")

# ✅ FastAPI App
app = FastAPI(debug=True)

# ✅ Request Model
class AssignmentRequest(BaseModel):
    contact_id: str # For matching in the Inbound Webhook automation
    contact_email: str
    day: int
    field1: str
    field2: str

# Helper Function to remove GPT Assistant auto-generated text
def remove_bracketed_text(text):
    return re.sub(r'【.*?】', '', text)

# Helper function to poll OpenAI run with exponential backoff
def poll_openai_run(client, thread, run, max_attempts=30):
    attempts = 0
    wait_time = 10  # initial wait time in seconds

    while run.status not in ["completed", "failed", "cancelled"]:
        if attempts >= max_attempts:
            logging.error("❌ OpenAI took too long. Aborting after %s attempts.", max_attempts)
            raise Exception("OpenAI response took too long.")
        time.sleep(wait_time)
        wait_time = min(wait_time * 1.5, 60)  # Exponential backoff capped at 60s
        attempts += 1
        run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        logging.info("🔄 Attempt %s: OpenAI Status = %s", attempts, run.status)

        if run.status == "failed" and "server_error" in str(run.last_error):
            logging.error("❌ OpenAI server error encountered during polling. Not retrying.")
            return run
    return run

# Helper function to send a Slack alert with timeout and logging
def send_slack_alert(contact_email, day, field1, field2, timeout=10):
    slack_payload = {
        "text": f"🚨 *Amazon Challenge Feedback Alert!*\n"
                f"❌ OpenAI failed to generate feedback for *{contact_email}* (Day {day}).\n"
                f"Field 1: {field1}, Field 2: {field2}.\n"
                f"📌 Please review manually and send a manual feedback email!"
    }
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=slack_payload, timeout=timeout)
        if response.status_code == 200:
            logging.info("✅ Slack alert sent successfully!")
            return True
        else:
            logging.warning("⚠️ Slack alert failed: %s, %s", response.status_code, response.text)
    except Exception as e:
        logging.error("❌ Error sending Slack alert: %s", str(e))
    return False

# Helper function to send failsafe payload to GHL
def send_failsafe_payload(contact_email, day, field1, field2, error_message, timeout=10):
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "email": contact_email,
        "day": day,
        "field1": field1,
        "field2": field2,
        "error_message": error_message
    }
    try:
        response = requests.post(GHL_WEBHOOK_FAILSAFE, json=payload, timeout=timeout)
        logging.info("Failed task sent. Status: %s, Response: %s", response.status_code, response.text)
    except Exception as e:
        logging.error("❌ Error sending failed task to GHL: %s", str(e))
        raise

# Helper function to send feedback to GHL
def send_ghl_feedback(contact_id, contact_email, feedback, webhook_url, timeout=10):
    payload = {
        "contact_id": contact_id,
        "contact_email": contact_email,
        "feedback": feedback,
    }
    try:
        response = requests.post(webhook_url, json=payload, timeout=timeout)
        logging.info("✅ Feedback sent! Status: %s, Response: %s", response.status_code, response.text)
    except Exception as e:
        logging.error("❌ Error sending feedback to GHL: %s", str(e))
        raise

# Define Celery Task
@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3, "countdown": 60})
def process_assignment(self, contact_id: str, contact_email: str, day: int, field1: str, field2: str):
    """
    Celery Background Task:
    1️⃣ Waits 1-3 min before processing.
    2️⃣ Sends request to OpenAI Assistants API.
    3️⃣ Sends feedback back to GHL.
    """

    # Determine the user input and appropriate GHL webhook URL based on day
    if day == 1:
        user_input = f"Användaren lämnar in sin läxa för Dag 1. Användaren har valt marknadsplatsen >{field1}< och tagit fram snittförsäljningen >{field2}<."
        ghl_webhook_url = GHL_WEBHOOK_URL_DAY_1
    elif day == 2:
        user_input = f"Användaren lämnar in sin läxa för Dag 2. Användaren har prisbilden >{field1}< och har en marginal på >{field2}<."
        ghl_webhook_url = GHL_WEBHOOK_URL_DAY_2
    elif day == 3:
        user_input = f"Användaren lämnar in sin läxa för Dag 3. Användaren kommer att sticka ut i sin förstabild genom: >{field1}<. Användarens viktigaste USP är: >{field2}<."
        ghl_webhook_url = GHL_WEBHOOK_URL_DAY_3
    elif day == 4:
        user_input = f"Användaren lämnar in sin läxa för Dag 4. Användaren kommer att stimulera A9 på >{field1}<st sätt. Användarens viktigaste målgrupp är >{field2}<."
        ghl_webhook_url = GHL_WEBHOOK_URL_DAY_4
    elif day == 5:
        user_input = f"Användaren lämnar in sin läxa för Dag 5. Användaren kommer att generera reviews på >{field1}<st sätt. Användarens viktigaste taktik för att generera reviews är >{field2}<."
        ghl_webhook_url = GHL_WEBHOOK_URL_DAY_5
    else:
        # Although this case should not occur, we include a safeguard
        error_msg = f"Invalid day value received: {day}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    # Random delay of 1-3 minutes before processing
    minutes = random.randint(1, 3)
    delay_seconds = minutes * 60
    logging.info("🕒 Assignment received from %s. Will process in %d minutes...", contact_email, minutes)
    time.sleep(delay_seconds)

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Step 1: Start OpenAI Thread
    try:
        thread = client.beta.threads.create(messages=[{"role": "user", "content": user_input}])
        run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=ASSISTANT_ID)
    except Exception as e:
        logging.error("❌ Error creating OpenAI thread: %s", str(e))
        raise self.retry(exc=e)

    # Step 2: Poll OpenAI for Completion with exponential backoff
    run = poll_openai_run(client, thread, run)
    if run.status == "failed":
        logging.error("❌ OpenAI run failed: %s", run)
        # Send Slack alert; if it fails, log a warning
        if not send_slack_alert(contact_email, day, field1, field2):
            logging.warning("Slack alert could not be sent successfully.")
        # Log failed task to GHL failsafe
        send_failsafe_payload(contact_email, day, field1, field2, str(run.last_error))
        raise Exception("OpenAI task failed.")

    # Step 3: Retrieve OpenAI Response with data validity checks
    try:
        message_response = client.beta.threads.messages.list(thread_id=thread.id)
        if not message_response.data:
            error_msg = "No messages received from OpenAI."
            logging.error("❌ %s", error_msg)
            raise Exception(error_msg)
        latest_message = message_response.data[0]
        # Validate expected structure of the message
        if not latest_message.content or not latest_message.content[0].text:
            error_msg = "Invalid message format received from OpenAI."
            logging.error("❌ %s", error_msg)
            raise Exception(error_msg)
        assistant_output = latest_message.content[0].text.value
        feedback = remove_bracketed_text(assistant_output)
        formatted_feedback = feedback.replace("\n", "<br>")
    except Exception as e:
        logging.error("❌ Error retrieving OpenAI response: %s", str(e))
        raise self.retry(exc=e)

    # Step 4: Send Feedback to GHL
    try:
        send_ghl_feedback(contact_id, contact_email, formatted_feedback, ghl_webhook_url)
    except Exception as e:
        logging.error("❌ Error in sending feedback to GHL: %s", str(e))
        raise self.retry(exc=e)

# FastAPI endpoint to receive assignments
@app.post("/receive-assignment/")
def receive_assignment(data: AssignmentRequest):
    logging.info("✅ Received assignment from %s", data.contact_email)
    logging.info("Day: %s", data.day)
    logging.info("Field 1: %s", data.field1)
    logging.info("Field 2: %s", data.field2)

    process_assignment.delay(data.contact_id, data.contact_email, data.day, data.field1, data.field2)
    return {"message": "Assignment received! Processing in Celery queue."}
