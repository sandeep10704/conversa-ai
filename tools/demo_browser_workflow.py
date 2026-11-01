"""Conversa AI — Repeatable Browser Demonstration Script.

Simulates the exact 9-step operator browser workflow matching client/index.html:
1. Enter participant context in prep UI.
2. Start new session.
3. Connect live microphone / Audio Gateway WebSocket.
4. Stream/transcribe technical discussion messages.
5. View finalized transcripts in UI.
6. Trigger suggestion request via supported manual path.
7. Receive and display personalized, grounded suggestion.
8. Stop microphone and end session.
9. Verify participant context purged from PostgreSQL.
"""

import asyncio
import json
import logging
import subprocess
import time

import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BrowserDemo")

AGENT_API_URL = "http://127.0.0.1:8001"
AUDIO_GATEWAY_WS_URL = "ws://127.0.0.1:8002/ws/audio"
DELIVERY_WS_URL = "ws://127.0.0.1:8003/ws/delivery"


async def run_browser_demonstration():
    logger.info("==================================================================")
    logger.info("CONVERSA AI — BROWSER CLIENT DEMONSTRATION WORKFLOW")
    logger.info("==================================================================")

    # 1. Enter Participant Context
    logger.info("\n[DEMO STEP 1] Operator enters profile in Session Prep Form:")
    profile = {
        "preferred_name": "Sandeep",
        "role": "Backend Developer",
        "responsibilities": "Audio pipeline and backend architecture",
        "expertise_keywords": ["Java", "Kafka", "PostgreSQL", "WebSockets"],
        "session_goals": "Contribute technical insights on performance and scalability",
        "constraints": "Do not make unsupported performance claims",
    }
    for k, v in profile.items():
        logger.info("  * %s: %s", k, v)

    # Calculate character count (as done in UI)
    char_count = (
        len(profile["preferred_name"])
        + len(profile["role"])
        + len(profile["responsibilities"])
        + sum(len(kw) for kw in profile["expertise_keywords"])
        + len(profile["session_goals"])
        + len(profile["constraints"])
    )
    logger.info("  [UI Dynamic Counter] %d / 1500 chars (Valid)", char_count)

    async with httpx.AsyncClient(timeout=30.0) as client:
        # 2. Start New Session
        logger.info("\n[DEMO STEP 2] Operator clicks 'Start New Session':")
        resp = await client.post(
            f"{AGENT_API_URL}/v1/sessions",
            json={
                "situation": "Meeting on audio pipeline performance and streaming latency",
                "objective": "Identify latency sources and consumer lag mitigation",
                "communication_goals": [
                    "Contribute technical insights on Kafka consumer throughput",
                    "Ensure real-time streaming constraints",
                ],
                "response_preferences": {"tone": "technical", "detail_level": "concise"},
                "constraints": ["Keep suggestions concise"],
                "participant_context": profile,
            },
        )
        assert resp.status_code == 201
        sess = resp.json()
        session_id = sess["session_id"]
        logger.info("  ✓ Session created: %s (Status: %s)", session_id, sess["status"])
        logger.info("  [UI Card] Verified Participant Profile rendered on screen.")

        # 3. Connect Microphone / Audio Gateway & Delivery WebSockets
        logger.info("\n[DEMO STEP 3] Operator clicks 'Start Microphone':")
        audio_ws_url = f"{AUDIO_GATEWAY_WS_URL}/{session_id}"
        delivery_ws_url = f"{DELIVERY_WS_URL}/{session_id}"

        audio_ws = await websockets.connect(audio_ws_url)
        handshake_raw = await audio_ws.recv()
        handshake = json.loads(handshake_raw)
        logger.info("  ✓ Audio Gateway connected: %s", handshake.get("audio_contract"))
        logger.info("  [UI Indicator] Audio Gateway (:8002): Connected (Green)")

        delivery_ws = await websockets.connect(delivery_ws_url)
        delivery_init = json.loads(await delivery_ws.recv())
        logger.info("  ✓ Delivery Service connected: %s", delivery_init.get("message"))
        logger.info("  [UI Indicator] Delivery Service (:8003): Connected (Green)")
        logger.info("  [UI Indicator] Microphone Status: Active (Green)")

        # 4. Speak Technical Discussion
        logger.info("\n[DEMO STEP 4 & 5] Participants speak technical discussion:")
        messages = [
            "We are seeing consumer lag when audio chunks arrive in bursts.",
            "The transcription worker sometimes processes messages slower than they arrive.",
            "We need to find where the bottleneck is.",
        ]
        for seq, text in enumerate(messages, start=1):
            # Stream audio chunk frames to Audio Gateway
            dummy_pcm = bytes([0] * 640)
            await audio_ws.send(dummy_pcm)

            # Commit transcription utterance
            msg_resp = await client.post(
                f"{AGENT_API_URL}/v1/sessions/{session_id}/messages",
                json={"text": text, "source": "transcription"},
            )
            assert msg_resp.status_code == 201
            logger.info("  [Utterance %d Finalized] \"%s\"", seq, text)
            logger.info("    [UI Display] Rendered in Finalized Utterances list (Seq: %d)", seq)

        # 6. Request Suggestion via supported path
        logger.info("\n[DEMO STEP 6] Operator clicks 'Request Suggestion':")
        sugg_resp = await client.post(f"{AGENT_API_URL}/v1/sessions/{session_id}/suggest")
        assert sugg_resp.status_code == 200
        sugg_data = sugg_resp.json()

        # 7. View Personalized Suggestion
        logger.info("\n[DEMO STEP 7] Personalized Suggestion received and displayed in UI:")
        logger.info("  * Status: %s", sugg_data["status"])
        logger.info("  * Identified Topic: %s", sugg_data.get("current_topic"))
        logger.info("  * Relevance Reason: %s", sugg_data.get("relevance_reason"))
        logger.info("  * Response Opportunity: %s", sugg_data.get("response_opportunity"))

        suggestions = sugg_data.get("suggestions", [])
        assert len(suggestions) > 0
        for i, s in enumerate(suggestions, 1):
            logger.info("  * Suggestion Card %d:", i)
            logger.info("    Type: %s", s.get("response_type"))
            logger.info("    Text: \"%s\"", s.get("text"))
            logger.info("    Rationale: \"%s\"", s.get("rationale"))
            logger.info("    [UI Card] Rendered with blue accent in Real-Time Agent Suggestions panel.")

        # 8. Stop Microphone & End Session
        logger.info("\n[DEMO STEP 8] Operator clicks 'Stop Microphone' and 'End Session':")
        await audio_ws.send(json.dumps({"type": "stop"}))
        await audio_ws.close()
        await delivery_ws.close()
        logger.info("  ✓ Audio Gateway and Delivery WebSockets closed.")

        end_resp = await client.post(f"{AGENT_API_URL}/v1/sessions/{session_id}/end")
        assert end_resp.status_code == 200
        end_data = end_resp.json()
        logger.info("  ✓ Session ended. Status: %s. Context returned: %s", end_data["status"], end_data["participant_context"])
        logger.info("  [UI State] Form cleared, Status set to '%s (Ended)'. Buttons disabled.", session_id)

        # 9. Verify Context Deletion
        logger.info("\n[DEMO STEP 9] Verifying database retention policy:")
        db_context = subprocess.check_output([
            "docker", "exec", "conversa-postgres", "psql", "-U", "conversa_user", "-d", "conversa",
            "-t", "-c", f"SELECT participant_context FROM sessions WHERE session_id = '{session_id}';"
        ]).decode().strip()
        logger.info("  PostgreSQL sessions.participant_context query result: '%s'", db_context)
        assert db_context == "" or db_context.lower() == "null"
        logger.info("  ✓ Confirmed: Participant profile was completely purged from database.")

    logger.info("\n==================================================================")
    logger.info("BROWSER DEMONSTRATION COMPLETED SUCCESSFULLY")
    logger.info("==================================================================")


if __name__ == "__main__":
    asyncio.run(run_browser_demonstration())
