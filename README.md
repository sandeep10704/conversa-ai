# Conversa AI

**Conversa AI** is an agentic conversational intelligence platform designed to listen to ongoing technical discussions, understand dialogue context in real time, evaluate opportune contribution moments, and generate context-aware, grounded response suggestions tailored to a participant's specific role and expertise.

The platform is engineered with **FastAPI**, **Silero VAD**, **faster-whisper**, **PostgreSQL 16 (SQLAlchemy 2.x + asyncpg)**, **Apache Kafka 3.8.0 (KRaft mode)** with the **Transactional Outbox Pattern**, **LangGraph**, and **Docker Compose**.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [End-to-End Processing Flow](#end-to-end-processing-flow)
- [Kafka Event-Driven Architecture](#kafka-event-driven-architecture)
- [LangGraph Reasoning Workflow](#langgraph-reasoning-workflow)
- [Session-Specific Participant Context](#session-specific-participant-context)
- [Turn-Completion Intelligence (Gate 1)](#turn-completion-intelligence-gate-1)
- [Privacy, Security & Data Boundaries](#privacy-security--data-boundaries)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Prerequisites & Docker Setup](#prerequisites--docker-setup)
- [Environment Configuration](#environment-configuration)
- [Starting the Application](#starting-the-application)
- [API & WebSocket Reference](#api--websocket-reference)
- [Automated Testing & Benchmarks](#automated-testing--benchmarks)
- [Demo Video](#demo-video)
- [Application Screenshots](#application-screenshots)
- [Current Limitations](#current-limitations)
- [Future Improvements & Roadmap](#future-improvements--roadmap)


---

## Problem Statement

Technical discussions—such as architecture reviews, sprint planning, and incident post-mortems—move rapidly. Participants often encounter several operational friction points:
1. **Cognitive Overload**: Balancing active listening, tracking conversational threads, and formulating high-value technical input simultaneously.
2. **Timing & Interruption Risks**: Speaking before a colleague finishes a thought risks awkward interruptions, while waiting too long means the discussion topic transitions before input can be shared.
3. **Context Grounding**: Generating suggestions that are strictly anchored to what was actually said, avoiding hallucinations or generic platitudes.
4. **Role Alignment**: Ensuring recommendations directly leverage the participant's specific technical competencies (e.g., backend architecture, streaming pipelines) rather than drifting into unrelated domains.

Conversa AI solves these challenges by combining low-latency local speech processing, guaranteed event delivery via Kafka and the Transactional Outbox pattern, syntactic turn-completion detection, and compact context-aware LangGraph orchestration.

---

## Key Features

- **Local Speech Ingestion & ASR**: Processes 16 kHz raw PCM audio over WebSocket directly within local containers. In isolated CPU benchmarks, Silero VAD executes in ~0.075 ms per 32 ms frame and faster-whisper `tiny.en` executes in ~111 ms for a 2.5s audio chunk using INT8 quantization, without sending raw audio to cloud APIs.
- **Transactional Outbox Pattern**: Employs atomic database transactions combining message transcripts and outbox events in a single PostgreSQL 16 commit. An asynchronous publisher uses `SELECT FOR UPDATE SKIP LOCKED` with Kafka `acks=all` for at-least-once broker delivery, eliminating dual-write inconsistencies.
- **Consumer Deduplication & Idempotency**: The consumer worker tracks processed `event_id` records in PostgreSQL (`processed_events`), filtering duplicate deliveries to achieve effective at-most-once execution of business side effects.
- **Conversational Turn-Completion Intelligence (Gate 1)**: Analyzes dialogue syntax, sentence-final punctuation, and hesitation markers to dynamically modulate debounce timers (1.5s for questions, 2.5s for complete thoughts, 4.5s for trailing thoughts). Cancels timers immediately on VAD speech resumption via a dedicated `conversation.speech.activity` Kafka topic.
- **Session-Specific Participant Context (Gate 2)**: Accepts user profile information (preferred name, role, responsibilities, expertise keywords, goals, constraints) at session creation. Enforces a 1,500-character cumulative limit and strictly purges context to SQL `NULL` upon session termination.
- **Compact Context Selector**: Intelligently prunes participant profiles before LLM inference, reducing context-added token overhead by **24.2%** while enforcing anti-fabrication constraints so the assistant never invents unverified personal experiences.
- **Multi-Step LangGraph Reasoning**: Six-node deterministic state graph: Understanding $\to$ Intent Detection $\to$ Relevance Evaluation $\to$ Planner $\to$ Generator $\to$ Deterministic Validator.
- **Deterministic Suggestion Validator**: Enforces strict grounding against recent transcript tokens, suppresses repetitive phrasing, and validates token limits before presenting suggestions.
- **Interactive Browser Workspace**: Clean, single-page interface (`client/index.html`) featuring session configuration, real-time microphone capture with audio level metering, live transcript streaming, manual and automatic suggestion requests, and session lifecycle controls.

---

## System Architecture

```mermaid
flowchart TD
    subgraph Client ["Client Layer"]
        UI["Browser Client\n(client/index.html)"]
    end

    subgraph AudioPipeline ["Speech Ingestion & ASR"]
        AG["Audio Gateway (:8002)\nWebSocket /ws/audio/{id}\n16kHz PCM Streaming"]
        TS["Transcription Service (:8004)\n• Silero VAD (0.075ms CPU)\n• faster-whisper (tiny.en int8)\n• SpeechSegmentManager"]
    end

    subgraph CoreBackend ["Core Persistence & Agent API"]
        API["Agent API (:8001)\nFastAPI + SQLAlchemy 2.x\nPOST /v1/sessions/{id}/messages"]
        PG[("PostgreSQL 16\n• sessions (JSONB context)\n• conversation_messages\n• outbox_events\n• processed_events\n• suggestions")]
        PUB["Outbox Publisher\nSELECT FOR UPDATE SKIP LOCKED\nacks='all' -> Kafka"]
    end

    subgraph EventStream ["Apache Kafka 3.8.0 (KRaft)"]
        T_FINAL["conversation.transcript.final"]
        T_SPEECH["conversation.speech.activity"]
        T_CMD["agent.commands"]
        T_SUGG["agent.suggestions"]
        T_DLQ["agent.dlq"]
    end

    subgraph Orchestration ["Agent Orchestration"]
        TC["Trigger Controller\n• Turn Debounce & Timer\n• Cancels on speech resumption\n• Dispatches agent commands"]
        AW["Agent Worker\nConsumes agent.commands\nLoads session + context"]
    end

    subgraph Reasoning ["LangGraph Suggestion Engine"]
        N_UND["1. Understanding Node\n(Topic & entity extraction)"]
        N_INT["2. Intent Node\n(Dialogue act classification)"]
        N_REL{"3. Relevance Node\n(Aligned with user?)"}
        N_SEL["4. Compact Context Selector\n(-24.2% token overhead)"]
        N_PLN["5. Planner Node\n(Contribution strategy)"]
        N_GEN["6. Generator Node\n(Anti-fabrication prompt)"]
        N_VAL["7. Validator Node\n(Grounding & length check)"]
    end

    subgraph RealtimeDelivery ["Real-Time Delivery"]
        DS["Delivery Service (:8003)\nWebSocket /ws/delivery/{id}\nPartials, Finals, Suggestions"]
    end

    %% Audio & Transcription Connections
    UI -- "Raw PCM Audio (WS)" --> AG
    AG -- "Peer WebSocket" --> TS
    TS -- "Partial Transcripts (HTTP)" --> DS
    TS -- "POST Final Utterance" --> API
    TS -. "Speech-Onset Events" .-> T_SPEECH

    %% Persistence & Outbox
    API -- "Single Atomic Commit" --> PG
    PUB -- "Polls pending events" --> PG
    PUB -- "Keyed by session_id" --> T_FINAL
    PUB -- "Keyed by session_id" --> T_SUGG

    %% Orchestration Flows
    T_FINAL --> TC
    T_SPEECH -. "Immediate Timer Cancellation" .-> TC
    TC -- "Dispatch Command" --> T_CMD
    T_CMD --> AW

    %% Reasoning Execution
    AW --> N_UND
    N_UND --> N_INT
    N_INT --> N_REL
    N_REL -- "Relevant" --> N_SEL
    N_REL -- "Not Relevant" --> AW
    N_SEL --> N_PLN
    N_PLN --> N_GEN
    N_GEN --> N_VAL
    N_VAL -- "Persist Suggestion" --> PG

    %% Client Delivery
    T_SUGG --> DS
    DS -- "Live Updates (WS)" --> UI

    %% Feature Flags Notation
    classDef defaultFlag fill:#f9f9f9,stroke:#666,stroke-width:1px;
    classDef activeService fill:#e1f5fe,stroke:#0288d1,stroke-width:2px;
    classDef storage fill:#fff3e0,stroke:#f57c00,stroke-width:2px;
    classDef kafkaTopic fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1px;

    class UI,AG,TS,API,PUB,TC,AW,DS activeService;
    class PG storage;
    class T_FINAL,T_SPEECH,T_CMD,T_SUGG,T_DLQ kafkaTopic;
```

> **Feature Flag Notice**: In production development, `AGENT_AUTO_RUN` and `AGENT_TURN_COMPLETION_ENABLED` are set to `false` by default. Suggestions can be requested manually via `POST /v1/sessions/{id}/suggest` or the browser UI button. `MOCK_LLM=true` protects developers against unexpected inference costs until live OpenRouter keys are configured.

---

## End-to-End Processing Flow

```
1. Client Configuration  ──► 2. Audio Capture  ──► 3. VAD & ASR  ──► 4. Atomic Commit
   (Participant Profile)      (16kHz PCM Stream)     (Silero + Whisper)  (Messages + Outbox)
                                                                               │
                                                                               ▼
8. Browser Display      ◄── 7. Delivery WS    ◄── 6. LangGraph   ◄── 5. Kafka Dispatch
   (Live Suggestions)         (Delivery Service)     (Reasoning Graph)   (Trigger Controller)
```

1. **Session Preparation**: The operator enters their name, role, responsibilities, technical expertise, and session goals in the browser client. The client calls `POST /v1/sessions` to initialize the session in PostgreSQL.
2. **Audio Streaming**: The user clicks "Start Audio Streaming". The browser records audio via Web Audio API, resamples to 16,000 Hz 16-bit mono PCM, and streams binary frames across WebSocket to the Audio Gateway (:8002).
3. **VAD & Speech Recognition**:
   - Audio Gateway forwards chunks to the Transcription Service (:8004).
   - Silero VAD evaluates each 32 ms frame (probability threshold 0.5).
   - When speech is detected, interim chunks are transcribed for partial hypothesis delivery to the user interface.
   - When silence exceeds 500 ms, `SpeechSegmentManager` finalizes the utterance and runs faster-whisper `tiny.en`.
4. **Atomic Transcript Persistence**:
   - The finalized utterance is posted to the Agent API (`POST /v1/sessions/{id}/messages`).
   - The Agent API opens an atomic database transaction: inserts the message into `conversation_messages`, assigns a monotonic `sequence_number`, and creates an event in `outbox_events`. Both write operations commit together or roll back entirely.
5. **Outbox Publication & Triggering**:
   - The Outbox Publisher polls `outbox_events` using `SELECT FOR UPDATE SKIP LOCKED` and publishes to `conversation.transcript.final` keyed by `session_id`.
   - The Trigger Controller consumes the event. If turn-completion intelligence is active, it analyzes syntactic completion and starts a dynamic debounce timer. If the speaker resumes talking, the VAD issues a speech-onset transition on `conversation.speech.activity`, instantly cancelling the pending timer.
6. **LangGraph Suggestion Execution**:
   - When the turn finishes, a command is dispatched to `agent.commands`.
   - The Agent Worker consumes the command, loads the session's participant context, selects discussion-relevant keywords, and executes the LangGraph state machine.
   - If relevant, the generator produces tailored response suggestions adhering to the strict anti-fabrication prompt.
7. **Deterministic Validation & Outbox Enqueue**:
   - The suggestion passes through the local rule-based validator (token grounding check, length check, repetition filter).
   - The valid suggestion is committed to `suggestions` and `outbox_events`.
8. **Real-Time Client Delivery**:
   - The Outbox Publisher pushes the suggestion to `agent.suggestions`.
   - The Delivery Service consumes the event and broadcasts it over WebSocket (:8003) to the browser UI.

---

## Kafka Event-Driven Architecture

Conversa AI utilizes **Apache Kafka 3.8.0** running in **KRaft mode** (no ZooKeeper dependency).

### Topic Topology

| Topic Name | Key | Partitions | Purpose |
| :--- | :--- | :---: | :--- |
| `conversation.transcript.final` | `session_id` | 3 | Committed, finalized transcript utterances |
| `conversation.transcript.partial` | `session_id` | 3 | High-frequency streaming partial hypotheses |
| `conversation.speech.activity` | `session_id` | 3 | Speech-onset transitions used for low-latency timer cancellation |
| `agent.commands` | `session_id` | 3 | Trigger commands dispatched to the LangGraph Agent Worker |
| `agent.suggestions` | `session_id` | 3 | Generated suggestions & relevance evaluation decisions |
| `agent.session.events` | `session_id` | 3 | Session lifecycle updates (`created`, `ended`, `purged`) |
| `agent.feedback` | `session_id` | 3 | Participant feedback signals on suggestion utility |
| `agent.dlq` | `session_id` | 3 | Dead Letter Queue for malformed or unprocessable messages |

### Idempotency & Delivery Guarantees

- **Partition Keying**: All events are keyed by `session_id`, ensuring strict sequential delivery within any single conversation partition. Across distinct topics (`conversation.transcript.final` vs `conversation.speech.activity`), cross-topic arrival ordering is not guaranteed by Kafka and is handled at the application layer.
- **Transactional Outbox**: Provides at-least-once message persistence by atomically committing message rows and outbox events in a single PostgreSQL transaction. Events remain `pending` in PostgreSQL until an ACK is received from Kafka with `acks=all`. If the publisher process restarts after sending to Kafka but before marking the event published, re-publication can occur.
- **Consumer Deduplication**: The consumer worker records each processed `event_id` in PostgreSQL (`processed_events`). When duplicate messages are redelivered, the consumer detects the prior record and skips execution, achieving effective at-most-once application of business side-effects.

---

## LangGraph Reasoning Workflow

The suggestion generation engine is implemented as a multi-step LangGraph workflow:

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────────┐
│  Understanding  ├────►│ Intent Detect├────►│ Relevance Evaluat │
└─────────────────┘     └──────────────┘     └─────────┬────────┘
                                                       │
                                          ┌────────────┴────────────┐
                                          ▼                         ▼
                                    [Not Relevant]             [Relevant]
                                          │                         │
                                          ▼                         ▼
                                      (Complete)        ┌───────────────────────┐
                                                        │Compact Context Selector│
                                                        └───────────┬───────────┘
                                                                    ▼
                                                        ┌───────────────────────┐
                                                        │     Planner Node      │
                                                        └───────────┬───────────┘
                                                                    ▼
                                                        ┌───────────────────────┐
                                                        │    Generator Node     │
                                                        └───────────┬───────────┘
                                                                    ▼
                                                        ┌───────────────────────┐
                                                        │    Validator Node     │
                                                        └───────────────────────┘
```

1. **Understanding Node**: Analyzes recent transcript utterances to extract active topics, technical entities, decision points, and unresolved questions.
2. **Intent Node**: Classifies the dialogue act of the last speaker (e.g., question, architectural proposal, clarification request, disagreement).
3. **Relevance Node**: Evaluates whether the current discussion intersects with the participant's role, responsibilities, or stated goals. If not relevant, execution safely terminates without incurring generator LLM cost.
4. **Compact Context Selector**: Compares the discussion topic with the participant's full profile to extract only intersecting keywords, reducing context-added token overhead by **24.2%** (saving 31 tokens out of 128 added tokens).
5. **Planner Node**: Determines the strategic angle of contribution (e.g., provide latency numbers, suggest an architectural alternative, raise an edge case).
6. **Generator Node**: Synthesizes 1–2 concise, actionable suggestions. Operates under strict anti-fabrication rules: suggestions must only refer to the participant's explicit expertise and never invent unverified work history.
7. **Validator Node**: Deterministic rule-based filter that verifies transcript grounding, prevents repetitive suggestions, and enforces strict length constraints (<300 characters).

---

## Session-Specific Participant Context

Conversa AI allows the operator to provide their personal background and participation context prior to starting a discussion.

### Schema & Invariants

```json
{
  "preferred_name": "Sandeep",
  "role": "Backend Developer",
  "responsibilities": "Audio pipeline and backend architecture",
  "expertise_keywords": ["Kafka", "PostgreSQL", "WebSockets", "FastAPI"],
  "session_goals": "Resolve consumer lag and optimize streaming performance",
  "constraints": ["Do not commit to unverified deployment timelines"]
}
```

- **Single-Operator Boundary**: Each session maintains exactly one participant context belonging to the operator. Conversa AI deliberately avoids multi-user diarization or complex voice fingerprinting.
- **1,500-Character Cumulative Limit**: The total character count across all context fields is strictly validated to prevent token bloating.
- **Privacy & Data Retention**: Stored in a JSONB column (`sessions.participant_context`). When the session is ended (`POST /v1/sessions/{id}/end`), the column is explicitly overwritten with SQL `NULL`, preventing persistent personal data retention.

### Compact Context Optimization & Empirical Token Measurements

The compact context selector extracts only keywords intersecting with active discussion topics while omitting personal names and irrelevant expertise:

| Prompt Configuration | Total Characters | Estimated Tokens | Incremental Context Overhead | Total Prompt Growth vs Baseline |
| :--- | :---: | :---: | :---: | :---: |
| **1. Baseline (No Participant Context)** | 4,430 | 1,107 | 0 tokens (reference) | — |
| **2. Raw / Uncompacted Context** | 4,943 | 1,235 | +128 tokens | +11.6% |
| **3. Compact Context Selector** | 4,816 | 1,204 | +97 tokens | +8.8% |

**Key Clarifications on Optimization**:
- **Context-Added Overhead Reduction**: The compact selector eliminates 31 tokens of context overhead ($\frac{128 - 97}{128} = \frac{31}{128} \approx \mathbf{24.2\%}$ reduction in the *added context overhead*).
- **Total Prompt Size**: Total prompt size decreases by **2.5%** (1,204 vs 1,235 tokens). It is **not** a 24.2% reduction in overall prompt size.
- **Controlled Prompt Growth**: With compact context enabled, the total prompt grows by only **+8.8%** (+97 tokens) over the unpersonalized baseline, compared to **+11.6%** (+128 tokens) when injecting raw context.

---

## Turn-Completion Intelligence (Gate 1)

Conversational turn detection determines when a speaker has finished their thought, allowing the assistant to prepare suggestions at the ideal moment.

### Mechanism & Pause Thresholds

The Trigger Controller analyzes transcript text and punctuation:
- **Questions (`?`)**: Dynamic pause window: `1,500 ms`. Questions invite responses and typically indicate immediate turn completion.
- **Complete Thoughts (`.`, `!`)**: Dynamic pause window: `2,500 ms`. Grammatically complete sentences require a moderate confirmation window.
- **Incomplete / Trailing Thoughts (commas, conjunctions like "and", "because", hesitation tokens like "um", "uh")**: Dynamic pause window: `4,500 ms`. Protects against prematurely interrupting a speaker pausing to think.

### Race Condition & Speech Resumption Protection

```
Transcript Final Event ──► Start Turn Timer (e.g. 2500ms)
                                  │
      ┌───────────────────────────┴───────────────────────────┐
      ▼                                                       ▼
Speaker Resumes Speaking                               Timer Expires (Quiet Window)
      │                                                       │
VAD Speech-Onset Transition                                   ▼
      │                                                Dispatch Agent Command
      ▼                                                to agent.commands Topic
conversation.speech.activity
      │
      ▼
Instantly Cancels Turn Timer
(Prevents Premature Interruption)
```

The dedicated Kafka topic `conversation.speech.activity` receives events on VAD speech-onset transitions. Because speech onset is detected within a 32 ms frame window by Silero VAD, pending turn timers are cancelled before stale commands can be dispatched, even if transcript finalization is still in progress.

---

## Privacy, Security & Data Boundaries

Conversa AI is designed with strict boundaries separating local audio processing from external LLM reasoning:

1. **Local Audio Ingestion & Transcription**:
   - Audio streaming and transcription execute entirely within the local container environment (`audio-gateway` and `transcription-service`).
   - Voice Activity Detection (Silero VAD) and Automated Speech Recognition (faster-whisper) run locally on CPU using local model weights.
   - Raw audio frames and voice recordings are **never** transmitted to cloud APIs or persisted to disk/database. Audio buffers in memory are bounded (30s maximum) and discarded immediately after processing.

2. **External LLM Inference Data Transmission**:
   - When real inference is enabled (`MOCK_LLM=false`), text prompts containing recent transcript excerpts, session goals, and filtered participant context are transmitted over HTTPS/TLS to the configured upstream LLM provider via OpenRouter.
   - When running in default budget-protection mode (`MOCK_LLM=true`), no external network requests are made; all suggestions are generated deterministically and locally.

3. **Session-Scoped Participant Context**:
   - The operator's participant profile (role, responsibilities, expertise) is strictly session-scoped.
   - Context is stored in a JSONB column (`sessions.participant_context`) in PostgreSQL and is explicitly purged to SQL `NULL` upon session termination (`POST /v1/sessions/{id}/end`). Permanent user profiles or cross-session tracking are not implemented.

4. **Access Control & Multi-User Boundary**:
   - Conversa AI is designed as a single-operator local assistant.
   - **No Multi-User Authentication**: REST endpoints and Delivery Service WebSockets (`/ws/delivery/{session_id}`) do not implement user authentication, JWT authorization, or encrypted per-user delivery channels. Any client with network access to the port can connect to a session ID.
   - In production or shared network deployments, authenticated reverse proxies, TLS termination, and network segmentation must be placed in front of all service ports.

---

## Technology Stack

| Layer | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Language & Runtime** | Python | 3.10+ | Core application runtime |
| **API Framework** | FastAPI | 0.115+ | High-performance asynchronous REST & WebSocket APIs |
| **Relational Database** | PostgreSQL | 16-alpine | Relational persistence, transactional outbox, and session state |
| **Database Driver** | asyncpg / SQLAlchemy | 2.0+ | Asynchronous database access and connection pooling |
| **Database Migrations** | Alembic | 1.13+ | Versioned database schema migrations |
| **Event Streaming** | Apache Kafka | 3.8.0 | Distributed event streaming in KRaft mode |
| **Kafka Client** | aiokafka | 0.11+ | Asynchronous Kafka producer and consumer |
| **Voice Activity Detection** | Silero VAD | v5 | Local, sub-millisecond voice activity detection on CPU |
| **Speech Recognition** | faster-whisper | 1.0+ | Fast CTranslate2 Whisper implementation (`tiny.en` int8) |
| **Agent Orchestration** | LangGraph | 0.2+ | Directed acyclic graph for conversation reasoning |
| **LLM Provider** | OpenRouter | — | Flexible multi-model LLM inference gateway |
| **Containerization** | Docker Compose | v2 | 10-service containerized architecture |
| **Test Framework** | pytest / pytest-asyncio | 8.0+ | Automated unit, integration, and performance testing |

---

## Repository Structure

```
conversa-ai/
├── client/                               # Single-page browser client
│   └── index.html                        # Workspace UI (Audio, Context, Transcripts, Suggestions)
├── docs/                                 # Documentation, demo, and screenshots
│   ├── audio_format_contract.md          # 16kHz PCM audio streaming protocol
│   ├── conversa-ai-demo.mp4              # Demonstration video recording (~7 MB)
│   └── screenshots/                      # Application workflow screenshots
├── infrastructure/                       # Deployment and infrastructure configuration
│   ├── docker-compose.yml                # 10-service container configuration
│   ├── kafka/
│   │   └── init-topics.sh                # Kafka topic creation script (partitions & replication)
│   └── postgres/
│       └── init.sql                      # Database initialization
├── services/
│   ├── agent-server/                     # Core Agent API, Worker, Controller, and LangGraph
│   │   ├── app/
│   │   │   ├── agent/                    # LangGraph workflow, nodes, and compact context selector
│   │   │   ├── api/                      # FastAPI routers (sessions, messages, suggestions)
│   │   │   ├── consumer/                 # Idempotent Kafka transcript consumer worker
│   │   │   ├── core/                     # Configuration, parsing, and Kafka producer
│   │   │   ├── database/                 # SQLAlchemy models, repositories, and engine
│   │   │   ├── orchestration/            # Trigger Controller & turn-completion debounce logic
│   │   │   ├── publisher/                # Transactional Outbox publisher worker
│   │   │   ├── schemas/                  # Pydantic validation schemas
│   │   │   └── worker/                   # Asynchronous LangGraph inference worker
│   │   ├── migrations/                   # Alembic database migrations (001 - 004)
│   │   └── tests/                        # 119 unit & integration tests
│   ├── audio-gateway/                    # WebSocket audio ingestion gateway (:8002)
│   ├── delivery-service/                 # WebSocket transcript & suggestion delivery (:8003)
│   └── transcription-service/            # Silero VAD + faster-whisper ASR worker (:8004)
├── shared/                               # Shared contracts and schemas
├── tests/                                # System verification, benchmarks, and canary scripts
│   ├── benchmark_speech_performance.py   # CPU latency and Real-Time Factor benchmark
│   ├── evaluation/                       # Evaluation framework & dataset integrity tests (11 tests)
│   ├── run_personalized_canary.py        # Controlled OpenRouter model canary verification
│   └── test_nemotron_inference_reliability.py # Model reliability test suite (14 tests)
├── tools/                                # Operational tools & verification scripts
│   ├── demo_browser_workflow.py          # Automated 7-step browser lifecycle verification
│   ├── serve_client.py                   # Local HTTP server for client UI (:3000)
│   └── verify_end_to_end_integration.py  # End-to-end multi-service verification script
├── .env.example                          # Sanitized environment configuration template
├── .gitignore                            # Secret and artifact exclusion rules
├── pyproject.toml                        # Workspace configuration
└── README.md                             # Documentation and portfolio overview
```

---

## Prerequisites & Docker Setup

### Prerequisites

- **Operating System**: macOS (Apple Silicon or Intel) or Linux (Ubuntu 22.04+ recommended).
- **Docker**: Docker Desktop 4.25+ or Docker Engine 24.0+ with Docker Compose v2.
- **Hardware**: Minimum 4 CPU cores and 8 GB RAM (runs entirely on CPU without GPU requirements).
- **Python** (optional, for running host tests): Python 3.10+ with `uv` or `pip`.

### Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/conversa-ai.git
   cd conversa-ai
   ```

2. **Configure environment variables**:
   ```bash
   cp .env.example .env
   ```
   > **Note**: The default `.env.example` is configured with `MOCK_LLM=true` so you can run the entire system, execute automated tests, and stream audio without incurring OpenRouter API charges. To enable real LLM responses, set `MOCK_LLM=false` and provide your `OPENROUTER_API_KEY`.

---

## Environment Configuration

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `APP_ENV` | `development` | Environment mode (`development`, `production`) |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `DATABASE_URL` | `postgresql+asyncpg://conversa_user:conversa_password@postgres:5432/conversa` | Async PostgreSQL connection string |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Internal Kafka broker address (`localhost:9092` from host) |
| `ENABLE_KAFKA_DELIVERY` | `true` | Enables Kafka consumer streaming in delivery service |
| `AGENT_AUTO_RUN` | `false` | When `true`, automatically dispatches trigger commands to agent worker |
| `AGENT_TRIGGER_MIN_NEW_MESSAGES`| `3` | Minimum new utterances before opportunity evaluation |
| `AGENT_TRIGGER_DEBOUNCE_MS` | `3000` | Silence quiet window (ms) before triggering agent |
| `AGENT_SESSION_COOLDOWN_SECONDS`| `20` | Minimum interval (s) between consecutive automated suggestions |
| `MOCK_LLM` | `true` | Safe default: returns deterministic mock suggestions without API costs |
| `AGENT_TURN_COMPLETION_ENABLED` | `false` | Gate 1 toggle: activates dynamic punctuation/syntactic pause windows |
| `AGENT_TURN_PAUSE_QUESTION_MS` | `1500` | Turn pause window for questions |
| `AGENT_TURN_PAUSE_COMPLETE_MS` | `2500` | Turn pause window for complete thoughts |
| `AGENT_TURN_PAUSE_INCOMPLETE_MS`| `4500` | Turn pause window for incomplete/trailing thoughts |
| `WHISPER_MODEL_SIZE` | `tiny.en` | faster-whisper model size (`tiny.en`, `base.en`) |
| `WHISPER_DEVICE` | `cpu` | Device (`cpu` or `cuda`) |
| `WHISPER_COMPUTE_TYPE` | `int8` | Model precision (`int8` for CPU, `float16` for GPU) |
| `OPENROUTER_API_KEY` | *(placeholder)* | OpenRouter API key for live inference |
| `OPENROUTER_MODEL` | `openrouter/free` | Target model identifier on OpenRouter |

---

## Starting the Application

### 1. Build and Start Docker Containers

Start all 10 distributed services with a single command:

```bash
docker compose -f infrastructure/docker-compose.yml up -d --build
```

### 2. Verify Container Health

```bash
docker compose -f infrastructure/docker-compose.yml ps
```

All containers should report `healthy` or `running`:

```
NAME                             STATUS                    PORTS
conversa-postgres                Up (healthy)              127.0.0.1:5432->5432/tcp
conversa-kafka                   Up (healthy)              127.0.0.1:9092->9092/tcp
conversa-agent-api               Up (healthy)              127.0.0.1:8001->8001/tcp
conversa-audio-gateway           Up (healthy)              127.0.0.1:8002->8002/tcp
conversa-delivery-service        Up (healthy)              127.0.0.1:8003->8003/tcp
conversa-transcription-service   Up (healthy)              127.0.0.1:8004->8004/tcp
conversa-outbox-publisher        Up                        —
conversa-consumer-worker         Up                        —
conversa-trigger-controller      Up                        —
conversa-agent-worker            Up                        —
```

### 3. Launch the Client Interface

In a separate terminal, serve the browser workspace:

```bash
python3 tools/serve_client.py --port 3000
```

Open `http://localhost:3000` in Google Chrome or any Chromium-based browser to access the application.

---

## API & WebSocket Reference

### 1. Audio Streaming WebSocket
`ws://localhost:8002/ws/audio/{session_id}`
- **Handshake**: The server transmits `{"status": "ready", "session_id": "..."}` upon connection.
- **Payload**: Binary frames of raw 16,000 Hz, 16-bit signed integer, mono PCM (`pcm_s16le`).

### 2. Transcript & Suggestion Delivery WebSocket
`ws://localhost:8003/ws/delivery/{session_id}`
- **Connected**: `{"type": "connected", "session_id": "..."}`
- **Partial Transcript**: `{"type": "partial", "session_id": "...", "text": "We are seeing lag", "timestamp": ...}`
- **Final Transcript**: `{"type": "final", "session_id": "...", "text": "We are seeing lag when audio chunks arrive.", "sequence_number": 1, ...}`
- **Suggestion**: `{"type": "suggestion", "session_id": "...", "suggestions": [...], "relevance_score": 0.85}`

### 3. Agent Server REST Endpoints (:8001)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/v1/sessions` | Initialize a discussion session with optional participant context |
| `GET` | `/v1/sessions/{id}` | Retrieve session status and configuration |
| `PUT` | `/v1/sessions/{id}/participant-context` | Update session participant profile |
| `DELETE`| `/v1/sessions/{id}/participant-context`| Clear participant context |
| `POST` | `/v1/sessions/{id}/end` | End session and purge participant context to `NULL` |
| `POST` | `/v1/sessions/{id}/messages` | Append transcript utterance (writes message + outbox event) |
| `GET` | `/v1/sessions/{id}/messages` | Retrieve chronological discussion transcript |
| `POST` | `/v1/sessions/{id}/suggest` | Manually trigger LangGraph suggestion generation |

### 4. Health & Monitoring Endpoints

- Agent API Health: `GET http://localhost:8001/health/live` & `/health/ready`
- Audio Gateway Stats: `GET http://localhost:8002/health/live` & `/stats`
- Delivery Service Stats: `GET http://localhost:8003/health/live` & `/stats`
- Transcription Service Stats: `GET http://localhost:8004/health/live` & `/metrics`

---

## Automated Testing & Benchmarks

The repository includes comprehensive automated test suites covering speech ingestion, event streaming, transaction isolation, parsing, and reasoning. All 144 unit and integration tests execute locally in seconds without incurring external API costs.

### Running Test Suites

```bash
# 1. Run Core Agent, Persistence, Outbox & Speech tests (119 passed)
services/agent-server/.venv/bin/pytest services/agent-server/tests -v

# 2. Run Model Inference Reliability tests (14 passed)
PYTHONPATH=services/agent-server:. services/agent-server/.venv/bin/pytest tests/test_nemotron_inference_reliability.py -v

# 3. Run Evaluation Framework tests (11 passed)
services/agent-server/.venv/bin/pytest -o pythonpath=. tests/evaluation/test_evaluation_framework.py -v
```

### Running Speech Latency Benchmarks

The speech pipeline performance can be benchmarked using the standalone benchmark script:

```bash
services/agent-server/.venv/bin/python tests/benchmark_speech_performance.py
```

#### Benchmark Specification & Methodology
- **Benchmark Source Script**: [`tests/benchmark_speech_performance.py`](tests/benchmark_speech_performance.py)
- **Audio Fixture**: [`tests/fixtures/audio/sample_utterance.pcm`](tests/fixtures/audio/sample_utterance.pcm) (a 2.50-second 16 kHz 16-bit mono PCM utterance)
- **Hardware & Environment**: Apple Silicon host CPU (MacBook, macOS Darwin ARM64, 8 cores). Model inference executed on CPU with INT8 quantization (`WHISPER_COMPUTE_TYPE=int8`, `WHISPER_MODEL_SIZE=tiny.en`).
- **Methodology**:
  - *Audio Ingestion*: 500 push/pop cycles of 100 ms chunks into the in-memory circular buffer.
  - *Silero VAD*: 200 evaluations over a synthetic 32 ms frame (512 samples) using ONNX runtime on CPU.
  - *ASR Inference*: 5 warm-started transcriptions of the 2.50s audio sample with `faster-whisper`.
  - *Segment Manager Pipeline*: Streaming simulation feeding 100 ms chunks through `SpeechSegmentManager` with mocked network calls (`_post_to_agent_server` and `_dispatch_to_delivery_service` are `AsyncMock`).

#### Measured Microbenchmark Results (Apple Silicon CPU, tiny.en INT8)

| Stage | Benchmark Metric | Measured Result | Isolated RTF |
| :--- | :--- | :---: | :---: |
| **Audio Ingestion Buffer** | In-memory push/pop (500 cycles) | **0.001 ms** (P50) / **0.010 ms** (P95) | — |
| **Voice Activity Detection** | Silero VAD per 32 ms frame (200 cycles) | **0.075 ms** (P50) / **0.078 ms** (P95) | ~0.0024x |
| **Speech Transcription** | faster-whisper `tiny.en` on 2.50s audio (5 runs) | **~111 ms** mean latency | ~0.045x |
| **Partial Hypothesis** | 0.5s audio slice transcription | **~231 ms** mean latency | ~0.46x |
| **In-Memory Streaming Pipeline** | Entire 2.5s streaming sample (mocked network) | **~161 ms** total execution time | ~0.064x |
| **Process Memory Footprint** | Peak Resident Set Size (RSS) | **~570 MB** (macOS Darwin report) | — |

> **Important Benchmark Caveats**:
> 1. These results reflect **isolated microbenchmarks** on Apple Silicon host hardware with network I/O mocked.
> 2. They do **not** represent end-to-end distributed round-trip latency across Docker containers, WebSocket network transport, or database transaction commit times.
> 3. CPU transcription latency will be substantially higher when using larger Whisper models (`base.en`, `small.en`, `medium.en`). For production deployments requiring larger models, GPU acceleration (`WHISPER_DEVICE=cuda`) is recommended.

### Running Automated End-to-End Integration Verification

```bash
python3 tools/verify_end_to_end_integration.py
```

Verifies the 7-step system lifecycle against the running Docker cluster: session creation, WebSocket audio ingestion, speech transcription, message persistence, outbox publication, manual suggestion generation, and participant context purge to `NULL`.

---

## Demo Video

[Watch Conversa AI Demo](docs/conversa-ai-demo.mp4)

The recorded demo illustrates the end-to-end local streaming and suggestion workflow:
- **Audio Capture & Streaming**: 16 kHz mono PCM audio streamed via WebSocket to the Audio Gateway.
- **Local Speech Processing**: Sub-millisecond Silero VAD and faster-whisper `tiny.en` transcription running entirely in local containers, generating low-latency partial hypotheses and finalized transcript utterances.
- **Manual Request Suggestion Workflow**: Suggestions are requested via the manual "Request Suggestion" button (`POST /v1/sessions/{id}/suggest`), matching the default operational configuration (`AGENT_AUTO_RUN=false`).
- **Deterministic Mock LLM Mode**: Executed with `MOCK_LLM=true` to demonstrate role-grounded suggestion synthesis without cloud API token costs or latency fluctuations.

---

## Application Screenshots

Key operational moments captured from the recorded demonstration session:

### Session Initialization

![Session Initialization](<docs/screenshots/Screenshot 2026-11-01 at 4.26.21 PM.png>)

*Initial workspace state showing single-operator participant profile (Sandeep, Backend Developer) and session goals configured prior to starting the session.*

### Active Session & Live Audio Capture

![Active Session & Microphone In Use](<docs/screenshots/Screenshot 2026-11-01 at 4.26.37 PM.png>)

*Session initialized with unique UUID, participant profile verified and saved under single-operator isolation, and live microphone capture active.*

### Live Transcription

![Live Transcription](<docs/screenshots/Screenshot 2026-11-01 at 4.27.21 PM.png>)

*Real-time audio level metering alongside live partial hypothesis streaming and chronological finalized utterances.*

### Manual Suggestion Request

![Manual Suggestion Request](<docs/screenshots/Screenshot 2026-11-01 at 4.27.29 PM.png>)

*Operator manually triggering suggestion generation via the "Request Suggestion" button after four finalized discussion utterances.*

### Context-Aware Suggestions

![Generated Suggestions](<docs/screenshots/Screenshot 2026-11-01 at 4.27.53 PM.png>)

*Structured, role-aligned `TECHNICAL_EXPLANATION` suggestion delivered under mock LLM mode (`MOCK_LLM=true`), addressing consumer lag and Kafka tuning without ungrounded fabrication.*

---

## Current Limitations

1. **Single-Operator Boundary**: Conversa AI is designed as a personal assistant for a single participant. It does not perform speaker diarization (voice identification across multiple speakers in the room).
2. **CPU Inference Latency**: While faster-whisper `tiny.en` executes in ~111 ms on CPU, larger models (`medium.en`, `large-v3`) require dedicated GPU hardware (`WHISPER_DEVICE=cuda`) for real-time responsiveness.
3. **Turn-Completion Feature Flag**: Gate 1 dynamic turn debouncing is disabled by default (`AGENT_TURN_COMPLETION_ENABLED=false`) to ensure predictable manual testing. It can be enabled by setting the environment variable to `true`.
4. **Inference Rate Limits**: When using public free-tier models via OpenRouter, inference can experience provider queuing or upstream rate limits during peak usage.

---

## Future Improvements & Roadmap

- [ ] **Speaker Diarization**: Integration of `pyannote-audio` to isolate the operator's voice from other meeting participants.
- [ ] **WebRTC Audio Transport**: Replace raw WebSocket PCM with WebRTC Opus streaming to provide built-in jitter buffering, packet loss concealment, and reduced uplink bandwidth.
- [ ] **Semantic Session Retrieval**: PostgreSQL `pgvector` integration to index historical discussion transcripts and retrieve relevant historical context during long-running sessions.
- [ ] **Voice Output (TTS)**: Low-latency local text-to-speech feedback (e.g., via Piper TTS or Kokoro) delivered directly into the operator's earpiece.
- [ ] **Multi-Tenant Enterprise RBAC**: Tenant isolation with OAuth2/OIDC authentication and team-level discussion archives.

---

## Highlights

### Project Summary
> **Conversa AI**: An agentic, real-time conversational intelligence platform that streams 16kHz PCM audio, extracts transcripts using local Silero VAD and Whisper, evaluates turn-completion dynamics, and generates role-grounded contribution suggestions using LangGraph and Kafka.

### Core Technical Accomplishments
- **Engineered an event-driven speech architecture** combining local Silero VAD (~0.075ms CPU frame latency) and faster-whisper tiny.en (~111ms ASR on 2.5s speech) to process 16kHz PCM audio over WebSockets entirely within local containers without cloud ASR data transmission.
- **Implemented the Transactional Outbox pattern** with PostgreSQL 16 and Apache Kafka 3.8.0 (KRaft mode), utilizing `SELECT FOR UPDATE SKIP LOCKED` and consumer idempotency tables to provide at-least-once broker delivery and eliminate dual-write inconsistencies.
- **Architected a 6-node LangGraph reasoning pipeline** that extracts dialogue acts, evaluates discussion relevance against participant background, and synthesizes grounded suggestions with deterministic rule-based output validation.
- **Developed a compact context selector** that reduces participant context token overhead by **24.2%** (limiting prompt token growth to +8.8% over baseline) while enforcing strict anti-fabrication prompt constraints, single-operator JSONB isolation, and automated context purging on session termination.
- **Designed a dynamic turn-completion state machine** that modulates debounce timers (1.5s–4.5s) using syntactic completion heuristics, cancelling stale LLM trigger dispatches via a dedicated `conversation.speech.activity` Kafka topic upon speech resumption.

### Core Stack
`Python 3.10` • `FastAPI` • `PostgreSQL 16` • `SQLAlchemy 2.x (asyncpg)` • `Apache Kafka 3.8.0 (KRaft)` • `Silero VAD` • `faster-whisper` • `LangGraph` • `WebSockets` • `Docker Compose` • `pytest`

---

## License

Open-source license terms (e.g. MIT License) are subject to repository owner review and final approval prior to public release. See [LICENSE](LICENSE) for details.
