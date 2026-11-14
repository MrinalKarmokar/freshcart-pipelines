# freshcart-pipelines

**Assignment: From Git to Gold** — Data Engineering · Pipeline Orchestration

> Version control, ELT, ML pipelines and event streaming, end to end.

---

## Repository Structure

```
freshcart-pipelines/
├── elt/                  ← FreshCart ELT pipeline (Part B)
├── iris_project/         ← Iris / MNIST ML pipeline with Prefect (Part C)
├── stream/               ← Zomato-style food-order stream with RabbitMQ (Part D)
├── tests/
│   └── test_prepare_data.py
├── .github/workflows/
│   └── ci.yml            ← GitHub Actions CI pipeline
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Parts Overview

| Part | Topic | Key technology |
|------|-------|---------------|
| A | Git & CI/CD | Git, GitHub Actions |
| B | FreshCart ELT | Snowflake, Python |
| C | Iris / MNIST ML pipeline | Keras, Prefect, Snowflake |
| D | Zomato food-order stream | RabbitMQ (topic exchange) |

---

## Quick Start

> This project uses [**uv**](https://docs.astral.sh/uv/) for fast, reproducible Python environments.
> Install uv once with: `pip install uv` or `winget install astral-sh.uv`

1. **Clone the repo**
   ```bash
   git clone https://github.com/<your-username>/freshcart-pipelines.git
   cd freshcart-pipelines
   ```

2. **Create virtual environment and install all dependencies**
   ```bash
   uv venv          # creates .venv/
   uv sync          # installs from uv.lock (exact versions, reproducible)
   ```

3. **Activate the environment**
   ```bash
   # Windows (PowerShell)
   .venv\Scripts\activate

   # macOS / Linux
   source .venv/bin/activate
   ```

4. **Set secrets** — copy `.env.example` to `.env` and fill in your credentials:
   ```
   SNOWFLAKE_ACCOUNT=...
   SNOWFLAKE_USER=...
   SNOWFLAKE_PASSWORD=...
   RABBITMQ_URL=...
   ```

5. **Run tests locally**
   ```bash
   uv run pytest tests/
   ```

### Adding a new package
```bash
uv add <package-name>          # adds to pyproject.toml + updates uv.lock
uv add --dev <package-name>    # adds to dev dependency group
```

---

## Git Four-Box Flow

```
Working Folder  →  git add  →  Staging Area
                                    ↓
                              git commit
                                    ↓
                            Local Repository
                                    ↓
                               git push
                                    ↓
                          Remote (GitHub) + CI
```

Each commit triggers the CI workflow, which runs the test suite automatically.

---

## Platform Requirements

- Python 3.10+
- Google Colab (for running notebooks interactively)
- Snowflake trial account
- GitHub account (CI/CD via GitHub Actions)
- RabbitMQ (CloudAMQP free tier or Docker)
