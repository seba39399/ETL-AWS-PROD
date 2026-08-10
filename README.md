# PySpark ETL Pipeline & AWS S3 Automated Deployment

A robust ETL (_Extract, Transform, Load_) data processing pipeline built with **PySpark**, containerized using **Docker**, and integrated with a Continuous Integration & Continuous Deployment (**CI/CD**) workflow to **AWS S3** for Apache Airflow orchestration.

---

## Architecture & Tech Stack

- **Language:** Python 3.10
- **Processing Engine:** PySpark (Spark 3.x, OpenJDK 17)
- **Containerization:** Docker
- **Testing & Code Quality:** PyTest, Flake8
- **Orchestration & Storage:** Apache Airflow, AWS S3
- **CI/CD Pipeline:** GitHub Actions

---

## Repository Structure

```text
├── .github/
│   └── workflows/
│       └── ci_cd.yml        # GitHub Actions CI/CD pipeline definition
├── dags/                    # Apache Airflow DAG definitions
├── scripts_spark/           # PySpark ETL transformation scripts
├── tests/                   # Unit & integration test suites
├── .gitignore               # Excludes database files, logs, and caches
├── Dockerfile               # Optimized container image build configuration
├── requirements.txt         # Python project dependencies
└── README.md                # Project documentation
```

## CI/CD Pipeline (GitHub Actions)

The workflow defined in `.github/workflows/ci_cd.yml` automates testing and deployment in two stages:

1. **Code Quality and Tests (`code_quality_tests`):**
   - Builds an isolated Docker container based on Python 3.10 and OpenJDK 17.
   - Runs linter checks (`Flake8`) to enforce PEP8 syntax standards.
   - Executes the automated unit test suite (`PyTest`) within the containerized environment.

2. **Deploy to AWS S3 (`deploy_to_s3_airflow`):**
   - Automatically triggers after a successful push to the `main` branch.
   - Authenticates securely with AWS using GitHub Secrets.
   - Synchronizes `./dags` and `./scripts_spark` directories directly to your S3 bucket (`s3://etlprodtest/airflow/`).

Author

Developed by Juan Sebastián Peña Valderrama
