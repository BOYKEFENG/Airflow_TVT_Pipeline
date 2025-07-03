# Airflow TVT Pipeline

This project automates retrieval, processing, and storage of transportation and economic data using Apache Airflow and Docker.

## Project Structure

```
airflow-tvt-pipeline/
├── dags/                    # Airflow DAG definitions (.py)
├── scripts/                 # Python ETL scripts called by DAGs
├── data/                    # Raw and processed datasets
│   ├── tvt/
│   ├── bea/
│   └── bls/
├── docker-compose.yaml      # Docker Compose configuration
├── Dockerfile              # (optional) custom Airflow image build
├── requirements.txt        # Python dependencies
├── .env                    # environment variables (API keys, etc.)
├── .gitignore              # files and folders ignored by Git
└── README.md               # this file
```

## Prerequisites

- Docker (version 20.10 or later)
- Docker Compose (version 1.29 or later)
- Git
- (Optional) Python 3.11+ for local testing of scripts

## Configuration

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/airflow-tvt-pipeline.git
cd airflow-tvt-pipeline
```

### 2. Create environment file

Create a `.env` file in the project root with any required settings:

```ini
AIRFLOW__CORE__FERNET_KEY=<your_fernet_key>
AIRFLOW__CORE__EXECUTOR=LocalExecutor
API_KEY_BEA=<your_bea_key>
API_KEY_BLS=<your_bls_key>
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Edit or add DAG files

- Place your DAG definitions in the `dags/` directory
- Ensure each DAG has a unique `dag_id` and a valid `schedule_interval`

## Launch Airflow with Docker

> **Note:** By default this setup mounts your local `dags/` and `scripts/` folders into the containers, so code changes appear immediately.

### 1. Start all services

```bash
docker-compose up -d
```

### 2. Initialize the metadata database (first run only)

```bash
docker-compose run airflow-init
```

### 3. Access the Airflow UI

Open your browser at `http://localhost:8080`

- **Username:** `airflow`
- **Password:** `airflow`

## Enable and Trigger a DAG

1. In the Airflow UI, go to **DAGs**
2. Toggle **ON** the DAG you want (e.g. `monthly_tvt_update`)
3. Click the **Trigger** (▶) button to run immediately, or wait for its schedule

## Updating DAG Code

### Using volume mounts (default)
- Edit files under `dags/` or `scripts/`
- Airflow will auto-detect changes (~30 second poll)

### If you bake code into the image
If using `COPY` in your Dockerfile:

1. Rebuild the image:
   ```bash
   docker-compose build
   ```

2. Restart services:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

## Viewing Logs

### In the Airflow UI
Select a task instance ▶ **Log**

### Inside the scheduler container
```bash
docker exec -it airflow-scheduler bash
cd /opt/airflow/logs/<dag_id>/<task_id>/<execution_date>/
cat *.log
```

## Data Processing Scripts

The project includes specialized scripts for processing TVT (Travel Volume Trends) data:

- `data_prep.py` - Downloads and preprocesses TVT Excel files from FHWA
- `merge_tvt_data.py` - Merges TVT data into consolidated VMT datasets
- `merge_tvt_page456.py` - Processes state mileage data from TVT reports

## Cleanup

### Stop and remove containers
```bash
docker-compose down
```

### Remove volumes and orphan containers
```bash
docker-compose down --volumes --remove-orphans
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues, please open an issue in the GitHub repository.
