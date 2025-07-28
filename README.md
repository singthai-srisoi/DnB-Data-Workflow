# DnB Data Processing

## How to run
```bash
streamlit run main.py
```

## Setup
1. Setup venv
```bash
py -m venv venv  
```
Activate venv
```bash
venv/scripts/activate
```
Install package
```bash
pip install -r requirements.txt
```

2. Create config.toml
```toml
[DATABASE]
file = "data.db"
```

3. Migrate
```bash
alembic upgrade head
```

4. Run
```
.\run-webui.bat
```