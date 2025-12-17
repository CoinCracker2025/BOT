# Paid Runners Scanner

Dashboard Streamlit pour scanner les tokens Solana via Dexscreener et afficher les runners payants.

## Prérequis
- Python 3.10+
- `pip install -r requirements.txt`

## Démarrer le dashboard
```bash
python -m venv .venv
source .venv/bin/activate  # Sur Windows: .venv\Scripts\activate
pip install -r requirements.txt

streamlit run app.py
```

La configuration et la blacklist sont persistées dans `scanner_config.json`.
