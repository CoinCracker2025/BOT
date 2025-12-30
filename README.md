# Paid Runners Scanner

Dashboard Streamlit pour scanner les tokens Solana via Dexscreener et afficher les runners payants.

## Prérequis
- Python 3.10+
- `pip install -r requirements.txt`

## Démarrer le dashboard
```bash
python -m venv .venv

# Windows (PowerShell):
.venv\\Scripts\\Activate.ps1
# Windows (cmd):
.venv\\Scripts\\activate.bat
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt

streamlit run app.py
```

La configuration et la blacklist sont persistées dans `scanner_config.json`.

## Vérifier rapidement la connexion aux API DexScreener

Pour vérifier que les endpoints publics DexScreener sont accessibles depuis la machine courante, lance une vérification légère :

```bash
python paid_runners_bot.py --check
```

Les résultats détaillent le statut de chaque endpoint interrogé ainsi que le debug HTTP.
