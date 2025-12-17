# app.py (V6.9.progress -> V6.9.progress.ads_source)
# Streamlit UI for the Paid Runners Scanner (Dexscreener / Solana).
# V6.9: expose source column in results and keep 'Inclure ads' checkbox (when checked ads are now candidates).

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
import streamlit as st

from paid_runners_bot import run_scan_for_modes

APP_TITLE = "Scanner de runners - Dexscreener / Solana"
CONFIG_PATH = Path(__file__).with_name("scanner_config.json")


def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_config(cfg: Dict[str, Any]) -> None:
    try:
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def solana_get_balance(wallet: str, rpc_url: str, timeout_s: int = 12) -> Optional[float]:
    wallet = (wallet or "").strip()
    if not wallet:
        return None
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [wallet]}
    try:
        r = requests.post(rpc_url, json=payload, timeout=timeout_s)
        r.raise_for_status()
        data = r.json() or {}
        lamports = (data.get("result") or {}).get("value")
        if lamports is None:
            return None
        return float(lamports) / 1_000_000_000.0
    except Exception:
        return None


_STYLES = """
<style>
/* Soft card look for controls */
.control-card {
  border: 1px solid rgba(49, 51, 63, 0.2);
  border-radius: 12px;
  padding: 14px 14px 6px 14px;
  margin-bottom: 14px;
  background: rgba(250, 250, 252, 0.35);
}
.right-card {
  border: 1px solid rgba(49, 51, 63, 0.2);
  border-radius: 12px;
  padding: 10px 12px;
  background: rgba(250, 250, 252, 0.25);
}
.small-muted {
  font-size: 12px;
  opacity: 0.75;
}
.mode-pill {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  border: 1px solid rgba(49, 51, 63, 0.18);
  margin-right: 8px;
  margin-bottom: 8px;
  font-size: 12px;
  background: rgba(36, 161, 72, 0.10);
}

.ext-link-btn {
  display: inline-block;
  width: 100%;
  padding: 10px 12px;
  text-align: center;
  border-radius: 8px;
  border: 1px solid rgba(49, 51, 63, 0.25);
  background: linear-gradient(135deg, #f9fafb, #eef1f5);
  color: #0f1116;
  text-decoration: none;
  font-weight: 600;
}

.ext-link-btn:hover {
  color: #0f1116;
  border-color: rgba(49, 51, 63, 0.45);
  background: linear-gradient(135deg, #eef1f5, #e3e7ed);
}
</style>
"""


def _render_external_link(label: str, url: str) -> None:
    safe_url = url or "#"
    st.markdown(
        f'<a class="ext-link-btn" href="{safe_url}" target="_blank" rel="noopener noreferrer">{label}</a>',
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.markdown(_STYLES, unsafe_allow_html=True)

    cfg = load_config()

    # Normalize / default new persisted settings
    if "token_blacklist" not in cfg:
        # Backward-compatible key support if you ever used another name
        cfg["token_blacklist"] = cfg.get("blocked_token_addresses") or []
    if "visible_columns" not in cfg:
        cfg["visible_columns"] = cfg.get("ui_visible_columns") or []
    if "max_display_rows" not in cfg:
        cfg["max_display_rows"] = int(cfg.get("max_rows", 200) or 200)

    # Session state holders
    if "last_scan_debug" not in st.session_state:
        st.session_state["last_scan_debug"] = None
    if "last_scan_df" not in st.session_state:
        st.session_state["last_scan_df"] = pd.DataFrame()
    if "token_blacklist_entries" not in st.session_state:
        st.session_state["token_blacklist_entries"] = []

    # Header
    col_h1, col_h2 = st.columns([3.8, 1], gap="large")
    with col_h1:
        st.markdown(f"# {APP_TITLE}")
        st.markdown(
            "Découvre les tokens Solana qui pumpent maintenant - triés par *spike score* et enrichis avec Dexscreener."
        )
    with col_h2:
        st.info("Mode pump: activable dans les options", icon="🚀")

    # Controls layout
    left, right = st.columns([3.4, 1.1], gap="large")

    with left:
        st.markdown('<div class="control-card">', unsafe_allow_html=True)
        st.subheader("Modes & paramètres")
        all_modes = ["ultra_early", "early_strict", "early", "strict", "degen"]
        default_modes = cfg.get("modes") or ["early_strict", "early", "strict", "degen"]
        modes = st.multiselect(
            "Sélectionne un ou plusieurs modes",
            options=all_modes,
            default=[m for m in default_modes if m in all_modes],
        )
        if modes:
            pill_html = " ".join([f'<span class="mode-pill">{m}</span>' for m in modes])
            st.markdown(pill_html, unsafe_allow_html=True)

        col_sl1, col_sl2 = st.columns([1, 1])
        with col_sl1:
            top_n = st.slider(
                "Nombre max de résultats", min_value=5, max_value=100, value=int(cfg.get("top_n", 20)), step=1
            )
        with col_sl2:
            candidates_max = st.slider(
                "Candidates max (Dexscreener)", min_value=50, max_value=1000, value=int(cfg.get("candidates_max", 250)), step=50
            )

        st.write("")
        st.subheader("Sources & filtres")

        col_ck1, col_ck2, col_ck3 = st.columns([1, 1, 1])
        with col_ck1:
            include_boosts = st.checkbox("Inclure Boosts", value=bool(cfg.get("include_boosts", True)))
            include_profiles = st.checkbox("Inclure Token Profiles", value=bool(cfg.get("include_profiles", True)))
        with col_ck2:
            include_cto = st.checkbox("Inclure CTO", value=bool(cfg.get("include_cto", True)))
            include_ads = st.checkbox("Inclure Ads", value=bool(cfg.get("include_ads", False)))
        with col_ck3:
            include_orders = st.checkbox("Inclure orders/placements (coûteux)", value=bool(cfg.get("include_orders", False)))
            unique_per_token = st.checkbox("Unique par token (réduit doublons)", value=bool(cfg.get("unique_per_token", True)))

        st.write("")
        col_opt1, col_opt2, col_opt3 = st.columns([1, 1, 1])
        with col_opt1:
            anti_dead = st.checkbox("Filtre anti-tokens morts", value=bool(cfg.get("anti_dead", True)))
            trending_filters = st.checkbox("Filtres 'trending' (min liq/vol/netbuy)", value=bool(cfg.get("trending_filters", True)))
        with col_opt2:
            verbose_debug = st.checkbox("Debug verbeux (why_filtered)", value=bool(cfg.get("verbose_debug", False)))
            pump_mode = st.checkbox("Mode pump agressif", value=bool(cfg.get("pump_mode", True)))
        with col_opt3:
            sort_by_spike = st.checkbox("Trier par spike score (priorise pumps)", value=True)

            with st.expander("Seuils avancés (facultatif)", expanded=False):
                trending_min_liq = st.number_input(
                    "Min liquidité (USD)",
                    value=float(cfg.get("trending_min_liquidity", 15000.0)),
                    step=1000.0,
                )
                trending_min_vol1h = st.number_input(
                    "Min vol 1h (USD)",
                    value=float(cfg.get("trending_min_vol1h", 1000.0)),
                    step=100.0,
                )
                trending_min_vol5m = st.number_input(
                    "Min vol 5m (USD)",
                    value=float(cfg.get("trending_min_vol5m", 500.0)),
                    step=50.0,
                )
                trending_min_netbuy5m = st.number_input(
                    "Min netBuy 5m",
                    value=int(cfg.get("trending_min_netbuy5m", 2)),
                    step=1,
                )
                spike_score_min = st.number_input(
                    "Min spike_score",
                    value=float(cfg.get("spike_score_min", 0.25)),
                    step=0.05,
                    format="%.3f",
                )

        # -----------------------
        # Display preferences (persisted)
        # -----------------------
        st.subheader("Affichage")

        max_display_rows = st.slider(
            "Résultats affichés",
            min_value=10,
            max_value=500,
            value=int(cfg.get("max_display_rows", 200)),
            step=10,
            help="Limite uniquement l'affichage. Le scan conserve ses résultats complets en mémoire.",
        )

        # Column visibility (persisted) - based on last scan dataframe
        last_df = st.session_state.get("last_scan_df")
        last_cols = list(last_df.columns) if isinstance(last_df, pd.DataFrame) and (not last_df.empty) else []
        with st.expander("Colonnes visibles (persistant)", expanded=False):
            if not last_cols:
                st.caption("Lance un premier scan pour charger la liste des colonnes disponibles.")
                visible_columns = cfg.get("visible_columns") or []
                reset_cols = False
            else:
                saved_cols = [c for c in (cfg.get("visible_columns") or []) if c in last_cols]
                # Default: all columns if none saved
                default_cols = saved_cols if saved_cols else last_cols
                visible_columns = st.multiselect(
                    "Choisis les colonnes à afficher",
                    options=last_cols,
                    default=default_cols,
                    help="Astuce: ce réglage remplace le masquage manuel dans le tableau (qui n'est pas persistable).",
                )
                reset_cols = st.button("Réinitialiser: toutes les colonnes", use_container_width=True)

            if reset_cols:
                visible_columns = last_cols
                st.success("Colonnes réinitialisées.")

        # -----------------------
        # Token blacklist (persisted)
        # -----------------------
        st.subheader("Exclusions")
        with st.expander("Blacklist (CA) - exclure des tokens", expanded=False):
            st.caption("Un tokenAddress (CA) par ligne. Les tokens listés seront retirés de l'affichage des résultats.")
            cfg_blacklist = cfg.get("token_blacklist") or []
            # Sync session state with config (preserve manual edits)
            if not st.session_state["token_blacklist_entries"]:
                st.session_state["token_blacklist_entries"] = [
                    str(x).strip() for x in cfg_blacklist if str(x).strip()
                ]
            bl_text = st.text_area(
                "CA blacklistés",
                value="\n".join(st.session_state["token_blacklist_entries"]),
                height=140,
                placeholder="Ex:\nGE5BJqTsVWfgv8qa6zQ6WFnLQGRATu3StN59kmTFpump",
                key="token_blacklist_text",
            )
            parsed = [line.strip() for line in (bl_text or "").splitlines() if line.strip()]
            # De-dup while preserving order
            parsed = list(dict.fromkeys(parsed))
            st.session_state["token_blacklist_entries"] = parsed
            st.caption(f"{len(parsed)} entrée(s) dans la blacklist :")
            if parsed:
                st.code("\n".join(parsed))
            c_bl1, c_bl2 = st.columns([1, 1])
            with c_bl1:
                save_bl = st.button("Enregistrer la blacklist", use_container_width=True)
            with c_bl2:
                clear_bl = st.button("Vider la blacklist", use_container_width=True)

            token_blacklist = parsed
            if clear_bl:
                token_blacklist = []
                st.session_state["token_blacklist_entries"] = []
                st.session_state["token_blacklist_text"] = ""
                st.success("Blacklist vidée.")
            elif save_bl:
                st.success(f"Blacklist enregistrée ({len(token_blacklist)} CA).")

        st.markdown("</div>", unsafe_allow_html=True)

        st.write("")  # spacing
        scan_btn = st.button("Lancer un scan maintenant", type="primary", use_container_width=False)

    with right:
        st.markdown('<div class="right-card">', unsafe_allow_html=True)
        default_wallet = cfg.get("wallet", "")
        default_rpc = cfg.get("rpc", "https://api.mainnet-beta.solana.com")
        wallet = st.text_input("Adresse wallet (Solana)", value=default_wallet, placeholder="Ex: ...")
        rpc = st.text_input("RPC (Solana)", value=default_rpc)
        refresh = st.button("Rafraîchir solde", key="refresh_balance")
        balance = None
        if refresh:
            balance = solana_get_balance(wallet, rpc)
            st.session_state["last_balance"] = balance
        else:
            balance = st.session_state.get("last_balance")
        if balance is not None:
            st.metric("Solde SOL", f"{balance:.4f} SOL")
        else:
            st.caption("Solde SOL: (clique sur 'Rafraîchir solde')", unsafe_allow_html=True)
        budget_pct = st.slider(
            "Budget par trade (%)",
            min_value=0.5,
            max_value=25.0,
            value=float(cfg.get("budget_pct", 7.0)),
            step=0.5,
        )
        if balance is not None:
            st.caption(f"Indication: {budget_pct:.1f}% = {(balance * budget_pct / 100.0):.4f} SOL")
        st.markdown("</div>", unsafe_allow_html=True)

    # Persist config
    cfg_update = {
        "wallet": wallet,
        "rpc": rpc,
        "budget_pct": budget_pct,
        "modes": modes,
        "top_n": top_n,
        "candidates_max": candidates_max,
        "anti_dead": anti_dead,
        "include_boosts": include_boosts,
        "include_profiles": include_profiles,
        "include_cto": include_cto,
        "include_ads": include_ads,
        "include_orders": include_orders,
        "trending_min_liquidity": float(trending_min_liq),
        "trending_min_vol1h": float(trending_min_vol1h),
        "trending_min_vol5m": float(trending_min_vol5m),
        "trending_min_netbuy5m": int(trending_min_netbuy5m),
        "unique_per_token": unique_per_token,
        "trending_filters": trending_filters,
        "verbose_debug": verbose_debug,
        "pump_mode": pump_mode,
        "spike_score_min": float(spike_score_min),
    }

    # Add persisted UI preferences
    cfg_update["token_blacklist"] = st.session_state.get("token_blacklist_entries") or []
    cfg_update["visible_columns"] = visible_columns if "visible_columns" in locals() else (cfg.get("visible_columns") or [])
    cfg_update["max_display_rows"] = int(max_display_rows) if "max_display_rows" in locals() else int(cfg.get("max_display_rows", 200))

    # Merge-save to avoid dropping unrelated keys
    cfg = {**cfg, **cfg_update}
    save_config(cfg)

    if "last_scan_debug" not in st.session_state:
        st.session_state["last_scan_debug"] = None
    if "last_scan_df" not in st.session_state:
        st.session_state["last_scan_df"] = pd.DataFrame()

    # Progress UI placeholders (will be created when scan starts)
    progress_bar = None
    progress_status = None

    if scan_btn:
        if not modes:
            st.warning("Sélectionne au moins un mode.")
        else:
            try:
                # create progress widgets
                progress_bar = st.progress(0)
                progress_status = st.empty()

                def _progress_cb(done: int, total: int) -> None:
                    try:
                        pct = int((done / max(1, total)) * 100)
                    except Exception:
                        pct = 0
                    try:
                        progress_bar.progress(min(max(pct, 0), 100))
                        progress_status.text(f"Enrichissement DexScreener: {done}/{total} ({pct}%)")
                    except Exception:
                        pass

                with st.spinner("Lancement du scan - cela peut prendre quelques secondes..."):
                    runners, debug = run_scan_for_modes(
                        selected_modes=modes,
                        top_n=int(top_n),
                        candidates_max=int(candidates_max),
                        anti_dead=bool(anti_dead),
                        include_boosts=bool(include_boosts),
                        include_profiles=bool(include_profiles),
                        include_cto=bool(include_cto),
                        include_ads=bool(include_ads),  # now drives inclusion of ads as candidates
                        include_orders=bool(include_orders),
                        unique_per_token=bool(unique_per_token),
                        trending_filters=bool(trending_filters),
                        trending_min_liquidity=float(trending_min_liq),
                        trending_min_vol1h=float(trending_min_vol1h),
                        trending_min_vol5m=float(trending_min_vol5m),
                        trending_min_netbuy5m=int(trending_min_netbuy5m),
                        verbose_debug=bool(verbose_debug),
                        pump_mode=bool(pump_mode),
                        sort_by_spike=bool(sort_by_spike),
                        progress_callback=_progress_cb,
                    )
                # Post-scan: apply blacklist to the displayed results
                blocked = set(
                    [str(x).strip() for x in (st.session_state.get("token_blacklist_entries") or []) if str(x).strip()]
                )
                removed = 0
                if blocked and runners:
                    before = len(runners)
                    runners = [r for r in runners if str(r.get("tokenAddress", "")).strip() not in blocked]
                    removed = before - len(runners)

                msg = f"Scan terminé - {len(runners)} résultat(s) trouvé(s)."
                if removed:
                    msg += f" ({removed} masqué(s) via blacklist)"
                st.success(msg)

                st.session_state["last_scan_debug"] = debug
                st.session_state["last_scan_df"] = pd.DataFrame(runners) if runners else pd.DataFrame()

                # Persist last scan timestamp only on successful scan
                cfg["last_scan_ts"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                save_config(cfg)

            except Exception as e:
                st.session_state["last_scan_debug"] = {"error": repr(e)}
                st.session_state["last_scan_df"] = pd.DataFrame()
                st.error(f"Erreur lors du scan: {e}")
            finally:
                try:
                    if progress_status:
                        progress_status.text("Traitement terminé.")
                except Exception:
                    pass

    # Display results / debug
    st.write("")
    st.caption(f"Dernier scan mémorisé: {cfg.get('last_scan_ts', '')}".strip() or "")

    df = st.session_state.get("last_scan_df")
    debug = st.session_state.get("last_scan_debug")

    # Apply blacklist to the currently displayed dataframe (even without re-scanning)
    try:
        blocked = set(
            [str(x).strip() for x in (st.session_state.get("token_blacklist_entries") or []) if str(x).strip()]
        )
        if isinstance(df, pd.DataFrame) and (not df.empty) and blocked and ("tokenAddress" in df.columns):
            df = df[~df["tokenAddress"].astype(str).str.strip().isin(blocked)].copy()
            st.session_state["last_scan_df"] = df
    except Exception:
        pass

    if debug:
        with st.expander("Debug scan (utile si ça sort vide)", expanded=(df is None or df.empty)):
            st.json(debug)
            if isinstance(debug, dict) and debug.get("why_filtered_list"):
                st.caption("Pourquoi filtré (extraits):")
                st.write(debug.get("why_filtered_list")[:200])

    if df is None or df.empty:
        st.info("Aucun token ne passe les filtres pour les modes sélectionnés.")
        return

    st.subheader(f"Top {min(len(df), int(top_n))} runners (tous modes confondus)")

    preferred = [
        "mode", "symbol", "name", "score",
        "spike_score", "tokenAddress", "source", "pairAddress",
        "priceUsd", "marketCap", "fdv", "liquidityUsd",
        "vol5m", "vol1h", "vol24h",
        "buys5m", "sells5m", "netBuy5m",
        "m5pct", "h1pct", "h6pct", "h24pct",
        "ageMin", "dexId",
        "boostAmount", "boostTotal", "boostType",
        "isCTO",
        "profileUrl", "profileLinksCount", "profileDescription",
        "urlDexscreener", "urlGMGN",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    # Apply persisted column visibility (if configured)
    vis = [c for c in (cfg.get("visible_columns") or []) if c in df.columns]
    if vis:
        ordered_vis = [c for c in df.columns if c in vis]
        df = df[ordered_vis]

    # Apply display row cap (persisted)
    try:
        cap = int(cfg.get("max_display_rows", 200))
    except Exception:
        cap = 200
    if cap > 0:
        df = df.head(cap)

    row_count = max(1, len(df))
    height = min(700, 60 + 35 * row_count)
    st.dataframe(df, width="stretch", height=height)

    st.write("")
    st.subheader("Actions de trading rapides (GMGN)")
    st.caption("Ces boutons n'envoient aucune transaction. Ils ouvrent simplement la page GMGN ou Dexscreener correspondante.")

    token_labels = []
    for _, row in df.iterrows():
        token_labels.append(f"{row.get('symbol','')} - {row.get('name','')} ({str(row.get('tokenAddress',''))[:6]}...)")

    idx = st.selectbox(
        "Choisis un token pour préparer un BUY / SELL (GMGN)",
        options=list(range(len(token_labels))),
        format_func=lambda i: token_labels[i] if i < len(token_labels) else str(i),
        key="token_selector",
    )
    chosen = df.iloc[int(idx)].to_dict()

    c1, c2, c3 = st.columns(3)
    gmgn_url = str(chosen.get("urlGMGN", "")) or "#"
    dexscreener_url = str(chosen.get("urlDexscreener", "")) or "#"
    with c1:
        st.text_input("CA token sélectionné", value=str(chosen.get("tokenAddress", "")), disabled=True)

        if st.button("Blacklister ce token", use_container_width=True, key="blacklist_selected_token"):
            ca = str(chosen.get("tokenAddress", "")).strip()
            if ca:
                bl = st.session_state.get("token_blacklist_entries", [])
                if ca not in bl:
                    bl.append(ca)
                    st.session_state["token_blacklist_entries"] = bl
                    st.session_state["token_blacklist_text"] = "\n".join(bl)
                    cfg["token_blacklist"] = bl
                    save_config(cfg)
                else:
                    st.info("Token déjà présent dans la blacklist.")
                # Remove it from current displayed dataframe
                try:
                    df_cur = st.session_state.get("last_scan_df")
                    if isinstance(df_cur, pd.DataFrame) and (not df_cur.empty) and ("tokenAddress" in df_cur.columns):
                        st.session_state["last_scan_df"] = df_cur[df_cur["tokenAddress"].astype(str).str.strip() != ca].copy()
                except Exception:
                    pass
                st.success("Token ajouté à la blacklist.")
            else:
                st.warning("Impossible de blacklister: tokenAddress vide.")

        _render_external_link("Ouvrir sur GMGN", gmgn_url)

    with c2:
        st.text_input("Pair address (si utile)", value=str(chosen.get("pairAddress", "")), disabled=True)
        st.caption("Le lien GMGN couvre BUY et SELL (pas de redémarrage Streamlit).")

    with c3:
        st.text_input("Dex (Dexscreener)", value=str(chosen.get("dexId", "")), disabled=True)
        _render_external_link("Voir sur Dexscreener", dexscreener_url)


if __name__ == "__main__":
    main()
