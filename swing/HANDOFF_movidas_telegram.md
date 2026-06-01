# HANDOFF — Pescar movidas tipo HOMEUSDT: triaje de alertas (no detección nueva)

> Handoff para retomar en una conversación nueva. Implementa mejoras de **triaje** del push de Telegram.
> NO toca señales ni `config.json` salvo donde se indique explícitamente.

## Context — por qué este handoff

El usuario quiere cazar subidas sostenidas tipo **HOMEUSDT** (+148% 20-may→01-jun-2026). Una investigación
completa (Fase A/B, ver memoria `project_swing_squeeze_release_faseB`) concluyó:

- **La detección NO es el cuello de botella.** El sistema YA pescó HOME temprano (COILING, 6-may, BEST→Telegram, a
  ~1/3 del precio final). De los movers +50%/14d, las señales actuales (BEST) cazan **65.7%**.
- **No hay señal nueva tuneable.** Squeeze BB+OBV+EMA = artefacto de volatilidad (lift<1.0). Filtro momentum
  (precio↑3d+OBV+%B) llena un hueco de recall real (+30pp) pero SOLO como **dragnet** (29% de barras, ~55/día,
  ~90% falsos) → inservible para un push precision-first.
- **El problema real es TRIAJE:** demasiadas alertas de baja precisión; no se distingue cuál es el HOME.

**Objetivo:** mejorar la capacidad de *distinguir* y *seguir* los movers reales entre las alertas que ya se
generan, respetando la prioridad del usuario (`lateness > precision > F-score > recall`, memoria
`project_scoring_priorities`). Ninguna idea promete adelantar a HOME más de lo que ya lo agarró el sistema.

## Guardrails empíricos (no re-derivar, no violar)

- `atr_pct_1d` es **el único separador robusto** que replicó OOS (ya gateado en COILING/PREBREAK). Es la base de la
  convicción. (`project_prebreak_atr_gate`, `project_swing_coverage`...)
- **NO suprimir repeticiones.** El experimento COILING-suppression bajó recall (`project_swing_coil_suppress_closed`).
  La repetición de la misma moneda es señal de convicción, hay que **elevarla**, no deduplicarla.
- Solo **BEST** va a Telegram (`project_swing_screener`). Cualquier cosa de menor precisión va al dashboard, no al push.
- El payoff madura >7d; **RIDING** es la mejor para hold largo (`project_swing_horizon_14d21d`).
- El filtro momentum NO va a Telegram como alerta (dragnet). Su único hogar honesto es un tier watchlist no-pusheado.

---

## Estado (2026-06-01)

- **Idea 1 — HECHA.** Badge `atr_pct_1d` en el header de cada alerta (` atr6.5`, `🔥atr9.1` si ≥ `ATR_BADGE_HIGH`,
  default 8.0). Campo inyectado centralmente en `backtest.classify` (loop ~1583, todas las señales) y leído en
  `screener.format_alert`. Verificado con smoke test (HOME en 6.5 = gated-pero-no-🔥, la historia correcta).
- **Idea 2 — HECHA (multi-día).** Decisión de scope: `counts_history` (24h) está acoplado a LATE_REPEAT (scoring),
  no se puede ensanchar. La racha se computa sobre **`screener_outcomes`** (retención 90d, trae `signal_type`) vía
  `fetch_symbol_streak(symbol)`, ventana `STREAK_LOOKBACK_DAYS` (default 14d). Línea de convicción en el mensaje:
  cross-tipo → `🔁 convicción: COILING→HOLD→RIDING (9× / 13d)`; mismo tipo → `🔁 reincide: RIDING 4× / 3d`. Query
  acotada a 1 símbolo dentro de `send_immediate` (≤5/run). Verificado con smoke test del arco HOME.
- **Pendiente de validar en vivo:** correr un scan real (env `TELEGRAM_TOKEN`/`CHAT_ID`/`SUPABASE_KEY`) y confirmar
  que `fetch_symbol_streak` lee bien `screener_outcomes` y que el badge/racha aparecen en un BEST real. Sin commit aún.
- **Idea 4 — HECHA.** Tag `🪢 candidata a hold — correa larga (seguir 14/21d)` para RIDING/HOLD reincidentes
  (`prev>0` o racha ≥2). Solo presentación; commit `e989c07` en rama `swing/telegram-triaje-badge-racha`.
- **Idea 3 — HECHA (rama aparte `swing/watchlist-momentum`, stacked sobre la de Telegram).** Watchlist momentum
  F/H que NO se pinguea. `_watch_flags(df1d)` reproduce exacto `_faseB_recall.daily_flags` (validado contra
  referencia); se computa en `analyze()` para el 1d reusando el df ya fetcheado (sin red extra) y se stashea en el
  feat. `main()` recolecta los hits F sobre TODO el universo (antes del early-return) y los escribe vía
  `insert_watchlist()` a la tabla Supabase **`swing_watchlist`**. Path 100% separado: no toca
  candidates/classify/insert_outcomes/Telegram. Knobs en sección `watchlist` (ENABLED/OBV_LOOKBACK/RETENTION_DAYS,
  defaults en código, no requiere editar config.json).
  - **ACCIÓN REQUERIDA antes del primer run:** crear la tabla en Supabase (si no, `insert_watchlist` falla en
    silencio y loguea). SQL:
    ```sql
    create table swing_watchlist (
      id bigserial primary key,
      scan_date date not null,
      scanned_at timestamptz not null,
      symbol text not null,
      flag_f boolean not null default true,
      flag_h boolean not null default false,
      price double precision,
      atr_pct_1d double precision,
      pctb_1d double precision,
      unique (symbol, scan_date)   -- upsert: 1 fila por símbolo/día (F/H es flag diaria)
    );
    create index on swing_watchlist (scanned_at);
    create index on swing_watchlist (symbol, scan_date desc);
    ```
    Lectura del watchlist (más reciente por símbolo): `select * from swing_watchlist
    order by scan_date desc, flag_h desc, atr_pct_1d desc`. Primera aparición de X:
    `select symbol, min(scan_date) from swing_watchlist group by symbol`.
  - **Display:** pendiente (viewer HTML mínimo o query Supabase). El sink ya persiste; el dashboard es opcional.
  - **Verificación en vivo:** correr el scan y confirmar que HOME cae en la watchlist el 11-15 may y que NO dispara
    `send_telegram`.

## Las 4 ideas (todas, ordenadas por relación valor/riesgo)

### Idea 1 ⭐ — Badge de convicción por `atr_pct_1d` en el mensaje
**Qué:** en cada alerta de Telegram, mostrar la banda de atr como tier visual (ej. 🔥 atr>8 "alta convicción" /
normal). Opcional: usar la banda para subir el orden en `dedupe_and_rank`.
**Por qué:** atr es lo único que separó movers de no-movers de forma robusta — exponerlo ayuda a confiar en las
coils correctas (HOME estaba en atr~6.5).
**Anclajes:** `screener.py:format_alert` (415) → agregar al `header` (433-437). Confirmar/exponer `atr_pct_1d` como
campo top-level del alert (vive en `tf_1d['atr_pct']`; en `backtest.classify` ya se guarda como `atr_pct_1d` en el
breakdown de PREBREAK — verificar que esté en el dict del alert, si no agregarlo en `classify`). Ranking opcional:
`dedupe_and_rank` (461). **Costo:** muy bajo. **Riesgo:** bajo (cosmético + ranking).

### Idea 2 ⭐ — Repetición = convicción (racha cross-señal)
**Qué:** contador/racha en el mensaje cuando la MISMA moneda se ilumina repetidamente (en días y/o señales
distintas). HOME hizo COILING(6-may)→HOLD(19-may)→RIDING(20-may). Hoy `hist_tag` solo cuenta repeticiones del mismo
`history_tf`; extenderlo a **suma cross-tipo por símbolo**.
**Por qué:** la repetición es la huella del mover sostenido vs el spike de un día. Guardrail: elevar, NO suprimir.
**Anclajes:** `screener.py:format_alert` (416-423, el bloque `hist_tag`). `counts_history` está keyed por
`(symbol, history_tf)` → sumar sobre todos los `history_tf` del símbolo da el total. Fuente: `fetch_history`
(~555) + tabla `screener_history` (record en ~149). **Costo:** bajo (datos ya existen). **Riesgo:** bajo.

### Idea 3 — Dos canales: push preciso + watchlist de caza (no-pusheado)
**Qué:** Telegram sigue precision-first (BEST). Agregar un tier **"watchlist momentum"** en el dashboard con el
dragnet F/H (`swing/_faseB_recall.py` define F=precio↑3d+OBV≥0, H=+%B>.8), que **NO se pinguea** pero se puede
escanear cuando se caza activamente. HOME aparecería 11-15 may.
**Por qué:** captura el +30pp de recall sin ensuciar el push; el usuario opta por el ruido cuando quiere.
**Anclajes:** escribir un campo/tabla aparte (NO `send_telegram`); display en `index.html` / mecanismo
`dump_outcomes.py`+`outcomes.html`. El cómputo de F/H es daily-only (reusar la lógica de `_faseB_recall.daily_flags`).
**Costo:** medio (nuevo tier de dashboard). **Riesgo:** bajo si jamás se pushea.

### Idea 4 — Marcar "tendencia sostenida" y darle correa larga
**Qué:** etiquetar RIDING/HOLD repetidos como "candidata a hold" y seguirlas a 14/21d. Mayormente presentación;
se solapa con Idea 2.
**Por qué:** el payoff madura >7d y RIDING rinde mejor a 21d (+30.8%). El valor está en aguantar, no en adelantar.
**Anclajes:** el tracker `update_outcomes.py` ya llena 14d/21d (memoria `project_swing_horizon_14d21d`); agregar el
tag en `format_alert` para RIDING/HOLD con `prev>0`. **Costo:** bajo. **Riesgo:** bajo.

## Recomendación de secuencia
1. **Idea 1 + Idea 2** juntas (badge atr + racha cross-señal) — baratas, bajo riesgo, atacan el triaje directamente.
2. **Idea 3** si se quiere recuperar el recall perdido sin spam (más trabajo, dashboard).
3. **Idea 4** como pulido sobre la 2.

## Verificación
- Idea 1/2/4: correr `screener.py` en seco (un scan local) con env `TELEGRAM_TOKEN`/`CHAT_ID`/`SUPABASE_KEY`;
  revisar el texto formateado de un BEST sin enviar (o enviar a un chat de prueba). Confirmar que el badge atr y la
  racha aparecen y son correctos contra `counts_history`.
- Idea 3: generar el watchlist sobre un día histórico (reusar `_faseB_recall.py`) y confirmar que HOME cae en él
  el 11-15 may, y que NO dispara `send_telegram`.
- Sanidad global: `screener.py` y `backtest.py` comparten semántica — si se toca el dict del alert en `classify`,
  reflejarlo en ambos (CLAUDE.md).

## Artefactos de la investigación (para retomar)
Memoria: `project_swing_squeeze_release_faseB` (veredicto completo). Scripts diagnóstico en `swing/`:
`_home_gate.py`, `_home_classify.py`, `_home_probe.py`, `_faseB_sweep.py` (separabilidad+survivorship),
`_faseB_recall.py` (recall-gap + def de F/H). Caches temporales: `.faseB_cache`, `.audit_cache`, `.backtest_cache`.
