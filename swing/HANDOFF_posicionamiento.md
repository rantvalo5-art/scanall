# HANDOFF — posicionamiento de futuros (`tt_pos`): estresarlo antes de operarlo

> **Solo `swing/`.** El day trader de la raíz no se toca. Leer primero `swing/CLAUDE.md`.
> Escrito 2026-08-23, al cierre de la sesión que hizo el plan de trading en la alerta y
> después desarmó de dónde sale la pérdida.
> Rama: **`swing/plan-trading`**, 8 commits sobre `origin/main`.

---

## 0. Lo que esta sesión dejó cerrado — no rehacer

**Plan de trading en la alerta (`HANDOFF_plan_trading.md`): TERMINADO.** Fases 0, 1 y 2.
El bloqueante del `STOP_PCT` no era tal (la config validada es 0.10, activada en
`550dd7b`; el `0.0` era el default de `_EX.get()` y el comentario estaba viejo). Fase 0
midió el "objetivo" contra el dardo pareado y **no cruzó** (−8,4pp con `major_max`,
−6,4pp con `one_h_resist`), así que se muestra `resistencia cercana` sin R:B, y sólo en
COILING/PREBREAK. Sanidad global verificada: 255/255 alertas idénticas con y sin la rama.

---

## 1. Contexto crítico — medido, no re-derivar

Todo sobre **1.028-1.065 alertas BEST reales** de `archivo_outcomes/screener_outcomes.csv`
(3.311 filas, 31-may → 22-ago 2026), horizonte 7d, neto de 0,30% round-trip.
El backtest **no sirve** para esto: genera 1/5 a 1/3 de las alertas reales y da vuelta la
media. Se usan las alertas del archivo y sólo se **recomputa** lo que haga falta.

### El diagnóstico que ordena todo (`atribucion.py`)

`retorno = BETA + UNIVERSO + HABILIDAD` (las capas suman exacto):

| capa | media | mediana |
|---|---|---|
| ALERTA | +0,06% | **−3,90%** |
| BETA (BTC misma ventana) | −0,53% | −0,50% |
| UNIVERSO (dardo − BTC) | −0,39% | −1,03% |
| HABILIDAD (alerta − dardo) | +0,98% | **−2,20%** |

**El bot elige bien la moneda y mal el momento.** Y las señales tienen enfermedades
distintas: BREAKOUT universo +0,01% / habilidad +3,90% (pura cola); HOLD universo +1,05%
/ habilidad +0,02%; COILING −1,72% / +0,25%; PREBREAK −1,49% / +0,38%.

### La firma que explica los ~450 fracasos previos

**Toda palanca probada intercambia MEDIANA contra COLA y deja la media en ~0.** Demorar
la entrada (regla de ejecución pura, sin re-seleccionar) sube la mediana +2,47pp a 120h y
hunde la media de +0,10% a −0,85%. Comprar el que subió y corrigió: dosis-respuesta
monótona hasta −15,30%. Excluir monedas baratas: mejora la mediana, regala la cola.
Shortear: mediana +2,06% con cualquier modelo de relleno, media 0.

Eso **no** es un screener roto: es la firma de no haber encontrado drift condicional.
Por eso conviven el techo oráculo (×966, la detección está bien) y la fuga del harvest.

### Trampas que ya costaron tiempo — heredarlas

1. **El motor mira la última vela CERRADA**, no la que contiene `alerted_at`
   (`candle_status` es "closed" en las 3.311 filas). Recomputar estructura con el offset
   ingenuo reproduce el `ref_price` exportado al **64%**; con el −1 barra, al **100%**.
   Está en `fase0_plan.engine_bar_index()`. Cualquier script sobre `screener_outcomes`
   lo necesita.
2. **Lookahead en las métricas horarias.** El valor de cada hora es el **último** dato de
   esa hora, así que la hora que contiene la alerta trae observaciones posteriores a
   ella. Hay que tomar la última hora *completamente cerrada*
   (`searchsorted(t, ts_ms - 1h)`). Verificado: acá no cambió nada, pero en otra variable
   puede ser todo el efecto.
3. **El modelo de relleno del stop es donde se esconde el autoengaño.** Relleno exacto vs
   cierre de la vela vs máximo de la vela cambió el corto de +1,16% a +0,08% y el peor
   caso de −20,3% a −89,1%. **Un stop no acota un salto.**
4. **Concentración en los DOS ejes, siempre.** Top-3 símbolos *y* top-3 semanas. La
   premisa de "COILING pesca mal" murió en semanas (−1,72% → +1,69%); todo promedio
   positivo del repo termina siendo TUTUSDT o BANKUSDT.
5. **El gate va ANTES de la búsqueda.** En esta sesión propuse una línea entera mirando
   un estimador puntual sin correrle la concentración. Se perdió una corrida.

---

## 2. Lo único vivo: `tt_pos`

`swing/posicionamiento.py`. Ratio long/short **por posición** de los top traders de
Binance Futures — de qué lado está el dinero grande.

**Fuente:** `https://data.binance.vision/data/futures/um/daily/metrics/{PERP}/{PERP}-metrics-{YYYY-MM-DD}.zip`
Un zip por símbolo-día, 288 filas de 5 min, **desde 2020**. No tiene el muro de 30 días
de la API REST. Los perps de precio chico se re-escalan (`1000PEPEUSDT`), y
`resolver_perp()` prueba las tres variantes. Cache en `swing/.metrics_cache/`
(gitignored): 251 símbolos × 94 días ya bajados.

**El hallazgo.** Alertas BEST con `tt_pos < 1,28` (umbral = p20 de la 1ª mitad, fijado
a priori) — **17,4% del feed**:

| chequeo | resultado |
|---|---|
| retorno 7d | **−9,30%** (mediana −6,36%) |
| IC95 (bootstrap por semana) | [−14,96 , −5,09] — no cruza |
| sin top-3 símbolos | −10,53% — **empeora** |
| sin top-3 semanas | −10,52% — **empeora** |
| vs dardo pareado | **−9,01pp** [−16,40 , −4,03] |
| 1ª mitad → 2ª mitad OOS | −9,80% → **−8,62%** |
| corr con log10(precio) | +0,026 — no es otro hallazgo disfrazado |

Seis filtros —BH sobre 10 variables, IC, las dos concentraciones, dardo, OOS— y los pasa
todos. Es el primero del swing. El peor grupo es **HOLD** (−16,85%, mediana −14,05%).

**Lo que NO establece:**
- El resto **no** queda rentable: media +2,20%, **mediana −3,46%**, IC95 [−3,92 , +11,60].
  Se deja de pagar un impuesto medible; no aparece una ventaja.
- **No se cobra en corto.** Sobre el mismo subconjunto, stop +20%: relleno exacto +2,88%,
  al cierre +2,30%, al máximo +0,93%; y OOS +0,76% → −1,21%.

**Descartados en la misma corrida (BH 10%):** `oi`, `oi_usd`, `ls_cuentas`, `tt_cuentas`,
`taker`, sus z de 14d, y "tener perp" (96,6% de las alertas ya lo tiene).

---

## 3. La decisión ya tomada

**Estresar la familia antes de deployar.** El hallazgo sale de 1.028 alertas de 12
semanas y una sola variable a la vez. La fuente tiene datos **desde 2020** y se usaron
tres meses. Deployar ahora repetiría el error del vender-volatilidad: un número real
medido en la ventana equivocada.

---

## 4. Fase 1 — profundidad histórica. **Es la que decide todo.**

**La pregunta:** ¿el efecto de `tt_pos` aguanta 2 años, o era esta ventana?

**El problema a resolver primero:** `screener_outcomes` sólo tiene 3 meses de alertas
reales, y el replay del backtest **no las reproduce** (memoria:
`project-replay-no-reemplaza-vivo`). O sea que no hay alertas reales de 2024-2025.

**Salida:** medir el efecto **sin alertas**, sobre el universo. Si `tt_pos` bajo predice
retorno negativo a 7d en una grilla diaria de símbolos × fechas (2022-2026), el mecanismo
existe con independencia del screener, y entonces la versión sobre alertas es un caso
particular. Si no aparece en el panel largo, lo de acá fue la ventana.

- Panel: los ~250 símbolos con perp, grilla diaria, forward 7d neto, 2022-2026.
- Benchmark: **el mismo símbolo a fechas al azar** (dardo pareado, igual que siempre) y
  el quintil superior de `tt_pos`.
- Bootstrap por **semana**, concentración por símbolo y por semana, y **por año**.

**Regla de parada (preescrita, no aflojar después de ver el número):** el margen del
quintil bajo contra el dardo tiene que ser negativo con IC95 sin cero **en al menos 3 de
los 4 años por separado**. Si vive en 1 o 2 años, es régimen y se archiva.

**Costo:** la descarga es lo caro — ~250 símbolos × ~1.500 días ≈ 375k zips. Bajar por
tandas; `frame_simbolo()` cachea por símbolo y es **reanudable**, y el harness mata a los
10 min (ya pasó: se relanza y sigue). Presupuestar 2-3 sesiones sólo de descarga.

---

## 5. Fase 2 — sólo si Fase 1 cruza

1. **Horizontes.** Se midió 7d. Probar 24h / 3d / 14d / 21d. El payoff del swing madura
   >7d, así que 14/21d importa.
2. **Cruces.** `tt_pos` bajo **+ OI subiendo** es la hipótesis con mecanismo: multitud
   cargada y grande del otro lado = cascada de liquidaciones. `oi` solo no dio; el cruce
   no se probó.
3. **Por señal.** HOLD es el peor grupo (−16,85%). Puede ser que la regla sea
   *HOLD + tt_pos bajo* y no `tt_pos` a secas. Ojo con n: HOLD dentro del grupo son 51.
4. **Continuo, no quintil.** El umbral 1,28 es un p20 de esta muestra. Ver si el efecto
   es monótono en el nivel crudo, que es lo que haría deployable un umbral fijo.

---

## 6. Fase 3 — a vivo, sólo si Fase 1 y 2 cruzan

**Esto NO es presentación: cambia qué se alerta.** Distinto del plan de trading, que era
inerte por construcción.

- Endpoint en vivo: `/futures/data/topLongShortPositionRatio` (la REST con 30d alcanza
  para vivo; los dumps diarios son para backtest).
- El swing **no tiene sección `derivatives` a propósito** (`swing/CLAUDE.md`). Agregarla
  es una decisión de identidad del fork: confirmarla con el usuario.
- Implementar como **filtro de bucket**, no de detección: la alerta se sigue calculando y
  baja de BEST a WATCH. Así el efecto es medible y reversible con un knob.
- `config.json` → sección nueva con `ENABLED: false` por default, para que la rama sea
  segura de mergear (mismo patrón que `exit_mgmt` en `17e6d03`).
- Preregistrar la métrica de éxito **antes** de encender, y dejar correr ≥8 semanas.

---

## 7. Lo que NO hay que hacer

- **No tunear scoring, buckets ni exits.** Ese pozo está medido: mueve mediana↔cola y
  conserva la media. ~450 hipótesis.
- **No buscar más features de precio/volumen.** Misma razón.
- **No shortear.** Medido dos veces (genérico y sobre el subconjunto bueno): muere en el
  modelo de relleno y OOS queda plano. La mediana es real y no se cosecha porque un stop
  no acota un salto.
- **No deployar `tt_pos` sin Fase 1.** Es la tentación obvia y es exactamente el error de
  vender-volatilidad.
- **No re-etiquetar `resistencia cercana` como `objetivo`** sin volver a correr Fase 0.

---

## 8. Herramientas — reusar, no reescribir

| script | qué hace |
|---|---|
| `fase0_plan.py` | loader de klines con cache, `engine_bar_index`, `boot_ci` (bootstrap por semana), `drop_top` (concentración). **Lo importan todos los demás.** |
| `dip_previo.py` | `fwd()` (retorno neto), `bh()` (Benjamini-Hochberg), `p_boot()` |
| `pileta.py` | `p_boot_corr()` (Spearman con p por bootstrap de semanas), loader con volumen |
| `atribucion.py` | descomposición beta/universo/habilidad |
| `corto.py` | corto con los 3 modelos de relleno |
| `posicionamiento.py` | `frame_simbolo()` (métricas de futuros, cacheado y reanudable), Fases A y B |
| `test_plan.py` | 40 chequeos de `_build_plan` / `_plan_lines`, sin red |

Caches (gitignored): `swing/.fase0_cache/` (klines 1h, OHLC y con volumen),
`swing/.metrics_cache/` (métricas de futuros, 251 símbolos × 94 días).

---

## 9. Estado de la rama

`swing/plan-trading`, sacada de `origin/main` (**ojo:** el `main` local está atrasado; le
faltan `53d0112`, `1c0a00f`, `be4cef3`, y los últimos dos siguen sólo en
`banco/primer-toque`). Sin pushear.

```
41cb2a2  tt_pos — lo primero que cruza los seis filtros. Es una regla de EVITAR
f38a659  la pileta y el corto — la mediana es real y estable, y no se puede cobrar
19cccbc  de donde sale la perdida — beta / universo / habilidad, y el eje mediana-cola
6171228  "comprar el que subio y ya corrigio" es la peor celda, con dosis-respuesta
036c964  Fase 2 — render del plan en format_alert()
6d55538  Fase 1 — _build_plan() en el motor compartido
4b04545  Fase 0 — el objetivo del plan NO cruza el dardo pareado
d195107  el stop duro validado es 10% — el comentario del tracker mentia
```

El working tree del usuario quedó intacto en `banco/primer-toque`, con el fix del token
de Telegram sin commitear (ya está aparte en `sec/telegram-token-leak`, `ca106ac`).
`swing/screener.py` difiere entre las dos ramas, así que un `git checkout` directo choca:
la rama vive en un worktree aparte.
