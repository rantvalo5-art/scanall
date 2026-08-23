# HANDOFF — posicionamiento de futuros (`tt_pos`): **tumbado por la Fase 1**

> **Solo `swing/`.** El day trader de la raíz no se toca. Leer primero `swing/CLAUDE.md`.
> Escrito 2026-08-23 al cerrar el plan de trading; **actualizado el mismo día** con el
> resultado de la Fase 1, que dio negativo.
> Rama: **`swing/plan-trading`**, 11 commits sobre `origin/main`.
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

## 2. Qué era `tt_pos` y qué se midió sobre alertas

> Todo lo de esta sección **sigue siendo cierto como medición** — y sigue sin
> servir, porque vive en 12 semanas. El §3 explica por qué.

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

## 3. Fase 1 — **CORRIDA, y NO CRUZA**

`swing/panel_tt.py` (tres pasos reanudables) + `swing/diag_panel_tt.py` +
`swing/test_panel.py` (14 chequeos sin red). Resultados en `panel_tt.txt` /
`panel_tt.json` / `diag_panel_tt.txt`.

**El panel.** 154.476 filas · 804 perps · grilla cada 3 días · 2023-01 → 2026-08 ·
forward 7d neto · contra dardo pareado del mismo símbolo a ±30 días saltando ±7.
Universo tomado del **listado S3 del bucket de Binance**, no del feed de hoy: 847 perps
USDT históricos, **98 de ellos ya delisteados**. El precio sale de las klines del propio
perp, que es el único que existe para un delisteado.

**El veredicto: 0 de 4 bloques.** Y no queda en nada — **el signo se invierte**:

| bloque | margen q1 (`tt_pos` bajo) | q5 (alto) | q1 − q5 |
|---|---|---|---|
| 2023-01 → 2023-11 | +0,65pp | −4,14pp | **+4,79pp** |
| 2023-11 → 2024-10 | +1,51pp | −2,41pp | **+3,93pp** |
| 2024-10 → 2025-09 | +0,94pp | −0,87pp | **+1,82pp** |
| 2025-09 → 2026-08 | −0,41pp | −0,03pp | −0,38pp ← contiene la ventana del hallazgo |

Tres años enteros dicen lo contrario de la hipótesis, y el único bloque con el signo del
hallazgo es el que solapa may-ago 2026. Ningún margen tiene el IC95 fuera del cero, en
ninguna dirección. **Eso es la definición de régimen, y la regla preescrita decía
archivar.**

**Por qué no reproduce el −9,30%** (`diag_panel_tt.py`):
- En la ventana del hallazgo el panel **sí** ve el signo correcto, pero −0,59pp con IC
  [−2,67 , +1,49]: quince veces más chico y dentro del ruido.
- Restringido a los 285 símbolos que el swing alertó en BEST: **+0,68pp** sobre
  2023-2026. No era la población de monedas.
- El escalón crudo (q1 +0,44% → q5 −1,73%) es **mitad moneda**: el dardo pareado baja con
  él (−0,03% → −0,97%).

### La traducción de la regla, y por qué hubo que hacerla

El handoff pedía "3 de los 4 años, 2022-2025". **2022 no existe en la fuente**:
`sum_toptrader_long_short_ratio` viene *presente pero vacía* casi todo el año (BTCUSDT
87% NaN, ETHUSDT igual; verificado mes a mes). La columna está en el header, así que un
`if c in df.columns` no lo detecta. Tramos limpios: 2020-09→2021-12 y 2023-01→hoy.

Se conservó el 3-de-4 partiendo el tramo limpio en cuatro bloques contiguos de igual
duración, **fijado antes de mirar ningún efecto** (está en el docstring de `panel_tt.py`
y en el commit `51564b3`, anterior a los resultados). Los años calendario se imprimen
como control y dan lo mismo.

### Lo único que la Fase 1 no puede testear

La interacción **alerta × `tt_pos`**. No hay alertas históricas y el replay no las
reproduce ([[project-replay-no-reemplaza-vivo]]). Es el resquicio honesto, y no hay forma
barata de cerrarlo: haría falta ≥6 meses de feed en vivo con `tt_pos` guardado.

---

## 4. Lo que dejó de estar en pie

Las Fases 2 y 3 del plan original (horizontes, cruce con OI, por señal, continuo vs
quintil; y después el deploy como filtro de bucket) **estaban condicionadas a que la
Fase 1 cruzara**. No cruzó. No hay que correrlas: buscar un horizonte o un cruce que sí
dé, sobre un efecto que ya se sabe que vive en un solo bloque, es exactamente el
autoengaño que el handoff venía evitando.

**La tentación específica a no seguir:** el bloque B4 tiene el signo "correcto". Mirar
sólo B4 y decir "en el régimen actual funciona" es elegir la ventana después de ver los
datos.

---

## 5. La sonda que quedó suelta — n chico, NO es un hallazgo

Cayó de costado en `diag_panel_tt.py`. Las 263 filas del panel que coinciden con un
día-símbolo **con alerta BEST real** dan margen **+10,01pp**, IC [+4,36 , +18,23] para el
grupo no-bajo. La diferencia con la medición sobre alertas es **dónde se entra**: acá la
entrada es 00:00 UTC del día de la alerta, no la vela de la alerta.

Si eso aguanta con más n, apunta a lo mismo que [[project-swing-entrada-breakout]] y
[[project-swing-mediana-vs-cola]] — el bot elige bien la moneda y mal el momento — y la
palanca sería **demorar hasta el corte del día**.

**Antes de emocionarse:** n=263 sale de que la grilla es cada 3 días; los dardos salen de
±30 días alrededor de un tramo de momentum; y "demorar la entrada" ya fue medido sobre
alertas y **subía la mediana hundiendo la media** (§1). O sea que el prior está en contra.
Si se prueba, se prueba con la grilla diaria (`--stride 1`, lo bajado no se tira) y con
regla de parada escrita antes.

---

## 6. Lo que NO hay que hacer

- **No tunear scoring, buckets ni exits.** Ese pozo está medido: mueve mediana↔cola y
  conserva la media. ~450 hipótesis.
- **No buscar más features de precio/volumen.** Misma razón.
- **No shortear.** Medido dos veces (genérico y sobre el subconjunto bueno): muere en el
  modelo de relleno y OOS queda plano. La mediana es real y no se cosecha porque un stop
  no acota un salto.
- **No deployar `tt_pos`, punto.** La Fase 1 lo tumbó (§3). Y no re-correrlo con otro
  horizonte, otro umbral o otro cruce a ver si alguno da: el efecto vive en un bloque
  de cuatro y en tres tiene el signo al revés.
- **No mirar sólo el bloque 2025-09 → 2026-08** porque ahí el signo cierra. Es elegir
  la ventana después de ver los datos.
- **No re-etiquetar `resistencia cercana` como `objetivo`** sin volver a correr Fase 0.

---

## 7. Herramientas — reusar, no reescribir

| script | qué hace |
|---|---|
| `fase0_plan.py` | loader de klines con cache, `engine_bar_index`, `boot_ci` (bootstrap por semana), `drop_top` (concentración). **Lo importan todos los demás.** |
| `dip_previo.py` | `fwd()` (retorno neto), `bh()` (Benjamini-Hochberg), `p_boot()` |
| `pileta.py` | `p_boot_corr()` (Spearman con p por bootstrap de semanas), loader con volumen |
| `atribucion.py` | descomposición beta/universo/habilidad |
| `corto.py` | corto con los 3 modelos de relleno |
| `posicionamiento.py` | `frame_simbolo()` (métricas de futuros, cacheado y reanudable), Fases A y B |
| `test_plan.py` | 40 chequeos de `_build_plan` / `_plan_lines`, sin red |
| `panel_tt.py` | **Fase 1.** `universo` (listado S3, incluye delisteados) / `bajar` (metrics diarias + klines por REST de futuros, reanudable por símbolo-año) / `panel` |
| `diag_panel_tt.py` | por qué el panel no reproduce el hallazgo sobre alertas |
| `test_panel.py` | 14 chequeos del panel, sin red |

Caches (gitignored): `swing/.fase0_cache/` (klines 1h, OHLC y con volumen),
`swing/.metrics_cache/` (métricas de futuros, 251 símbolos × 94 días),
`swing/.panel_cache/` (**167 MB**: 2.196 símbolo-año de métricas + 847 series de
klines diarias + el panel armado). Densificar a grilla diaria es sólo `--stride 1`:
no tira nada de lo bajado, y cuesta ~2h más de descarga.

Dos cosas de operación que costaron tiempo: **96 workers rinden menos que 48**
(throttling; 48 req/s es el techo del bucket), y las klines conviene bajarlas por la
**REST de futuros** — 2 requests por símbolo en vez de ~70 zips, y sirve igual para
los delisteados (verificado al centavo contra los dumps).

---

## 8. Estado de la rama

`swing/plan-trading`, sacada de `origin/main` (**ojo:** el `main` local está atrasado; le
faltan `53d0112`, `1c0a00f`, `be4cef3`, y los últimos dos siguen sólo en
`banco/primer-toque`). Sin pushear. Vive en un worktree aparte porque
`swing/screener.py` difiere entre las dos ramas y un `git checkout` directo choca.

```
<<HASH4>>  handoff: la Fase 1 tumbo tt_pos
<<HASH3>>  Fase 1 — tt_pos NO cruza. Se da vuelta el signo fuera de su ventana
<<HASH2>>  chequeos del panel — 14, sin red
<<HASH1>>  Fase 1 — panel historico de tt_pos sin alertas
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
