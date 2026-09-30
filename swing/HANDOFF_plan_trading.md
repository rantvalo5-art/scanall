# HANDOFF — Plan de trading en la alerta (entrada / invalidación / objetivo / R:B) — SWING

> **Solo `swing/`.** El day trader de la raíz no se toca. Leer primero `swing/CLAUDE.md`
> (identidad del fork, reparto de tablas Supabase, reglas de rama).
> Escrito 2026-08-22 al cierre de la sesión que midió la banda ATR y cerró el OI shock.

---

## 0. Decisión ya tomada — no re-litigar

El usuario **decidió construir esto sabiendo** que el sistema sobre el que se monta no
tiene ventaja medida. Se le presentó la objeción con números (sección 1) y la reafirmó.

**No volver a plantearla.** Lo que sí hay que sostener son los guardrails de honestidad
del diseño: etiquetas correctas, nada de números inventados, Fase 0 antes de mostrar.
La objeción era sobre *cómo se etiqueta*, no sobre *si se hace*.

---

## 1. Contexto crítico que esta conversación no tiene

Todo esto se midió el 2026-08-22. **No re-derivarlo.** Está en la memoria del proyecto
(`project-swing-*`) y en `PROXIMOS_CHEQUEOS.md`.

### El screener de swing quedó cerrado como línea de investigación

Sobre 2.084 alertas live (10-jul → 22-ago, `archivo_outcomes/screener_outcomes.csv`):

| medición | resultado |
|---|---|
| BEST vs resto del feed, pareado por día (mover 24h) | **+17,8pp** [+11,4 , +24,3], 72% de días |
| BEST vs resto del feed, pareado por día (PnL con TP/SL) | **+0,23%** [−0,62 , +1,08], 50% de días |
| en el tramo alcista (17-ago+) | **−1,86%** [−3,70 , −0,01], 17% de días a favor |
| BEST a 7d | mediana **−3,61%**, 33,7% positivas; la media entera es TUTUSDT |
| barrido de 40 combinaciones TP/SL | **ninguna** con IC95 inferior sobre cero |

**Traducción:** la banda ATR **detecta** volatilidad (concentra movers 3-5×, confirmado
tres veces) y **no la convierte** en dinero. No hay contenido direccional.

### El dato que más importa para este handoff

Resuelto con klines de 5m, ambigüedad 0,00%, sobre las 2.084 alertas:

| bucket | tocó primero el target | tocó primero el stop | ratio |
|---|---|---|---|
| BEST | 31,5% | **35,1%** | 0,90 |
| WATCH | 13,5% | 13,1% | 1,03 |

La banda sube las dos tasas y sube **un poco más la del stop**. Esto es exactamente lo
que Fase 0 va a volver a encontrar con otros niveles y otro horizonte, así que conviene
esperarlo.

### El defecto de entrada, medido

`project-swing-entrada-breakout`: BREAKOUT **compra el techo de la vela de extensión**,
**−4,2pp contra azar a 48h**. Demorar la entrada repara; comprar el dip empeora.

Esto decide la pregunta 3 de las decisiones abiertas (sección 4).

### El estándar de evidencia del repo

**Dardo pareado.** Ningún número absoluto vale sin benchmark: misma moneda a horas al
azar, o el resto del feed el mismo día. Es el filtro que mató ~450 hipótesis. Un 45% de
tasa de toque se ve bien y no dice nada sin el pareado al lado.

Y desde hoy hay un segundo eje: **la trampa de concentración se chequea también en el
tiempo, no sólo por símbolo.** El OI shock aguantaba perfecto sacando los top-3 símbolos
y murió en semanas (46% de semanas arriba del umbral, p 0,9210).

---

## 2. Por qué este handoff existe

La alerta responde **"qué"** (símbolo, señal, score, atr, racha) pero no **"a qué precio"**.
El operador recibe `🌀 [BEST] [COILING] XXXUSDT score 12 🔥atr9.1` y saca los niveles a
mano en TradingView. La idea es adjuntar a cada alerta BEST un plan (zona de entrada,
invalidación, stop, objetivo, R:B) **con datos que el motor ya calcula**.

**Es presentación, NO detección.** No se agrega señal nueva, no se toca la lógica de
decisión de `classify()`, no se mueve ningún bucket ni score. Mismo guardrail que
`swing/HANDOFF_movidas_telegram.md`.

---

## 3. ⚠️ BLOQUEANTE — primera tarea, antes de cualquier código

Contradicción entre config y código sobre el stop duro:

- `swing/config.json` → `exit_mgmt.STOP_PCT: 0.10`
- `swing/exit_tracker.py:53` → `STOP_PCT = _EX.get("STOP_PCT", 0.0)` con el comentario
  `# 0 = sin stop duro (config validada)`

El código dice que la config validada es **sin stop duro**; el JSON tiene 10%. Si la
alerta imprime "stop −10%" y el tracker no lo aplica (o al revés), el operador recibe dos
verdades sobre el mismo trade.

**Qué hacer:** revisar `sim_exit.py` (es donde se validó la capa de salida: ARM 12% /
TRAIL 8%, win 44%→56%, mediana −1,4%→+2,3%) y la memoria del proyecto, determinar cuál
de las dos es la validada, y dejar **una sola**.

**Ojo — esto NO es reversible como el resto.** Cambia lo que el `exit_tracker` hace en
vivo sobre posiciones que está gestionando. Commit propio, separado, y confirmar con el
usuario antes de aplicarlo.

---

## 4. Decisiones abiertas — confirmar ANTES de Fase 1 (no hacen falta para Fase 0)

| # | pregunta | recomendación de la sesión anterior | estado |
|---|---|---|---|
| 1 | ¿STOP_PCT 0.10 o sin stop duro? | se averigua, no se opina (sección 3) | **sin resolver** |
| 2 | ¿Uno o dos objetivos? | **uno**, y etiquetado `resistencia cercana` hasta que Fase 0 cruce | recomendado, **sin confirmar** |
| 3 | ¿Plan en todas las señales o sólo en las que no rompieron? | **sólo COILING/PREBREAK**; invalidación sola para el resto | recomendado, **sin confirmar** |

**El razonamiento de la 3, que no es estético:** mostrar "zona de entrada" en una señal
que ya rompió es operacionalizar el defecto medido de BREAKOUT (−4,2pp vs azar). Le
estarías poniendo interfaz prolija a lo único que se sabe que resta.

**El razonamiento de la 2:** dos objetivos con dos R:B duplican el problema de falsa
precisión, y agregan ~120 chars a un mensaje que va como caption (límite **1024**, no
4096 — el chart está ENABLED).

---

## 5. Estado actual — qué ya existe, no reimplementar

| Pieza | Dónde | Qué da |
|---|---|---|
| `price` | dict de alerta | precio actual |
| `ref_price` | dict de alerta, por señal | **el nivel de estructura**: PREBREAK/COILING/BREAKOUT → `recent_max`; RIDING → `riding_break_close`; HOLD → `riding_break_ref or recent_max` |
| `atr_pct`, `atr_pct_1d` | dict de alerta | volatilidad del tf y diaria |
| `dist_to_res` | feature dict por tf | `(one_h_resist − price)/price` → `one_h_resist = price*(1+dist_to_res)` |
| `major_struct_dist` | feature dict por tf | `(major_max − price)/price` → `major_max = price*(1+major_struct_dist)` (lookback 60) |
| gestión de salida | `swing/exit_tracker.py` | trailing ARM 12% / TRAIL 8%, validado en `sim_exit.py` |
| outcomes históricos | Supabase `screener_outcomes` (90d) | `entry_price`, `ref_price`, horizontes hasta 21d |
| **archivo local** | `archivo_outcomes/screener_outcomes.csv` | **3.311 alertas 31-may → 22-ago, ya bajadas.** Usar esto antes que pegarle a Supabase |

**Hallazgo que baja el riesgo a casi cero:** los dos niveles de resistencia se reconstruyen
desde campos ya exportados. **No hace falta tocar `analyze()` ni `analyze_at_time()`** —
o sea que no hay que editar los DOS dicts de features (líneas ~536 y ~956).

---

## 6. Diseño — de dónde sale cada nivel

**Zona de entrada.** Depende de si ya rompió:
- **COILING / PREBREAK** (no rompió): `[ref_price, ref_price*(1+buffer)]`, entrada *al
  romper*. Buffer = `PREBREAK_NEAR_MAX` (0.012), la constante que ya define "cerca del
  máximo". Reusarla, no inventar otra.
- **BREAKOUT / HOLD / RIDING** (ya rompió): la entrada de referencia es `ref_price` y el
  mensaje ya muestra `% desde zona de ruptura`. El aporte sería marcar si sigue siendo
  entrada válida o ya está extendido — `BREAKOUT_MAX_EXTENDED` (0.12) es el umbral que el
  motor ya usa para descartar breakouts tardíos. **Ver decisión 3: probablemente no vaya.**

**Invalidación (estructural).** `ref_price` perdido con **cierre de 4h** por debajo. Es el
número honesto y **no requiere ninguna estadística**: contesta "¿dónde deja de existir
este setup?", que es factual, no predictivo.

**Stop (numérico).** Lo que resuelva el bloqueante. Dos candidatos, elegir UNO:
- (a) **Empírico:** `exit_mgmt.STOP_PCT`, el que gestiona el tracker. Coherente por construcción.
- (b) **Por volatilidad:** `price − k*ATR`. **No está validado acá.** Si se elige, medirlo
  en Fase 0, no asumirlo.

**Objetivo.** `one_h_resist` (gate `not_near_resistance`) y/o `major_max` (60 barras). Si
vienen `None` o negativos (precio ya por encima), **no mostrar**: `sin resistencia a la
vista`, nunca un número roto.

**R:B.** `(objetivo − entrada)/(entrada − stop)`. Etiquetar **`R:B teórico`**, jamás como
expectativa. Es geometría correcta; la probabilidad de llegar no está en el número, y en
este sistema esa probabilidad está medida y no acompaña.

---

## 7. Fase 0 — validar antes de mostrar. NO SALTEAR.

**Cambio respecto del plan original:** el criterio de corte era *"si el Objetivo 1 se toca
en <40% de los casos, no mostrarlo como objetivo"*. **Eso no alcanza** — una tasa absoluta
no significa nada sin benchmark (sección 1).

**Criterio corregido:**

1. Para cada alerta BEST histórica, computar los niveles propuestos.
2. Medir **objetivo-antes-que-stop**, y compararlo contra un **dardo pareado**: la misma
   moneda entrando a horas al azar de la misma ventana, con los mismos niveles relativos.
3. **Corte:** si el margen contra el pareado no tiene el IC95 inferior sobre cero, el
   nivel **no se llama "objetivo"**. Se llama `resistencia cercana` y no lleva R:B.
4. Chequear concentración en **los dos ejes**: sacando los top-3 símbolos *y* sacando las
   top-3 semanas.
5. Comparar **R:B teórico vs R:B realizado**. Si divergen mucho, los niveles están mal
   diseñados y hay que arreglarlos *antes* de mostrarlos.

**Herramienta:** reusar `sim_exit.py` (ya replaya klines 1h forward desde `alerted_at`).
No escribir un simulador nuevo. Para resolver orden target/stop dentro de una vela, hay
precedente de bajar 5m de Binance por alerta — funcionó con ambigüedad 0,00%.

**Qué esperar:** con TP+10/SL−5 a 24h, BEST tocó primero el target 31,5% y primero el stop
35,1%. Fase 0 usa otros niveles y otro horizonte, así que no está contestada — pero si sale
parecido, no es sorpresa y no es motivo para re-operacionalizar hasta que dé lindo.

---

## 8. Fase 1 — inyección central en `classify()`

**Anclaje:** `swing/backtest.py:1568`, el loop `for c in candidates:` donde ya se inyecta
`c["atr_pct_1d"] = tf_1d.get("atr_pct")`. Ahí `tf_1h`, `tf_4h`, `tf_1d` y `tf_data` están
en scope.

Agregar `c["plan"]` con: `entry_low`, `entry_high`, `invalidation`, `stop`, `target_1`,
`target_2`, `rr_1`, `rr_2`. Todos `float` o `None`.

Hacerlo acá da una sola implementación para las cinco señales, y el backtest la ve igual
que el screener (mismo requisito que `atr_pct_1d`). Extraer a un helper
`_build_plan(c, tf_data, cfg)` para poder testearlo aislado.

---

## 9. Fase 2 — render en `format_alert()`

**Anclaje:** `swing/screener.py:533` (`format_alert`), después del bloque de `candle_line`
y antes de `streak_line`, siguiendo el patrón de `_streak_line` / `_hold_candidate_line`
(helpers que devuelven `None` cuando no aplican).

`_plan_line(alert)` → las líneas del plan, o `None` si `alert.get("plan")` es `None`.

```
  🎯 entrada 0.01234-0.01249 · inval <0.01234 (cierre 4h) · stop 0.01110
  📈 resistencia cercana 0.01380 (R:B teórico 1.2)
```

- **Markdown de Telegram:** el parser rompe con nº impar de `_` y de `*`. El código ya
  envuelve nombres de componente en backticks por eso (ver `_reasons_from_alert`). Si se
  agregan etiquetas con guión bajo → backticks.
- **Formato de precios:** `:.6g` como el resto del archivo (`price_line`). Hay pares con
  8 decimales y otros con 2.
- **Límite:** caption de `sendPhoto` = **1024 chars**, no 4096.

---

## 10. Fase 3 — opcional, sólo si Fase 0 sale bien

"Memo de decisión": bloque largo (setup + fuerza + riesgo + plan + estado) en el dashboard
(`project_swing_screener`), no en Telegram.

**No** agregar botón de aprobación/ejecución. El sistema alerta; la decisión y la ejecución
las hace el humano. Es decisión de diseño, no limitación.

---

## 11. Verificación

- **Unitaria de `_build_plan()`** con dicts sintéticos: `dist_to_res=None`,
  `major_struct_dist<0`, `ref_price=0`, `stop >= entrada` (R:B negativo o infinito →
  devolver `None`, **nunca imprimir `inf`**).
- **Smoke de formato:** correr `screener.py` en seco y revisar el texto de un BEST **sin
  enviar**, o mandarlo a un chat de prueba. Confirmar que las líneas nuevas aparecen y que
  no se pasa de 1024 chars.
- **Coherencia con el tracker:** tomar 3 alertas reales y verificar a mano que el `stop`
  impreso es el mismo que `exit_tracker` aplicaría. Si no coincide, el bloqueante no quedó
  bien resuelto.
- **Sanidad global (= la garantía de reversibilidad):** correr el mismo scan con y sin la
  rama y diffear buckets y scores. Tienen que ser **idénticos**. Si algo cambió, el cambio
  dejó de ser presentación y hay que revisarlo.

---

## 12. Reversibilidad — qué vuelve atrás y qué no

**Vuelve sin costo:** todo el código. Es una rama; el cambio es presentación pura; los
commits van separados por fase, así que se puede revertir el render (Fase 2) conservando
el cálculo (Fase 1). El diff de buckets de la sección 11 es la prueba de que es inerte.

**No vuelve atrás:**
- **El STOP_PCT.** Cambia lo que el tracker hace en vivo. Commit propio, confirmado.
- **Las alertas ya enviadas.** No se des-envían.
- **Las decisiones que el operador tome mirándolas.** Ésta es la irreversibilidad real y es
  la razón de todos los guardrails de etiquetado de este documento.

---

## 13. Rama y commits

- Rama: **`swing/plan-trading`** (convención `swing/*` de `CLAUDE.md`).
- Commits separados por fase:
  0. resolución del bloqueante STOP_PCT *(confirmar con el usuario antes)*
  1. Fase 0 — el script de medición y su resultado
  2. `_build_plan` + inyección en `classify()`
  3. render en `format_alert()`

**Estado del repo al escribir esto (2026-08-22):** rama activa `banco/primer-toque`, con
`screener.py`, `swing/screener.py` y `swing/exit_tracker.py` modificados sin commitear (es
el fix de la fuga del token de Telegram, ya commiteado aparte en `sec/telegram-token-leak`,
`ca106ac`). Sacar `swing/plan-trading` de `main`, no de `banco/primer-toque`.

---

## 14. Guardrails — no violar

- **Solo BEST va a Telegram.** Menor precisión → dashboard.
- **No inventar números.** Todo nivel sale de un campo que el motor ya computa o de una
  estadística ya medida sobre `screener_outcomes`.
- **`atr_pct_1d` es el único separador robusto.** Sirve para dimensionar (stop ∝ ATR), no
  para gatear nada nuevo.
- **El payoff madura >7d.** Cualquier objetivo tiene que ser coherente con ese horizonte,
  no con un intradía.
- **`screener.py` y `backtest.py` comparten semántica.** Campo nuevo en el dict → se calcula
  en `classify()` (backtest) y se lee en `screener.py`. Nunca duplicar el cálculo.
- **No aflojar un criterio después de ver el número.** Si Fase 0 no cruza, la salida no es
  bajar el umbral: es la versión reducida — **mostrar sólo la invalidación**, que es honesta
  pase lo que pase porque no afirma ninguna probabilidad.
