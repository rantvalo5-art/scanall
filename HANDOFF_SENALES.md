# HANDOFF — Lo que queda por probar (y con qué regla se cierra)

> Abrir en una conversación nueva y empezar por la sección 5.
> Escrito el **2026-08-15**, después de medir y cerrar cuatro líneas de trabajo.
> Complementa `HANDOFF_BASIS.md` (basis/funding, cerrado).
>
> **Revisado el 2026-08-16.** Apareció el archivo histórico del proyecto (51 memorias de
> jun-ago-2026, indexadas bajo la ruta vieja del repo, `C--Users-asd-scancrypto-scanall`).
> Contenía experimentos que este handoff daba por no hechos. **El ítem 4.3 quedó
> prácticamente cerrado por evidencia previa** — ver ahí. Lo que decidía algo está
> consolidado en la memoria activa.

---

## 0. Regla de parada GLOBAL — leer antes que nada

Este repo lleva meses de construir antes de medir. La disciplina que lo dio vuelta fue
**escribir el criterio antes de mirar los números**. Se mantiene:

> **Cada ítem de la sección 4 tiene su regla de parada ya escrita. No se renegocia
> después de ver el resultado.** Si un experimento no puede cambiar la decisión, no
> se corre.

Y un límite que no existía antes:

> **Presupuesto total: 4 sesiones.** Si al agotarlas nada cruzó su umbral, se cierra la
> búsqueda de ventaja direccional y el capital queda en el piso (prestar stablecoins,
> 5-10% anual). Reabrir requiere una hipótesis nueva, no otra variante de las de acá.

**No hay capital operando hoy.** Nada sangra mientras se decide. Esto es investigación,
no rescate.

---

## 1. Lo que ya está medido y muerto — NO volver a probar

| línea | veredicto | número que lo cerró |
|---|---|---|
| Swing (predecir dirección con velas) | sin ventaja | −1,61% vs BTC; universo −1,65%; azar −0,79% |
| Basis / funding de perpetuos | bajo el piso | BTC +3,35% bruto anual; mediana del universo −4,71% |
| Basis de futuros con vencimiento | bajo el piso | BTC dic-2026 +4,45% anual (converge seguro, pero chico) |
| Day trader (alertas reales, con costos) | **peor que al azar** | −0,86pp a 4h, −3,27pp a 24h; ranking INVERTIDO |
| Selección de moneda (primer toque ±8%) | suma ~0 | win rate 48,63% vs 51,25% necesario |
| Corte transversal / momentum relativo | no replica a 5 años | **0/35 combos** positivos por media winsorizada |
| Reversión de corto plazo | = al costo | edge bruto +0,31% máx contra 0,40% de costo |
| Perseguir memes que pumpean | negativo | −4,2pp vs azar; tamaño chico no cambia el signo |
| Entradas a dos puntas (straddle sintético) | imposible estructuralmente | se regala k·ATR por trade |
| Detectores de régimen (familia convencional) | ninguno gateable | batería de 7 sobre 28w: 0 pasan; el "ganador" no replicó sobre 22 trimestres (+0,09 → −0,08) |
| Inversión del ranking del day trader | era composición | el score cae −0,576pp/punto a 24h, pero **−0,178pp (t=−0,81) sacando FADING**, que está apagado |
| Vender volatilidad (BTC/ETH, 5,3 años) | el premio se compitió | BTC de +16,6% a +9,9% de la prima → +7,33%/año reciente, dentro del piso; ETH a +0,5% |

**Contexto que hay que tener presente:** la mediana de los pares USDT cayó **−2,81%
cada 14 días durante 5 años**. Ninguna estrategia larga-sola sobre alts sobrevive eso.
Todo lo que quede tiene que ser neutral o corto.

---

## 2. El marco de medición — usarlo, no reinventarlo

Vive en **`banco/`**. Correr desde ahí:

```
py -3.13 primer_toque.py [--target 8 --stop 8 --regimen]
```

Para probar una señal nueva:

```python
from klines import load_panel
from primer_toque import tabla, evaluar_senal
panel = load_panel("2025-08-01", "2026-08-01", n=200)
T = tabla(panel, target=8, stop=8, horizonte_d=30)
evaluar_senal(T, mi_mascara, "mi señal")
```

Imprime cuántos pp aporta y **si cruza el umbral**, que es lo único que decide.

### Las dos fórmulas que evitan engañarse

```
win rate necesario = (stop + costo) / (target + stop)
```

El win rate **es una perilla, no una habilidad**: con +8%/−16% sacás 64% de aciertos sin
ninguna señal y perdés plata igual. Comparar contra el umbral, nunca contra 50%.

Y: **un activo sin dirección da ~52%, no 50%** (para recuperarte de −8% necesitás +8,7%;
la barrera de abajo está más lejos en escala log).

### La línea base pareada — usarla para toda pregunta de timing

Salió midiendo el 4.1 y es mejor control que el azar global. Para cada alerta se sortean
~10 horas al azar **del mismo símbolo, en la misma ventana**, y se compara contra ellas. Eso
descuenta de una las tres cosas que ensucian: la selección de moneda, el sesgo de universo y
la caída general del mercado. Lo que queda es puro *timing*, que es lo único que el screener
elige. Son ~20 líneas sobre `banco/klines.py` (velas 1h, `c[k:]/c[:-k] − 1`).

Con ese control, **la línea base de azar global del handoff queda como referencia gruesa**:
la pareada da deltas distintos por señal y es la que decide.

---

## 3. Las trampas que ya mordieron — con nombre y apellido

1. **Concentración.** Todo promedio positivo se rechequea sacando el top-1/3/5. Dio
   vuelta resultados **cuatro veces**: BANKUSDT (swing, dos veces), 币安人生USDT +
   BANKUSDT (basis), ZECUSDT + DEXEUSDT + STOUSDT (transversal, de +27,5% a +0,1%).
2. **Media contra mediana — y cuál manda.** LUNAUSDT (may-2022) cayó a $0,0000001 y el
   retorno forward daba **+5.199.900%**, contaminando una rejilla entera. Usar mediana +
   media winsorizada a ±100%. **Pero para una cartera el P&L es la MEDIA**: si mediana y
   media se contradicen, gana la media. La mediana es chequeo de robustez, no resultado.
3. **Ventana relativa a hoy.** Siempre fechas explícitas. `--weeks` fabricó artefactos de
   varios puntos porcentuales.
4. **Costos desde el día uno.** Antes de ago-2026 ningún número del repo los incluía. El
   backtest de la raíz **todavía no los tiene**.
5. **Cache que no pega.** `banco/klines.py` mira `.parquet` **y** `.csv`. Chequear solo
   uno hacía re-descargar 186 monedas por corrida (7 min → 2 s).
6. **Subpotenciado ≠ refutado.** El transversal daba +27,5% con 1 año y 0/35 con 5. Si un
   resultado depende de 3 nombres de 149, no se puede distinguir de cero: pedir más datos
   antes de creerlo *o* descartarlo.
7. **Matar el vigilante junto con el vigilado.** Un loop `until grep` quedó huérfano horas
   después de matar el proceso que miraba.

---

## 4. Lo que queda — ordenado por lo que yo probaría primero

### 4.1 — Fadear el propio screener  ·  **CERRADO — era composición (2026-08-16)**

**Hipótesis original.** El ranking del day trader está invertido: BEST rinde peor que WATCH
(mediana a 4h: −1,786% contra −0,878%). Si la inversión es real, es información aprovechable.

**Medido** sobre `daytrader_outcomes` (10.273 filas, 26-jun → 16-ago, 327 símbolos, 8 semanas).
**La sospecha de composición era correcta, y más fuerte de lo que se pensaba:**

1. **El bucket ES el tipo de señal.** FADING es el 75,3% de las filas y **el 100% de FADING es
   WATCH**; el 98,4% de WATCH es FADING. EXPLOSION no tiene ninguna WATCH, RIDING ninguna
   STRONG, HOLD una sola. Los scores son casi disjuntos por señal (FADING 4-8, RIDING 6-8,
   EXPLOSION 8-11, HOLD 9-15). "BEST vs WATCH" era "señales activas vs FADING".
2. **Controlando por señal, el gradiente no es monótono ni invertido.** Spearman(score, retorno)
   dentro de cada señal, con IC95 bootstrap: a 4h BREAKOUT −0,036 / FADING −0,021 / HOLD −0,013 /
   PREBREAK −0,081, **todos con el IC cruzando cero**. La única que lo excluye es **RIDING
   +0,112, con el signo al revés**. Igual a 24h.
3. **El coeficiente agregado muere al sacar la señal apagada.** Regresión con dummies de señal y
   timeframe, winsorizada a ±100%: −0,576pp por punto de score a 24h (t = −4,30) — pero
   **sin FADING queda en −0,178pp (t = −0,81)**. El efecto vivía entero dentro del único tipo de
   señal que **no corre en producción** (`active_signals.FADING=false`).
4. Sin FADING, **WATCH son 220 filas = 8,7% del feed real**: el contraste que motivaba el ítem
   casi no existe en vivo.

> **La regla de parada dice cerrar, y se cierra.** No se renegocia.

**Lo que sí apareció, como hipótesis NUEVA (ver 4.7).** Se construyó una línea base pareada
—**mismo símbolo, hora al azar** en la misma ventana— que aísla el *timing* de la selección de
moneda y de la caída del mercado. Las alertas entran en momentos peores que el azar, pero **el
defecto se concentra en las dos señales que compran extensión**:

| señal | delta media 4h | delta media 24h |
|---|---|---|
| EXPLOSION | **−1,94pp** | **−3,17pp** |
| BREAKOUT | −0,49pp | **−2,52pp** |
| FADING | −0,32pp | −1,56pp |
| RIDING | +0,19pp | −0,03pp |
| HOLD | +0,02pp | +0,67pp |
| PREBREAK | +0,42pp | +0,98pp |

Es confirmación independiente, sobre datos del day trader, de lo que el swing ya había medido:
**el defecto de entrada es comprar el techo de la vela de extensión.** HOLD y PREBREAK no lo
tienen.

---

### 4.2 — Funding extremo como señal contraria  ·  esfuerzo: 1 sesión  ·  prior: medio-bajo

**Hipótesis.** La Fase 1 midió el funding como *ingreso a cobrar* y murió porque el nivel
es chico. Nunca se midió como **sentimiento**: funding muy positivo = longs apalancados
amontonados = posible reversión.

**Ventaja práctica.** Los datos **ya están cacheados** en `basis/.funding_cache/`
(351 símbolos, ago-2025 → ago-2026). No hay que bajar nada.

**Qué medir.** Máscara sobre el banco: entradas donde el funding trailing está en el
percentil 95+ (o 5−). Pasar por `evaluar_senal()`.

> **Regla de parada.** Sigue solo si cruza el umbral de rentabilidad (no basta con sumar
> pp), con mediana por semana positiva y sobreviviendo el top-3. Y **el sesgo de un año
> bear hay que declararlo**: el funding extremo positivo es raro en bear.

---

### 4.3 — Detectores de régimen alternativos  ·  **CERRADO por evidencia previa (2026-08-16)**

**Por qué parecía vivo.** El régimen **domina** el resultado — el win rate mensual va de
34,68% (nov-2025) a 83,30% (jul-2026), casi 50 puntos, contra los ~3 que sumaría una
señal de selección. Es la variable que decide.

**Por qué se cierra.** Este ítem se escribió creyendo que se había probado **una** familia
(retorno pasado del mercado). El archivo histórico muestra que se probó una **batería de 7**
sobre `diag_28w.json` (3.704 alertas, 7 folds mensuales), con barra declarada de antemano —
signo estable ≥6/7 **y** costo de recall <20% — y **ninguna pasó las dos**:

| detector | resultado |
|---|---|
| tendencia BTC (SMA100 / pendiente SMA50 / drawdown60) | 5/7 **negativos** (reversión, al revés del manual) |
| **volatilidad BTC (percentil de rvol)** | 4/7, inestable |
| **funding agregado de mercado** | 5/7, mixto |
| pendiente de amplitud | 4/7 |
| **correlación cruzada promedio** | única con signo 6/7, pero Spearman +0,09 y tira **31% de los movers** |
| **amplitud en nivel (% sobre SMA50)** | señal en U, ayuda 3/7 meses, tira 36% de movers = overfit de calendario |

Las cuatro en negrita son exactamente las que este ítem proponía como "falta probar".

Y hay confirmación sobre **historia profunda** (2021→2026, 22 trimestres, 9 bull / 9 bear
incluido el bear 2022 completo, 276 muestras de 7d no solapadas): ningún detector
deployable. La correlación media, el "6/7 robusto" de la batería, **no replicó**
(Spearman +0,09 → −0,08). La volatilidad **invierte el signo** entre bull y bear.

**La regla de parada de este ítem ya está disparada**: pedía el mismo signo en ≥3 lookbacks
y lo que hay es inestabilidad de signo entre regímenes reales, que es la versión fuerte del
mismo test. Sumado al problema de lag ya escrito acá (si el régimen dura 2-3 semanas y el
detector tarda 2 en confirmar, llegás tarde por construcción).

**Lo único que sobrevive sin probar:** ATR agregado **del universo** como nivel (se probó
rvol de BTC, no ATR del universo). Prior muy bajo y mismo problema de lag. **No gastarle una
sesión** salvo que se agoten los demás ítems. Si se retomara la familia, tendría que ser un
enfoque **no-gate** (sizing suave, expectativa baja), no otro filtro de entrada.

> **Veredicto: el régimen es real pero no anticipable.** Queda cerrado. Presupuesto
> liberado: este ítem devuelve 1-2 sesiones a los otros.

---

### 4.4 — Vender volatilidad con opciones  ·  **CERRADO — el premio se compitió (2026-08-16)**

**Por qué era distinto.** Era el único lugar donde la habilidad *demostrada* del screener
tiene comprador natural: está medido que predice **cuánto** se mueve una moneda y que no
predice **para qué lado**, y las opciones pagan exactamente por lo primero.

**Medido** con DVOL de Deribit (implícita a 30d) contra la realizada de los 30 días
**siguientes**, 2021-03 → 2026-07 = **5,3 años, 65 meses no solapados**. Reproducible con
`opciones/iv_rv.py`.

**El premio existió y era grande.** Vendiendo una straddle ATM por mes, neto de 5% de la
prima en costos: BTC **+20,96%/año** sobre toda la muestra, drawdown máximo −11,3%,
retorno/DD 10,05, y aguanta sacar los 3 mejores meses (+15,03%/año). Es el mejor número
que produjo este repo, y a diferencia de todo lo demás **tiene mecanismo**: el premio de
varianza es una prima de seguro, no un patrón encontrado buscando.

**Y se compitió.** Ésta es la parte que decide, y hubo que separarla del efecto nivel:

| año | implícita | realizada | IV/RV | % de la prima | neto BTC |
|---|---|---|---|---|---|
| 2021 | 91,8% | 73,7% | 1,312 | **+19,1%** | +53,94% |
| 2022 | 73,6% | 61,4% | 1,238 | +14,7% | +33,36% |
| 2023 | 49,6% | 43,0% | 1,185 | +12,1% | +8,49% |
| 2024 | 57,7% | 50,5% | 1,222 | +11,6% | +13,59% |
| 2025 | 46,0% | 39,6% | 1,273 | +13,6% | +9,03% |
| 2026 | 45,2% | 44,4% | 1,201 | **−0,0%** | −4,85% |

El nivel de implícita bajó 38% (82% → 50%), **pero el premio relativo también se
comprimió**: IV/RV de 1,289 a 1,241, y el premio de **+16,6% a +9,9% de la prima**. Que se
comprima lo *relativo* es lo que importa — si fuera solo nivel se arreglaría con tamaño.

**El número que dispara la regla: en el régimen reciente (2023→) BTC da +7,33%/año neto**,
que cae **dentro** del piso de stablecoins (5-10%), con drawdown de 11% y la cola abierta.

**ETH está muerto sin ambigüedad:** el premio relativo pasó de +8,4% a **+0,5% de la
prima** (IV/RV 1,077), régimen reciente **−4,56%/año**, drawdown −37,2%, y **sin los 3
mejores meses el rendimiento de 5 años completo es +0,17%/año** — o sea que el resultado
entero eran tres meses. Es la trampa de concentración disparando de manual.

**Y todo lo que falta contar empuja para abajo:** el delta hedging (~30 rebalanceos por
trade, no contados, y la fórmula modela captura de varianza que *requiere* cubrir); vender
al bid y no al medio; que el −11% del peor mes es mark-to-market y con margen un spike te
liquida al peor precio, que no tiene piso; y todo el capital en un solo venue offshore
—FTX está adentro de la muestra—.

> **La regla de parada dispara. Se cierra sin construir nada.** Que era exactamente para
> lo que estaba escrita: hacer la cuenta antes.

**Lo único que lo reabriría** es que el premio relativo se vuelva a abrir. `iv_rv.py` queda
para re-chequear en un año: si IV/RV vuelve sostenido por encima de ~1,30 con el premio
arriba del 15% de la prima, vale mirarlo de nuevo. **Pero apostar a que eso pase es apostar
a un régimen, y este repo ya midió que el régimen no se anticipa.**

---

### 4.5 — Funding entre exchanges  ·  esfuerzo: 2 sesiones  ·  prior: bajo

Cobrar la **diferencia** de funding entre Binance / Bybit / OKX en vez del nivel — que es
justamente lo que falló por ser muy chico. Delta-neutral.

**Contras:** capital partido en dos exchanges, transferencias, el doble de superficie
operativa, y la diferencia también está competida. Además `fapi.binance.com` está
geo-bloqueado desde runners cloud (451/403) → hace falta PC propia o VPS.

> **Regla de parada.** Medir primero la **diferencia histórica** entre exchanges neta de
> los costos de las cuatro patas. Si no supera el piso de stablecoins con mediana positiva
> por símbolo y por semana, se cierra sin construir nada.

---

### 4.7 — Fadear las señales de extensión  ·  esfuerzo: 1 sesión  ·  prior: medio-bajo

**Hipótesis NUEVA** (nace de la medición del 4.1; no es una renegociación de aquella regla).
No fadear *el ranking* —eso ya murió— sino **las dos señales que compran extensión**, que son
las únicas con un defecto de timing grande y medido: EXPLOSION (−3,17pp vs azar a 24h) y
BREAKOUT (−2,52pp). Fadear BEST activas a 24h dio media **+1,19%** (sin top-3: +0,84%),
mediana +4,18%, 6 de 8 semanas con media positiva.

**Por qué NO se sigue de una:**
- **Ventana de 51 días, un solo régimen bear.** El repo ya se comió esto varias veces.
- **Shortear no es spot.** Necesita perpetuos: funding, borrow, y `fapi.binance.com`
  geo-bloqueado desde runners cloud (hace falta PC propia o VPS).
- **Sin slippage**, y las alertas apuntan justo a los pares finos donde más duele.
- **La cola izquierda de un short no tiene piso.** El máximo bruto de la muestra fue **+744%**:
  un short ahí pierde 7 veces la posición. La brecha media-mediana (+1,19 contra +4,18) es esa
  asimetría asomando, y **para una cartera el P&L es la media**.

> **Regla de parada.** Sigue solo si, sobre las señales de extensión y **neto de los costos
> reales de un short en perpetuos** (fee + funding + un supuesto de slippage explícito):
> (a) la **media** —no la mediana— queda positiva; (b) sobrevive sacar el top-3 de símbolos;
> (c) es positiva en **≥6 de 8 semanas**; y (d) sigue positiva **restando el peor símbolo
> individual de la ventana**, que es el test que importa cuando la cola es ilimitada.
> Si falla cualquiera de las cuatro, se cierra sin construir nada.

**Antes de codear nada:** hacer la cuenta de la cola, al estilo de la sección 3 del handoff de
basis. Si un solo +744% borra el acumulado de la ventana, no hace falta backtest.

---

### 4.6 — Descartados de entrada (para no volver a proponerlos)

- **Libro de órdenes / microestructura:** compite en latencia contra infraestructura
  profesional. No es terreno para retail desde una PC.
- **On-chain:** señal lenta, ruidosa y ya arbitrada por quien tiene datos mejores.
- **Market making:** el maker spot en Binance VIP0 no tiene rebate; sin ventaja de fees
  no hay negocio.

---

## 5. Primeros pasos para la conversación nueva

1. Leer la sección 3 (las trampas). **Cinco de los seis hallazgos falsos de este repo
   salieron de ahí.**
2. **Chequear el archivo histórico antes de proponer nada.** 51 memorias de jun-ago-2026 en
   `~/.claude/projects/C--Users-asd-scancrypto-scanall/memory/`; su `MEMORY.md` es un índice
   de una línea por experimento. Ya pasó una vez que este handoff diera por no-probado algo
   probado y enterrado (el ítem 4.3).
3. Elegir **un** ítem de la sección 4 y releer su regla de parada antes de correr nada.
4. Correr la línea base de `banco/` para tener contra qué comparar.
5. Medir. Contrastar contra la regla escrita. Seguir o cerrar — sin renegociar.

**Prompt sugerido:**

> Leé `HANDOFF_SENALES.md` en la raíz. Arranquemos por el ítem 4.2 (funding extremo como
> señal contraria): los datos ya están en `basis/.funding_cache/`, no hay que bajar nada.
> Contrastar contra la regla de parada escrita en ese ítem.

---

## 6. Expectativa honesta

Cuatro líneas medidas, cuatro cerradas. Eso es evidencia real de que **el rincón
explorado está vacío**: reglas de umbral sobre velas públicas, solo largo, horizontes de
minutos a 30 días. Pero es un rincón, no la habitación — lo de la sección 4 son familias
distintas, no variantes de lo mismo.

Lo más probable sigue siendo que ninguna cruce. Si eso pasa, el resultado no es el
fracaso: es haber comprado certeza barata. Cuatro proyectos cerrados en días, con la
regla escrita antes de mirar, es más disciplina de la que aplica la mayoría de la gente
que opera con plata real durante años.

**El piso de stablecoins no es el premio consuelo. Es el rival, y hasta hoy va ganando.**
