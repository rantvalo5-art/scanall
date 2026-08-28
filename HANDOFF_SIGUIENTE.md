# HANDOFF — las cinco direcciones que quedan

> Escrito el **2026-08-26**, **actualizado el 2026-08-27** (ver sección 0.5: la sesión
> del 27 cerró 4.1, respondió parte de 4.2 y produjo lo primero que sobrevivió).
> Este reemplaza a `HANDOFF_PENDIENTE.md` como **punto de entrada**: aquel quedó con TODAS sus secciones cerradas (1, 4.1 y 4.3) y sirve ya
> solo como registro. Los anteriores (`HANDOFF_UNLOCKS.md`, `HANDOFF_SENALES.md`,
> `HANDOFF_BASIS.md`, `HANDOFF_CIERRE.md`) son históricos.
>
> Se puede abrir en frío: la sección 0 tiene lo operativo y la 2 el mapa de lo muerto.

---

## 0. Cómo arrancar (verificado en carne propia, no de memoria)

```bash
# BANCO DE HIPOTESIS — se corre DESDE banco/
cd banco
py -3.13 -u lote.py                    # batería estándar de precio
py -3.13 -u leadlag.py --nula 5        # calibrar look-elsewhere de un lote
py -3.13 -u libro.py --top 600 --orden 10000 --reps 3   # costos reales del libro

# DAY TRADER (raíz) — se corre desde la raíz
py -3.13 -u backtest.py --weeks 2 --end-date 2026-08-15 --max-pairs 200 \
    --scan-interval-min 2 --out salida.json
```

**Gotchas, todos pisados:**

- Siempre `py -3.13`, nunca `python`. Siempre `-u`, o una corrida de horas no muestra
  nada y si muere el log queda vacío.
- `$env:PYTHONIOENCODING = "utf-8"` antes de correr, o revienta con cp1252.
- Los procesos se llaman `python3.13`. **`ps` de Git Bash NO los ve** — usar
  `Get-Process python3.13` desde PowerShell. Un `ps` vacío no significa que murió.
- Corridas pesadas **de a una**.
- **`Parallel(n_jobs=-1, backend="loky")` no imprime NADA** entre el dispatch y el
  resultado. Una corrida de 8 semanas del day trader son ~2,9 h de silencio total y
  **murió 3 veces ahí**; partida en trozos de 2 semanas (~45 min, ~2,0M tareas) anda.
  No era memoria: medido en vivo, padre 1,63 GB + 12 workers × 0,08 GB = ~2,6 GB
  contra 11,8 GB libres.
- **`ta` calcula Bollinger con `ddof=0`**, no el default de pandas (`ddof=1`). Sesgo
  constante de 2,6% en UNA dirección y **no falla nada**. Verificar numéricamente
  contra `ta` antes de reimplementar cualquier indicador suyo.
- **Nunca `groupby.apply` con lambdas** sobre miles de grupos: en `micro.py` costaba
  273 s por par y vectorizado quedó en 0,10 s (2700×).
- El writer de parquet falla en esta máquina: el caché del banco es `.csv` por fallback.
- **Heredocs de bash con tildes/backticks fallan** al escribir archivos grandes; usar
  la herramienta de escritura directa.

**Cachés (grandes, no borrar):**

| caché | qué tiene | tamaño |
|---|---|---|
| `banco/.kline_cache/` | 1h angosto y ANCHO (`_v2`) de `base200`, 2025-08→2026-08 + 5m | 1,7 GB |
| `.backtest_cache/` | klines del day trader + **1m de 200 pares 2026-06-20→08-15** | 1,77 GB |
| `banco/.unlock_cache/`, `.metrics_cache/`, `.funding_cache/` | ya gitignoreados | 140 MB |

`banco/klines.py: load_panel(..., full=True)` trae el panel **ANCHO** (o/h/l/c/v/qv/n/vb).
Con `pin="base200"` el universo queda **congelado y reproducible** — usarlo siempre.

---

## 0.5. ESTADO AL 2026-08-27 — leer esto antes que la sección 4

La sesión del 27 de agosto cambió el mapa. Resumen de una línea: **se cerró la dirección
en serio, y apareció lo primero que sobrevive un forward test.**

### Lo que se midió

| | |
|---|---|
| formas de predecir **dirección** probadas | **4.140** — precio, flujo del kline y posicionamiento de futuros, las **dos direcciones**, horizontes de 4h a 7d, k de 3/8/16, 5 años, 4 regímenes |
| **sobrevivieron** | **0** |
| formas de predecir **magnitud** | 38 sobreviven, aguantan los cuatro regímenes |

Herramienta nueva: **`banco/ranking.py`** — evalúa rankings **transversales por barra**
(top-k contra el universo de la misma barra), que es un diseño distinto del de `lote.py`
y elimina por construcción cuatro defectos: el corte pooled que confunde tiempo con
sección cruzada, la trampa de `SEM_N_MIN`, el control apareado por símbolo en vez de por
barra, y las entradas solapadas. **El veredicto negativo se sostuvo igual**, así que ya
no se le puede achacar al diseño.

Todo el detalle, con las reglas escritas antes de cada corrida, en
**`banco/PREREGISTRO_TRANSVERSAL.md`** (4 corridas).

### Lo que se construyó: `radar/`

El único hallazgo positivo, ya en producción. Rankea por **`n_surge`** (operaciones de la
última hora contra su mediana de 7 días) y devuelve las 8 monedas que más probablemente
**se muevan** en las próximas 4h. **No dice dirección.**

- **240 líneas y cero configuración**, contra 2.111 líneas y 233 parámetros del screener
  de la raíz.
- Medido: **1,21×** el recorrido de la moneda típica, **62,6%** de acierto contra 49,5%
  de línea base, **100% de 251 semanas**, t = 22,6.
- El horizonte de 4h **se midió, no se eligió**: la señal se decae con el tiempo
  (4h 1,21× / 24h 1,15× / 7d 1,11×).
- Corre solo cada 4h por GitHub Actions y guarda en Supabase (`radar_runs`).
- **Es modesto**: "se mueve ~21% más que la típica", no "se mueve el doble". Lo que vale
  es la consistencia.

**Sigue sin instrumento**: saber que algo se va a mover no es plata sin convexidad.

### Los tres pendientes CON FECHA

| fecha | qué | comando |
|---|---|---|
| **~8 sep** | primer chequeo del radar — si el efecto es del tamaño medido, ya se ve | `cd radar && py -3.13 -u medir.py` |
| **~14 oct** | el chequeo que decide para un efecto de la mitad | idem |
| **19 oct** | forward test de **4.7 (fade)**, de `HANDOFF_CIERRE.md` | `cd fade && py -3.13 evaluar.py` |

`radar/HANDOFF_FORWARD_TEST.md` tiene la decisión de cada resultado, escrita antes de que
existiera un dato.

### Dos números heredados que resultaron estar mal

Los dos se venían arrastrando sin examinar, y los dos cambiaron al medirlos:

1. **El horizonte de 24h.** Era el que el banco fijó al principio y nunca se cuestionó.
   Barrido con métricas sin escala, el correcto es **4h**.
2. **El "esperá 8 semanas"** del forward test. Copiado del `SEM_MIN` de `lote.py`, que
   existía porque allá las entradas **se solapan**. Acá no. Calculado con la
   autocorrelación real (`banco/cuanto_esperar.py`): si el efecto es del tamaño medido se
   ve en **12 días**.

> **La lección, que vale más que los dos números:** cuando un parámetro no viene de una
> medición, viene de una sesión anterior que tampoco lo midió.

### Rama y estado del repo

- **`main`**: el radar, corriendo. No toca nada de lo anterior.
- **`banco/primer-toque`**: toda la investigación. ⚠️ **Arrastra 3 commits que cambian el
  swing en producción** (`swing/backtest.py` +753 líneas, `screener.py`, `config.json`).
  Si se mergea, esos se revisan aparte.
- El screener de la raíz y el swing: **intactos**.

---

## 1. Estado del repo al cerrar la sesión

**Nada mergeado. Tres ramas vivas:**

| rama | commits de hoy |
|---|---|
| `day/forming-replay` | `93e6b3b` forming cableado + `_cache_slice` · `e5d2631` · `c7cdd51` |
| `banco/primer-toque` | `3fe6893` unlocks+micro · `a21740c` lead-lag · `74818fc` costos · `c3f24bf` |
| (sin rama) | `swing/exit_tracker.py` y `swing/screener.py` modificados, **sin commitear** |

**Ojo con la partición:** `HANDOFF_PENDIENTE.md` y este archivo viven en
`day/forming-replay`; el trabajo del banco vive en `banco/primer-toque`. Desde una
rama no se ve lo de la otra. **Decidir si se mergean** — es la fricción principal.

Sin trackear a propósito: `bt_*.json`, `fade/puente*.json` (salidas regenerables,
ninguna se commiteó nunca). Sí commiteado: `archivo_outcomes/` (CSV de Supabase, **no
regenerables** tras la purga de 30 días).

---

## 2. El mapa: qué está muerto y — lo que importa — QUÉ PATRÓN DEJA

### 2.1 Lo cerrado (no re-proponer)

| familia | cómo murió |
|---|---|
| Precio, todas sus transformaciones (450+ hipótesis) | 0 sobreviven |
| Régimen (7 detectores × 22 trimestres) | ninguno gateable |
| Salida y timing (7 confirmaciones) | mueven mediana/cola, **la media nunca** |
| ML sobre 36 features | techo condicional medido; el +17pp eran 3 alertas de TUTUSDT |
| Volumen y forma de vela a 1h | 0 de 86 |
| Microestructura intra-vela, top-200 | 0 de 192 |
| Unlocks | β=−5,91pp/década pero IC cruza 0; 1.040 eventos son TODOS los que hay |
| **Lead-lag entre alts** | **0 de 384** (hoy) |
| **Cola ilíquida** | **cerrada por costos** (hoy), ver 2.3 |
| Basis / funding | el funding cobrable (BTC +3,35%) < piso de stablecoins |
| Straddles con órdenes stop | sin convexidad; regalás k·ATR por trade |
| Day trader | pierde en todo horizonte y **peor que al azar**; ranking INVERTIDO |
| Swing | sin ventaja; el defecto es el timing de entrada |
| **Ranking transversal, DIRECCION** | **0 de 4.140** (27-ago) — precio + flujo + posicionamiento, las dos direcciones, 4h a 7d, k 3/8/16, 5 años, 4 regímenes |
| **Market making en spot (4.1)** | **cerrado por comisión** (27-ago) — spread realizado 0,0133% contra un fee de 0,0750%; tampoco cruza el de futuros (0,0200%) |

### 2.2 EL PATRÓN, que es lo más valioso de todo esto

> **Todo lo direccional falló. Lo único que funcionó fue NO direccional y con mecanismo.**

**Vender volatilidad**: +20,96%/año en BTC, **5,3 años**, DD máx −11,3%, sobrevive
sacar los 3 mejores meses. Es el mejor número que produjo el repo y —a diferencia de
todo lo demás— tiene **mecanismo** (prima de seguro), no es un patrón encontrado
buscando. Murió porque **se compitió**: +7,33%/año en el régimen reciente, dentro del
piso de stablecoins. **No era falso, era tarde.**

Contra eso: diez familias direccionales, todas en cero. Eso no es mala suerte — es una
respuesta consistente sobre predicción direccional en cripto líquida.

**Corolario para elegir qué sigue:** priorizar ideas **con mecanismo** y **no
direccionales** sobre buscar una feature más.

> **Actualización 2026-08-27.** El corolario se cumplió, y con un matiz que vale: lo que
> apareció (`radar/`) es no direccional y con mecanismo —agrupamiento de volatilidad—
> pero **sigue sin instrumento para cobrarse**. Vender volatilidad tenía instrumento y
> se compitió; esto tiene señal y no tiene instrumento. Son dos formas distintas de no
> llegar, y conviene no confundirlas al elegir qué sigue.

### 2.3 Los costos reales (medidos hoy — cambian todas las cuentas)

El banco asume `COSTO_PCT = 0,20%` (solo fee taker, **sin spread ni slippage**). Se
midió el costo real caminando el libro (`banco/libro.py`, 480 pares USDT spot vivos):

| banda | orden $1k | orden $10k | win rate necesario (target/stop 8%) |
|---|---|---|---|
| rank 1-50 | 0,230% | 0,279% | 51,44% / 51,74% |
| rank 51-200 | 0,339% | 0,597% | 52,12% / 53,73% |
| rank 201-400 | 0,441% | 0,994% | 52,76% / **56,21%** |
| rank 401-600 | 0,524% | 1,261% | 53,28% / **57,88%** |

Entre **1,5× y 6,3×** lo supuesto. **NO se cambió `COSTO_PCT`**: rompería la
comparabilidad, y como todo lo cerrado se cerró contra un costo demasiado BARATO,
subirlo solo lo mata más. Pero para cualquier resultado **positivo**, o cualquier cosa
**fuera del top-200**, usar esta tabla.

**Lo que NO funciona para estimar spread:** Corwin-Schultz y Roll sobre OHLC. El rango
high-low mediano de una hora de BTCUSDT son ~49 bps contra un spread real de ~1 bp, así
que CS mide volatilidad (8,4 bps a 1h, 42,9 a 1d) y sale **plano** entre cuartiles de
volumen. Peor: su piso de ruido **escala con la volatilidad**, o sea que habría inflado
la cola por volátil y no por ilíquida. **Medir el libro, no estimarlo.**

### 2.4 Dos sutilezas de contabilidad, para no re-descubrirlas

1. **La convención aritmética infla al dardo.** `winrate_necesario` usa
   `(stop+costo)/(target+stop)`, que trata +8% y −8% como simétricos; en log no lo son.
   Con 0,20% eso le da a un activo **sin deriva** un margen aparente de **+0,75 pp** que
   geométricamente es **cero**. No es plata: es la brecha aritmético/geométrico.
2. **Ensanchar las barreras NO crea ventaja.** Para un activo sin deriva el retorno log
   esperado por trade es `−costo` a cualquier ancho. Lo único que cambia es la
   **cantidad de round trips** por unidad de tiempo. Si parece que ensanchar "da
   ventaja", es la convención aritmética otra vez.

---

## 3. Las reglas de método (no se negocian)

> **La regla de parada se escribe ANTES de mirar.** Si se afloja después de ver un
> número, el experimento no vale.

> **El p que decide es el de BLOQUES**, no el binomial. La brecha entre los dos *es* el
> autoengaño. Casos medidos: micro p_indep 1,1e-36 → p_bloques 0,3845; lead-lag
> 2,3e-44 → 0,1735.

> **El n efectivo son las SEMANAS, no las entradas.** 111.330 entradas solapadas con
> régimen autocorrelacionado valen ~49 bloques.

> **`sin_top3` antes que nada.** En unlocks, las 4 hipótesis con efecto visible dieron
> vuelta el signo al sacar 3 símbolos.

> **Contar el n POST-JOIN y calcular el MDE con la nula real ANTES de estimar.**
> Convierte un "no se pudo medir" en "no está". Es lo que cerró unlocks y la cola.

> **La nula de look-elsewhere es por DESPLAZAMIENTO CIRCULAR, no permutando filas.**
> Barajar destruye la autocorrelación y hace la nula demasiado fácil.
> Implementado en `micro.py --nula N` y `leadlag.py --nula N`.

> **Si el test es contra un prior contaminado, escribirlo TWO-SIDED.**

> **Generar ancho, no pre-filtrar.** Las compuertas son sobre conclusiones, no sobre
> explorar. El harness mata barato; el prior propio no aporta.

> **Ojo con el denominador entre corridas.** La composición depende del período: el
> mismo replay da ratio 0,84 en 56 días y 0,35 en 7. Entre ventanas distintas solo son
> comparables **multiplicadores**.

---

## 4. LAS CINCO DIRECCIONES

Ordenadas por lo que yo haría. Las dos primeras salen directamente de lo medido hoy y
no necesitan datos nuevos.

### 4.1 Ser el MAKER, no el taker — ~~★ la que yo haría primero~~ **CERRADO 2026-08-27**

> **Medido y cerrado, spot y futuros.** Mediana de `RS_bal(60s)` sobre 20 pares =
> **0,0133%**, contra un fee de maker de 0,0750% (spot con BNB) y 0,0200% (futuros):
> no cruza ninguno. `sin_top3` lo lleva a **0,0004%**, los dos estimadores de mid
> coinciden, y `p` de bloques = 1,0000. **No es que la selección adversa se coma el
> spread: el spread capturable ni llega al fee.** Ver `banco/PREREGISTRO_MAKER.md`.
> Lo de abajo queda como registro de por qué se probó.

**La idea.** Hoy se midió que en la cola el spread es 0,11–0,15% y el slippage hace
inviable *cruzar*. Pero ese costo es **el ingreso de otro**. Poner órdenes límite
invierte el signo: en vez de pagar el spread, lo cobrás. Es **no direccional** y **con
mecanismo** — el único perfil que funcionó en este repo (ver 2.2).

**A favor.** La herramienta ya está construida: `banco/libro.py` mide spread, profundidad
y cuánto se mueve entre snapshots, por símbolo. Es exactamente el insumo que necesita
una evaluación de market making. Y el terreno que hoy quedó descartado para el taker
(la cola) es justo donde el spread es más ancho.

**Lo que lo mata, y hay que medirlo PRIMERO: la selección adversa.** Te llenan cuando
tenés razón el otro. Un MM cobra el spread pero compra justo antes de que baje. La
pregunta cuantitativa: **¿el retorno posterior a un fill condicional supera al spread
cobrado?** Si no, el spread es una ilusión contable.

**Primer paso concreto, medible sin poner una orden:**
1. Bajar `aggTrades` de Binance (endpoint `/api/v3/aggTrades`, o los dumps de
   data.binance.vision) para ~20 pares de distintas bandas, 1–2 semanas.
2. Cada trade trae `m` (si el comprador fue el maker). Reconstruir: para cada trade,
   el retorno del midprice a +1s, +10s, +60s.
3. **Selección adversa = retorno medio del mid después de un fill del lado que te
   tocaría.** Comparar contra el medio-spread cobrado.
4. Regla de parada, escribirla antes: si `medio_spread − selección_adversa(60s) <= 0`
   en la mediana de los pares, el market making no paga y se cierra.

**Costo:** bajo en datos (aggTrades de 20 pares × 2 semanas), medio en análisis.
**Ojo:** esto mide el *piso*. Un MM real además tiene riesgo de inventario y
competencia de latencia, que solo empeoran el número. Si el piso ya es negativo, cerrado.

### 4.2 Futuros en vez de spot — **prior MUY bajado 2026-08-27**

> El posicionamiento de futuros (OI y ratios long/short, 5 años) se metió en un
> ranking transversal en las dos direcciones: **0 de 276**. No cierra 4.2 literalmente
> —falta re-correr sobre *klines* de futuros con su costo— pero las señales de
> posicionamiento no ordenaron ni de un lado ni del otro, que era la mitad del caso.

**La idea.** Todo el repo mide un juego **long-only sobre spot**. Dos cosas cambian con
perpetuos:
- **El fee taker es ~0,05%/lado contra 0,10% del spot** — la mitad del término de fee de
  toda la tabla de 2.3.
- **Se puede shortear de verdad.**

**Por qué importa el short.** El repo encuentra señales del lado corto una y otra vez y
**no tiene instrumento para operarlas**: lead-lag dio 149 cortas y 0 largas; funding
sentimiento dio dosis-respuesta monótona en el short (+3,86pp); OI shock bajista dio
+7pp vs dardo pareado. Es un desajuste estructural entre lo que se mide y lo que se
podría operar.

**En contra, y hay que decirlo de entrada:** esas señales murieron por **consistencia**
(OI shock p 0,92; funding p_bloques 0,1550), no por costo. Abaratar el trading **baja la
vara, no resucita nada**. Además el perpetuo cobra/paga funding, que hay que meter en el
costo, y la fuente ya existe (`banco/funding.py`, `banco/metricas.py` con OI y
posicionamiento cada 5 min desde 2020).

**Primer paso:** re-correr el lote estándar (`lote.py`) sobre klines de **futuros** con
el costo de futuros y las dos direcciones, y comparar contra el mismo lote en spot. Es
barato: la maquinaria está entera, solo cambia la fuente de klines y el costo.

**Regla de parada:** si el lote de futuros no da MÁS sobrevivientes que el de spot
sobre la misma ventana y la misma nula circular, el cambio de instrumento no aporta.

### 4.3 Datos on-chain

**La idea.** La única clase de información **no derivada del precio** que el repo nunca
tocó: flujos de entrada/salida de exchanges, oferta de stablecoins, actividad de wallets.

**A favor.** Mecanismo plausible (monedas saliendo de exchanges = menos oferta vendedora)
y es información genuinamente nueva. La maquinaria de estudio de evento esparcido ya
está construida y validada (`banco/test_unlocks.py`: `preparar()`, `_p_permutacion()`,
`_ic_bootstrap()`).

**En contra.** El acceso a datos es el cuello: las APIs buenas (Glassnode, Nansen) son
pagas; las gratis tienen granularidad pobre o pocos activos. Y el n útil por activo va a
ser chico, que es **exactamente la trampa de unlocks**.

**Antes de escribir una sola regla:** contar el **n post-join** y calcular el **MDE con
la nula real**. Unlocks murió con 1.040 eventos y el MDE fijado en 6,6 pp/década. Si
on-chain no llega a un n comparable, se cierra antes de empezar.

### 4.4 Volatilidad de alts

**La idea.** Vender volatilidad funcionó 5,3 años y solo se midió en **BTC/ETH**, donde
Deribit es eficiente. En alts la competencia es mucho menor, así que la prima debería
estar menos comprimida.

**A favor.** Es la única idea que extiende **lo único que funcionó**, y con el mismo
mecanismo. `opciones/iv_rv.py` ya existe, cacheado y re-corrible.

**En contra, y es serio:** **puede que el instrumento no exista.** No hay mercado de
opciones líquido para alts — Deribit es básicamente BTC/ETH. Sin opciones, "vender
volatilidad" hay que sintetizarlo, y el repo ya cerró la vía de sintetizarlo con
órdenes stop (`[[project-dos-puntas-descartado]]`: regalás k·ATR por trade, falta
convexidad).

**Primer paso, y es de viabilidad, no de estadística:** averiguar si existe algún
mercado de opciones de alts con volumen real (Deribit listó algunas; hay OKX y Bybit).
**Si no hay instrumento, se cierra ahí** y no se gasta una sesión en medir una prima que
no se puede cobrar.

### 4.5 Eventos de listado en Binance

Último ítem vivo del handoff anterior. Timestamp exacto, efecto documentado en la
literatura, y no sale del precio. La maquinaria de evento esparcido sirve tal cual.

**En contra:** no hay endpoint (se scrapea el blog de Binance o se usa un dataset de
terceros) y **el n va a ser chico** — pocos listados por mes. El handoff anterior ya lo
marcaba como "exactamente la trampa de unlocks". **Contar el n post-join y el MDE antes
de escribir la regla no es opcional.**

---

## 5. Inventario de herramientas reutilizables

| archivo | qué hace |
|---|---|
| `banco/klines.py` | `load_panel(start, end, n, tf, pin=, full=, workers=)`. `pin` congela el universo |
| `banco/primer_toque.py` | `tabla()` → una fila por (par, entrada) con primer toque ±8%/30d |
| `banco/lote.py` | `lote(T, {nombre: máscara})` → **seis compuertas cableadas** + FDR |
| `banco/leadlag.py` | features de lead-lag por grupo + `nula()` circular. Patrón a copiar |
| `banco/micro.py` | 12 features intra-vela + test condicional a volatilidad |
| `banco/libro.py` | **spread y profundidad reales** del order book, camina el libro |
| `banco/costos.py` | Corwin-Schultz y Roll — **documentado como NO usable a 1h** |
| `banco/test_unlocks.py` | estudio de evento esparcido: permutación + bootstrap de símbolos |
| `banco/metricas.py`, `funding.py` | OI y posicionamiento de futuros cada 5 min desde 2020 |
| `opciones/iv_rv.py` | DVOL de Deribit vs realizada — el estudio de vender volatilidad |

**Las seis compuertas de `lote.py`** (cableadas, no son sugerencias): umbral,
FDR q=0,10, pareado (no selección-de-moneda), `sin_top3`, `sin_top1`, consistencia
semanal ≥60%. Veredicto por default: CERRADA.

**Ventana OOS virgen: 2024-08-01 → 2025-08-01.** Declarada en `PREREGISTRO_ANCHO.md` y
**nunca mirada**, porque nunca sobrevivió nada que promover. Sigue disponible.

---

## 6. Lo que este repo mide, dicho sin vueltas

El mercado es eficiente respecto de la información que hay en el precio, en las 200
monedas más líquidas, a horizonte de días a semanas. Eso no es un fracaso del método:
es el resultado correcto para el segmento más competido.

Lo que se agregó hoy: **y los costos reales son 1,5× a 6,3× lo que se venía asumiendo**,
lo cual cierra el escape hacia la cola ilíquida — ahí las features son más grandes, pero
el costo lo es más.

Cambiar la respuesta requiere cambiar **la información** (4.3), **el lado de la orden**
(4.1), **el instrumento** (4.2, 4.4) o **el juego** (4.1 y 4.4 otra vez: no
direccional, con mecanismo). Iterar más sobre features de precio sube la vara del azar,
no baja la del hallazgo.
