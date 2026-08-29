# HANDOFF — las direcciones que quedan

> Escrito el **2026-08-28** al cerrar las corridas 5, 5b, 6 y 7.
> **Actualizado el 2026-08-29:** la corrida 8 cerró la volatilidad de alts (ahora §2.0). Quedan
> **dos direcciones y una medición**.
>
> **Este es el punto de entrada.** `HANDOFF_SIGUIENTE.md` es el registro completo de lo
> cerrado (§0.5 a §0.8 tienen el detalle de la sesión del 28); los anteriores
> (`HANDOFF_CIERRE.md`, `HANDOFF_SENALES.md`, `HANDOFF_BASIS.md`) son históricos.
>
> Se abre en frío: la §0 es operativa, la §1 dice dónde estamos, la §2 son las direcciones,
> la §3 las reglas que no se negocian.

---

## 0. Cómo arrancar (verificado, no de memoria)

```bash
cd banco
py -3.13 -u ranking.py --nula                  # el harness bueno: MDE primero
py -3.13 -u correr_onchain.py --nula           # patrón de "contar el n ANTES"
py -3.13 -u libro_perp.py --top 200 --reps 3   # costos reales, spot y perp
py -3.13 -u correr_velas.py --tf 1d            # eventos con control por barra
```

**Gotchas, todos pisados:**

- Siempre `py -3.13`, nunca `python`. Siempre `-u`.
- `$env:PYTHONIOENCODING = "utf-8"` antes de correr, o revienta con cp1252.
- Los procesos se llaman `python3.13`. **`ps` de Git Bash NO los ve** — usar
  `Get-Process python3.13` desde PowerShell. Un `ps` vacío no significa que murió.
- Corridas pesadas **de a una**. La de velas a 1h son ~54 min.
- **Heredocs de bash con tildes/backticks fallan.** Escribir archivos con la herramienta
  de escritura directa, o con un script de Python.
- **`ta` calcula Bollinger con `ddof=0`.** Verificar numéricamente antes de reimplementar
  cualquier indicador suyo.
- **Nunca `groupby.apply` con lambdas** sobre miles de grupos (2700× de diferencia
  medida).
- El writer de parquet falla en esta máquina: el caché del banco cae a `.csv`.

**Cachés (grandes, no borrar, todos gitignoreados):** `banco/.kline_cache/` (1,7 GB, ahora
con **1d y perp**), `.backtest_cache/` (1,77 GB), `banco/.funding_cache/`,
`.metrics_cache/`, `.unlock_cache/`, **`.onchain_cache/`**.

**Rama:** `banco/primer-toque`. ⚠️ Arrastra 3 commits que tocan el swing en producción
(`swing/backtest.py` +753 líneas, `screener.py`, `config.json`). Si se mergea, se revisan
aparte. `main` tiene el radar corriendo y no toca nada de esto.

---

## 1. Dónde estamos

### 1.1 Las tres cosas con FECHA (esto es lo único que corre solo)

| fecha | qué | comando |
|---|---|---|
| **~8 sep** | primer chequeo del radar | `cd radar && py -3.13 -u medir.py` |
| **~14 oct** | el chequeo que decide para un efecto de la mitad | idem |
| **19 oct** | forward test del **fade** (`HANDOFF_CIERRE.md`) | `cd fade && py -3.13 evaluar.py` |

`radar/HANDOFF_FORWARD_TEST.md` tiene la decisión de cada resultado, escrita antes de que
existiera un dato. **No aflojarla.**

### 1.2 El mapa, comprimido

Diez familias estándar de confirmación de dirección. **Nueve medidas, todas en cero:**

| familia | veredicto |
|---|---|
| Momentum transversal | 4.140 hipótesis, **0** |
| Momentum de serie / trend | ingredientes medidos, **0** |
| Reversión / fade | **la única viva** — forward test 19-oct |
| Carry (funding) | **0** como señal; y **cash-and-carry cerrado 28-ago**: media −7,23%/año en líquidos (§2.4.A) |
| Order flow / microestructura | 0 de 192; maker cerrado por comisión |
| Posicionamiento / contrarian | 0 de 276 |
| Régimen | 7 detectores × 22 trimestres, ninguno gateable |
| On-chain | 0 de 420 (corrida 6) |
| Estudios de evento | unlocks cerrado; **listados = 2.1** |
| **Patrones de velas** | 0 de 660 (corrida 7) — la canónica acierta **50,0%** |
| ML / no lineal | techo condicional medido |
| **Patrones de gráfico** | **← nunca tocada (2.2)** |

**Lo único que funcionó, dos veces, es no direccional:**
- **Vender volatilidad** (+20,96%/año en BTC, 5,3 años, mecanismo real). Se cerró en
  BTC/ETH por no superar el piso de stablecoins, y la **corrida 8 cerró la extensión a
  alts (29-ago)**: el instrumento existe y el spread es barato, pero **no hay historia de
  implícita para medir la prima** — MDE 39%/año en SOL contra un umbral de 10%, y **BTC
  con la misma ventana da 27,1%**. Detalle en §2.0.
- **Magnitud** (el radar): sobrevive en **cinco** diseños distintos y cuatro regímenes.
  Sigue **sin instrumento** para cobrarse.

### 1.3 Lo que las sesiones del 28 y 29-ago agregaron al método

Seis cosas que cambian cómo se corre lo que sigue:

1. **El universo se filtra por CLASE DE ACTIVO, no solo por volumen.** `base200` arrastra
   7 stablecoins/FX y 9 acciones tokenizadas: 9,1% del universo que se lleva **37-44% de
   los cupos** de cualquier ranking que elija "lo quieto". Usar la lista `FUERA` de
   `correr_velas.py`.
2. **Cuando cambian tres cosas a la vez, se corren los paneles intermedios.** Un salto de
   +0,41 ATR parecía ser el instrumento y era el universo.
3. **La reserva OOS 2024-08 → 2025-08 ya NO está virgen** para el ranking transversal
   (corridas 3, 4 y 6 la contienen). Sigue disponible solo para `lote.py` y
   `lote_ancho.py`.
4. **La tabla de costos de spot subestima al perpetuo por 2×.** Perp: 12-18 bps a $1k,
   15-32 bps a $10k. Spot: 24-34 y 30-62. Medido en el mismo instante, apareado por
   símbolo (`libro_perp.py`).
5. **Sumar monedas NO agrega potencia: la vol de cripto es un factor.** ρ = +0,92 entre el
   P&L mensual de la straddle de BTC, ETH, SOL y XRP → **4 subyacentes son 1,07
   independientes** (corrida 8). Cualquier plan futuro que diga "gano n poniendo más
   activos" tiene que medir esa ρ primero.
6. **Un número que contradice una estructura de mercado conocida es un bug, no un
   hallazgo.** La corrida 8 midió USD 628.000 millones/día de opciones de BTC en OKX —seis
   veces Deribit— y eso, no el código, fue lo que delató un error de unidades de 100×.

---

## 2. LAS DIRECCIONES QUE QUEDAN

Eran tres. La **§2.0 se cerró el 29-ago** y queda arriba porque lo que la mató es un
resultado de método que aplica a todo lo que venga. Quedan **dos direcciones** (§2.1 y
§2.2) y **una medición** (§2.4.D).

Ordenadas por **lo que yo haría**, y el criterio no es el prior: es **cuánto cuesta
cerrarlas**. Las dos tienen prior bajo. La primera es casi toda adquisición de datos; la
segunda necesita construir un detector.

**Y §2.4**, que no es una dirección sino el mapa de **los otros negocios** —dónde sí se gana
plata en este mercado y por qué está afuera de lo probado—, con los dos que resultaron
medibles ya medidos y **uno que sigue abierto y que ahora va antes que §2.2**.

---

### 2.0 Volatilidad de alts — **CERRADA 29-ago (corrida 8)**

**La idea era la mejor que quedaba:** vender volatilidad es lo único que este repo encontró
que funcionó de verdad (+20,96%/año en BTC, mecanismo real), y se había medido **solo en
BTC/ETH**, donde el premio ya se arbitró. En alts la competencia es menor, así que la prima
debería estar menos comprimida. Era la única dirección que **extendía lo que funcionó**.

**Se cerró en una tarde, y no por donde se esperaba.** El preregistro
(`banco/PREREGISTRO_OPCIONES.md`) puso tres compuertas. Las dos que se temían pasaron:

| compuerta | lo que se temía | lo que pasó |
|---|---|---|
| **(A)** ¿existe el instrumento? | que no hubiera mercado | **PASA**: 6 alts con opciones listadas; SOL, XRP y HYPE cumplen vol ≥ $1M/24h y OI ≥ $5M |
| **(B)** ¿se puede cruzar? | spreads impagables | **PASA por un orden de magnitud**: ATM 2,3-4,5% relativo, o sea 1-2% de la prima |
| **(C)** ¿se puede **medir**? | — | **FALLA por 4×** |

**Lo que la mató fue la potencia, y el número que lo cierra es sobre BTC, no sobre las alts.**

- **El DVOL de Deribit solo existe para BTC y ETH.** La única historia de implícita para
  alts es el índice de Bybit: **18 meses para SOL, 10 para XRP, 7 semanas para HYPE.**
- Straddles mensuales **no solapadas** → n = 18, 10 y 0 meses. MDE **39,0%**, **56,5%** e
  **infinito** por año, contra un umbral preregistrado de 10%.
- **La calibración es lo incontestable: BTC con esos mismos 18 meses da MDE 27,1%/año.** Su
  efecto está medido y es conocido, y **tampoco sería detectable**. No es que las alts sean
  especiales — es que el estimador tiene una señal/ruido de ~1/5 por mes.
- Para un MDE de 10%/año harían falta **11 años en BTC, 23 en SOL, 27 en XRP**. Con la σ del
  DVOL largo, 17 años en BTC.

**Las tres salidas están medidas y tapadas** (§(C) del preregistro): delta-hedgear no baja
la varianza porque la σ usada *ya es* la del P&L hedgeado; esperar no alcanza; y poolear
subyacentes **es la peor de las tres** — ver la regla nueva en §3, ρ = +0,92.

**Lo único que la reabriría:** una fuente con **historia larga** de implícita en alts. No es
un problema de método: **el dato no existe.**

---

### 2.1 Eventos de listado en Binance  ★ la primera

**La idea.** Un listado tiene **timestamp exacto**, efecto documentado en la literatura, y
**no sale del precio**. La maquinaria de estudio de evento esparcido sirve tal cual:
`banco/test_unlocks.py` (`preparar()`, `_p_permutacion()`, `_ic_bootstrap()`).

**En contra, y es lo mismo que mató a unlocks:** no hay endpoint (se scrapea el blog de
anuncios de Binance o se usa un dataset de terceros) y **el n va a ser chico** — pocos
listados por mes.

> **Antes de escribir una sola regla: contar el n POST-JOIN y calcular el MDE con la nula
> real.** Es lo que convirtió un "no se pudo medir" en un "no está" en unlocks (1.040
> eventos, MDE 6,6 pp/década) y en la cola ilíquida, y lo que dejó a la corrida 6 poder
> concluir en serio (41 activos, 257 semanas, MDE ±0,065 — igual que el que cerró
> derivados).
>
> **Regla de parada:** si el n post-join no alcanza un MDE comparable al de las corridas
> que sí concluyeron, el veredicto es **"no se pudo medir"** y se cierra sin gastar más.
> `correr_onchain.py --nula` es el patrón exacto a copiar.

**Un detalle que importa y que la corrida 7 dejó armado:** el estimador correcto para un
evento es el de `correr_velas.py` — **control POR BARRA**, no por símbolo. Un listado
ocurre en un momento del mercado, y si el mercado subía ese día, el evento "funciona" sin
que el listado aporte nada. `lote.py` aparea por símbolo y nunca neutralizó ese término.

**Costo:** medio, y casi todo es adquisición de datos.
**Prior:** bajo-medio. El efecto está documentado, pero es el más publicado del mundo y
lleva años de arbitraje.

---

### 2.2 Patrones de gráfico — la décima familia, la única sin tocar

**La idea.** Hombro-cabeza-hombro, triángulos, banderas, cuñas, dobles techos y pisos.
Es la **única familia estándar de confirmación de dirección que el repo nunca midió**, y
quedó explícitamente fuera del alcance de la corrida 7 (§6 de `PREREGISTRO_VELAS.md`).

**Por qué no es lo mismo que las velas.** Un patrón de velas es una conjunción sobre 2-3
barras — una función local del OHLC. Un patrón de gráfico es **estructura sobre decenas de
barras**: requiere detectar pivotes, ajustar líneas de tendencia, y decidir tolerancias.
Esa forma funcional no está en ninguna de las corridas anteriores.

**Por qué va última, y hay que ser honesto:**

1. **Hay que construir el detector**, y ahí vive el peligro. Cada patrón tiene ~4
   parámetros libres (cuántas barras de lookback, qué tolerancia para "dos techos
   iguales", cuánto puede desviarse la línea, qué cuenta como ruptura). **Eso es una
   máquina de fabricar falsos positivos**, y hay que preregistrarlos igual que se
   preregistraron los umbrales de `velas.py`.
2. **El prior bajó** después de la corrida 7. La familia adyacente —la que comparte la
   premisa de que la forma del gráfico anticipa dirección— dio la dirección canónica
   acertando **exactamente 50,0%** contra su propio espejo.
3. **Es subjetiva por construcción.** Dos implementaciones razonables de "hombro-cabeza-
   hombro" detectan conjuntos distintos, y no hay una definición canónica como la hay para
   un envolvente.

**Cómo lo haría, si se hace:**

- **Preregistrar los parámetros del detector ANTES**, con la misma disciplina que
  `velas.py`: cada tolerancia escrita en el código con su valor canónico y sin tocarla
  después. Si un umbral se ajusta viendo el resultado, la corrida no vale.
- **Declarar la dirección de cada patrón antes de medir.** La corrida 7 mostró por qué:
  tres de sus cinco mejores brazos estaban *invertidos*, y elegido el signo después, los
  cinco contaban como aciertos.
- **Estimador con control por barra**, reutilizando `correr_velas.py` tal cual: solo hay
  que reemplazar `velas.patrones(df)` por el detector nuevo. Todo lo demás —compuertas,
  FDR sobre el lote entero, `sin_top3`, bootstrap de bloques semanales, el umbral de "no
  se pudo medir"— ya está escrito y validado.
- **Dos resoluciones, 1d y 1h**, por la misma razón: los patrones de gráfico se definieron
  en diarias.
- **Un control obligatorio:** un detector con **los mismos parámetros pero pivotes
  barajados**. Si el patrón real no se separa de su versión con la estructura destruida,
  lo que se detectó es ruido con forma.

**Costo:** el más alto de lo que queda. El detector son ~300 líneas y el diseño de sus
parámetros es la mitad del trabajo.
**Prior:** el más bajo de lo que queda.

---

### 2.4 Los otros negocios — dónde SÍ se gana plata, y cuál de ellos es medible acá

Las nueve familias en cero son todas de **información pública, gratis, sin ventaja de
latencia, sobre las 200 monedas más líquidas**. Donde efectivamente se gana plata en este
mercado está afuera de eso: latencia y co-location, flujo privado, arbitraje entre venues
con infraestructura, market making a escala con rebates y modelo de inventario, e
información no pública.

**Ninguna de esas es "una feature que al screener se le escapó".** Son negocios distintos,
con otro costo de entrada — y decirlo así no es resignación, es la diferencia entre
"buscar más" y "cambiar de negocio". Pero dos de ellas tenían una pregunta **medible con
lo que ya está en disco**, así que se midieron el 28-ago en vez de quedar como intuición.

#### A. Cash-and-carry / cosechar funding — **MEDIDO Y CERRADO (28-ago)**

**La idea, y por qué merecía el intento.** Comprar spot y shortear el perpetuo del mismo
activo es **delta-neutral**: no apuesta dirección, y cobra el funding. Tiene **mecanismo**
(el funding es lo que paga la demanda de apalancamiento) y es **no direccional** — el
único perfil que funcionó dos veces en este repo. El cierre anterior era sobre **BTC solo**
(+3,35%/año, bajo el piso de stablecoins); nadie había mirado la sección cruzada.

**Medido sobre 253 perps, 2025-08 → 2026-08, funding anualizado que cobraría el short:**

| universo | n | mediana | **media** | p90 |
|---|---|---|---|---|
| `base200` (líquidos) | 172 | −1,78% | **−7,23%** | +3,68% |
| el resto (cola) | 81 | +1,59% | −3,17% | +15,99% |
| todos | 253 | −1,46% | −5,93% | +7,06% |

> **En los nombres líquidos la media es NEGATIVA: −7,23%/año. El cash-and-carry pierde
> plata en la moneda mediana antes de pagar un solo costo.**

**Y la selección tampoco lo salva.** Persistencia entre semestres: correlación **+0,253**.
El **top-20 por funding del primer semestre rinde +1,57%/año en el segundo** (contra un
universo de −5,92%). O sea que elegir agrega ~7,5 pp de valor relativo, pero el absoluto
—+1,57%— queda **debajo del piso de stablecoins (~4-5%)** y muy debajo del costo:

- una vuelta de cash-and-carry son **4 patas** (comprar spot, shortear perp, deshacer las dos),
- ≈ 24-34 bps de spot + 12-18 bps de perp = **40-50 bps por ciclo**,
- rebalanceando mensual, **~5-6%/año solo de costo**.

**Veredicto: cerrado.** +1,57% de rendimiento forward contra 5-6% de costo y un piso libre
de riesgo de 4-5%, con riesgo de liquidación y de base encima.

> ⚠️ **Y deja una trampa de método que hay que llevarse:** *el funding que cobra una
> posición es la MEDIA, no la mediana.* El carry mediano por barra da **+0,0161%/24h**
> (≈ +5,9% anualizado) y parece viento de cola; la media por símbolo es **−7,23%**. La
> diferencia es que el funding se va muy negativo **de golpe** —a los largos les pagan
> fuerte en los picos— y una posición se come **todos** los pagos, no la mediana de ellos.
> Cualquier cosa que se cobre pago a pago se evalúa con la media.

#### B. Market making sobre el perpetuo — **muerto por aritmética, NO re-medirlo**

`PREREGISTRO_MAKER.md` midió el spread realizado sobre **`aggTrades` de spot** y dejó
escrito que, *si* cruzaba el fee de futuros (0,0200%), el paso siguiente sería re-medir
sobre `aggTrades` de futuros. **No cruzó** (0,0133%), así que la regla nunca se activó.

Tentaba re-abrirlo porque la corrida 5b midió que el spread cotizado del perp en la banda
51-200 es **0,024-0,025%**, o sea *arriba* del fee de maker. Pero la cuenta lo mata sin
gastar una corrida:

- en spot, el spread realizado fue **0,0133% sobre un cotizado de 0,067-0,070%** → una
  tasa de captura de **~19%** (el resto se lo come la selección adversa),
- 19% del cotizado del perp (0,024%) son **~0,005%**,
- contra un fee de maker de **0,0200%**. No cruza, y no cruza por 4×.

**No re-medir.** Si alguien quiere igual, que primero muestre por qué la tasa de captura
del perp sería 4× la del spot.

#### C. Lo que queda genuinamente afuera, y por qué

| negocio | por qué no es accesible acá |
|---|---|
| latencia / co-location | requiere infraestructura y capital; se compite en microsegundos |
| flujo privado (internalización, OTC) | no es información que se pueda comprar suelta |
| arbitraje entre venues con infra | el spread existe pero se cierra a velocidad de máquina |
| market making a escala con rebates | los rebates viven en tiers de volumen inalcanzables |
| información no pública | fuera de alcance, y no solo técnico |

**No son "lo mismo pero más difícil": tienen otro costo de entrada y otra unidad de
competencia.** Meterlas en el mismo handoff que las direcciones abiertas sería confundir
categorías.

#### D. La única de esta lista que sigue siendo MEDIBLE y no se hizo

**¿Cuánto dura una dislocación entre venues?** El negocio de latencia es inaccesible, pero
tiene una pregunta empírica barata: *¿las diferencias de precio entre Binance, OKX y Bybit
se cierran en microsegundos, o hay una cola lenta?* Si una dislocación de más de ~30 bps
persiste **más de 5 segundos** de forma recurrente, es capturable **sin** co-location.

- **Cómo:** REST público de los tres venues, el mismo par, muestreado cada ~1s durante
  varios días. No hace falta websocket ni cuenta.
- **Qué medir:** distribución del |spread entre venues| y, condicional a que supere el
  costo de cruzar (~40-60 bps, dos venues), **cuánto tarda en volver bajo ese umbral**.
- **Regla de parada, escribirla antes:** si la mediana de duración de las dislocaciones
  que superan el costo es **menor a 2 segundos**, el negocio es de latencia y **se cierra**
  — no hay versión lenta.
- **Ojo con dos artefactos** que van a inflar el resultado si no se controlan: los precios
  de venues distintos **no están sincronizados** (hay que comparar timestamps del servidor,
  no de llegada), y un par puede estar **halted o en subasta** en un venue, lo que fabrica
  dislocaciones enormes que no son operables.

**Prior:** bajo — es la parte más obviamente competida del mercado. **Costo:** bajo, y es
una **medición**, no una construcción. Va **después de §2.1 y antes de §2.2**: es lo segundo
de la lista.

---

## 3. Las reglas que no se negocian

> **La regla de parada se escribe ANTES de mirar.** Si se afloja después de ver un número,
> el experimento no vale.

> **Contar el n POST-JOIN y calcular el MDE con la nula real ANTES de estimar.** Convierte
> un "no se pudo medir" en un "no está". Cerró unlocks, la cola ilíquida, y habilitó la
> conclusión de la corrida 6.

> **La dirección de una hipótesis se declara antes de medirla.** Si se elige el signo
> después, todo acierta. Medido: 3 de los 5 mejores brazos de la corrida 7 estaban
> invertidos respecto de lo que el patrón afirma.

> **El p que decide es el de BLOQUES**, no el binomial. La brecha entre los dos *es* el
> autoengaño. Casos medidos: micro 1,1e-36 → 0,3845; lead-lag 2,3e-44 → 0,1735.

> **El n efectivo son las SEMANAS, no las entradas.**

> **El control va POR BARRA, no por símbolo.** El término de mercado es el sesgo principal
> de cualquier test de eventos o rankings, y aparear por símbolo no lo toca.

> **`sin_top3` antes que nada.** En unlocks, las 4 hipótesis con efecto visible dieron
> vuelta el signo al sacar 3 símbolos.

> **Todo se corre a DOS costos.** Un sobreviviente que solo vive al costo barato no cuenta
> (corrida 5). Y si el costo no está medido para ese instrumento, **medirlo** antes de
> declarar el cierre (corrida 5b).

> **El universo se filtra por clase de activo, no solo por volumen.**

> **Lo que se cobra pago a pago se evalúa con la MEDIA, no con la mediana.** El funding
> mediano por barra da +5,9% anualizado y parece viento de cola; la media por símbolo es
> **−7,23%**. Se va muy negativo de golpe, y una posición se come todos los pagos (§2.4.A).

> **Cuando cambian varias cosas a la vez, se corren los paneles intermedios.**

> **La potencia se calcula sobre la UNIDAD DEL ESTIMADOR, no sobre las filas del dato.**
> La serie de implícita de Bybit tiene ~720 puntos por mes y **ninguno** es independiente:
> una straddle mensual no solapada da **un** dato por mes. Corrida 8: 18 meses de SOL →
> MDE 39%/año.

> **Antes de decir "sumo activos y gano potencia", MEDIR la correlación.** ρ = +0,92 entre
> el P&L de la straddle de BTC, ETH, SOL y XRP: `n_ef = k/(1+(k−1)ρ)` da **1,07 de 4**. La
> barra se angosta 1,03×, no 2×. La volatilidad de cripto es un solo factor.

> **Un estimador puede ser demasiado ruidoso para cualquier muestra realista, y eso es un
> veredicto.** Vender una straddle mensual tiene señal/ruido ~1/5 por mes: para un MDE de
> 10%/año harían falta **11 años en BTC y 23 en SOL**. "No se pudo medir" no siempre es
> culpa del n — a veces es culpa del estimador, y hay que decir cuál de las dos.

> **Un número que contradice una estructura de mercado conocida es un bug hasta que se
> demuestre lo contrario.** No fue el código el que delató un error de unidades de 100× en
> la corrida 8: fue que el resultado ponía a OKX seis veces arriba de Deribit.

> **Generar ancho, no pre-filtrar.** Las compuertas son sobre conclusiones, no sobre
> explorar. El prior propio no aporta; el harness mata barato.

---

## 4. Herramientas — qué usar para qué

| archivo | qué hace |
|---|---|
| **`banco/ranking.py`** | **el harness bueno.** Ranking transversal por barra: sin corte pooled, sin trampa de `SEM_N_MIN`, control por barra, sin solape. Seis compuertas + FDR |
| **`banco/correr_velas.py`** | **el harness de EVENTOS.** Control por barra para máscaras booleanas. Para §2.2 solo hay que cambiar el detector |
| `banco/velas.py` | 15 patrones de velas con sus umbrales canónicos fijados. El patrón a copiar para preregistrar un detector |
| `banco/klines.py` | `load_panel(..., tf=, full=, pin=, syms=, mercado=)`. **`mercado="fut"`** trae perpetuos; `tf="1d"` trae diarias |
| `banco/onchain.py` | CoinMetrics Community (gratis, sin key). Une por `AssetEODCompletionTime`: **sin lookahead y sin lag fijo** |
| `banco/futuros.py` | funding alineado al tablero. Entra en el **retorno**, no en el costo |
| **`banco/libro_perp.py`** | **costos reales del libro**, spot y perp en el mismo instante y apareados |
| `banco/test_unlocks.py` | estudio de evento esparcido: permutación + bootstrap de símbolos. **La base para §2.1** |
| `opciones/iv_rv.py` | DVOL de Deribit vs realizada futura. Cerró BTC/ETH; **su barra de error está ahora medida en la corrida 8** |
| **`opciones/viabilidad.py`** | **foto de los 3 venues de opciones**: volumen, OI y spread ATM por subyacente, todo a nocional USD. El patrón de "medir si el instrumento existe ANTES de medir el efecto" |
| **`opciones/potencia.py`** | **n, σ, MDE y años necesarios** para un estimador dado, con calibración contra un efecto conocido y la ρ que decide si poolear sirve |
| `banco/correr_onchain.py` | el patrón de `--nula`: contar el n y el MDE antes de nada |
| `banco/libro.py` | camina el libro. `--mercado fut` para perpetuos |

**Los preregistros** (`banco/PREREGISTRO_*.md`) tienen las reglas escritas antes de cada
corrida y los resultados debajo de la línea. `TRANSVERSAL` (4 corridas), `FUTUROS` (5 y
5b), `ONCHAIN` (6), `VELAS` (7).

---

## 5. Lo que este repo mide, dicho sin vueltas

El mercado es eficiente respecto de la información que hay en el precio, en las 200 monedas
más líquidas, a horizontes de horas a semanas. Después de siete corridas y nueve de las
diez familias estándar, eso ya no es un resultado provisorio.

Lo que se agregó el 28-ago, y que cierra tres escapes que quedaban abiertos:

- **No es el instrumento.** El perpetuo ordena igual que el spot: +0,004 ATR de diferencia
  media sobre 140 brazos. Abaratar el trading baja la vara, no resucita nada.
- **No es la fuente.** On-chain —la única clase de información que no sale del mercado—
  da 0 de 420 con la misma potencia que cerró derivados.
- **No es la resolución.** A velas diarias, con 253 semanas y un MDE más fino que el de 1h,
  tampoco hay nada.

Y el screener tiene su propio defecto, medido y separado del anterior: **compra +3,12 ATR
después de que el movimiento ya pasó**, y devuelve −0,94 en las 48h siguientes. Ningún
scoring lo arregla, porque el daño es común a todas sus alertas.

Y el 29-ago se agregó una cuarta, que es de otra naturaleza:

- **No siempre es que no haya efecto: a veces el efecto no es medible con nada.** La
  corrida 8 encontró que para vender volatilidad en alts **el instrumento existe y es
  barato de cruzar** —lo contrario de lo que el prior decía— y aun así cerró, porque la
  historia de implícita da MDE 39%/año contra un umbral de 10%. **BTC, con esa misma
  ventana, da 27,1%.** Es la primera vez en el repo que una dirección muere por el
  **estimador**, no por el mercado ni por el dato.

**Lo que sí sobrevive todo lo que se le tire es la MAGNITUD** — cinco diseños, cuatro
regímenes, 100% de las semanas en el mejor caso. El problema nunca fue medirla: es que
**no hay instrumento para cobrarla**.

Y la corrida 8 le puso un número a esa frase. La única forma conocida de cobrar magnitud es
vender volatilidad; el instrumento **sí existe** en SOL, XRP y HYPE, y el spread **sí deja
margen**. Lo que falta es la historia para saber si pagan. **Ya no es "no hay instrumento":
es "no hay con qué decidir si conviene usarlo".**
