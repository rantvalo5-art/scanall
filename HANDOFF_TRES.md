# HANDOFF — las direcciones que quedan

> Escrito el **2026-08-28** al cerrar las corridas 5, 5b, 6 y 7.
> **Actualizado el 2026-08-30:** las corridas 8 a 13 cerraron **las tres direcciones** y
> además el último hueco de horizonte: volatilidad de alts (§2.0.A), eventos de listado
> (§2.0.B), patrones de gráfico (§2.0.C) y horizontes > 1 semana (§2.0.D).
> **El mapa de direcciones ya no tiene huecos — pero sí tiene una RESOLUCIÓN medida, y hay
> que leerla antes que cualquier otra cosa: está en la §5.**
> No queda ninguna dirección abierta: quedan **una medición corriendo y tres fechas**.
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
| Momentum transversal | 4.140 hipótesis, **0** — y la corrida 13 midió que ese 0 descarta ~32%/año bruto, no 10% |
| Momentum de serie / trend | ingredientes medidos, **0** |
| Reversión / fade | **la única viva** — forward test 19-oct |
| Carry (funding) | **0** como señal; y **cash-and-carry cerrado 28-ago**: media −7,23%/año en líquidos (§2.4.A) |
| Order flow / microestructura | 0 de 192; maker cerrado por comisión |
| Posicionamiento / contrarian | 0 de 276 |
| Régimen | 7 detectores × 22 trimestres, ninguno gateable |
| On-chain | 0 de 420 (corrida 6) |
| Estudios de evento | unlocks cerrado; **listados cerrados 29-ago** (corrida 9): 544 eventos, 266 semanas, MDE 4,2% a 1d |
| **Patrones de velas** | 0 de 660 (corrida 7) — la canónica acierta **50,0%** |
| ML / no lineal | techo condicional medido |
| **Patrones de gráfico** | **0 de 300 brazos** (corridas 11 y 12) — la ruptura pelada le gana a las cinco figuras |

**Lo único que funcionó, dos veces, es no direccional:**
- **Vender volatilidad** (+20,96%/año en BTC, 5,3 años, mecanismo real). Se cerró en
  BTC/ETH por no superar el piso de stablecoins, y la **corrida 8 cerró la extensión a
  alts (29-ago)**: el instrumento existe y el spread es barato, pero **no hay historia de
  implícita para medir la prima** — MDE 39%/año en SOL contra un umbral de 10%, y **BTC
  con la misma ventana da 27,1%**. Detalle en §2.0.A.
- **Magnitud** (el radar): sobrevive en **cinco** diseños distintos y cuatro regímenes.
  Sigue **sin instrumento** para cobrarse.

### 1.3 Lo que las sesiones del 28-ago al 30-ago agregaron al método

Nueve cosas que cambian cómo se corre lo que sigue:

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
   veces Deribit— y eso, no el código, fue lo que delató un error de unidades de 100×. La
   corrida 9 volvió a pisarlo: "Binance deslistó el 0,1% de sus pares" es falso de entrada,
   y era un filtro por nombre en vez de por `status`.
7. **Contar el n NO alcanza, y esto corrige la regla escrita.** La corrida 9 tiene **266
   semanas** —más que las 257 con las que la corrida 6 concluyó— y **no pudo concluir**,
   porque su σ es 20× más grande. El n nunca fue el que decidía: decidía `σ/√n`. Hay que
   calcular las dos cosas, y **decir cuál de las dos falló**, porque una se arregla
   esperando y la otra no.
8. **La resolución que conviene no es la que tiene más barras.** A 1h hay 25× más datos y
   el ruido baja a 0,029 ATR, pero **el costo de una vuelta sube a 0,155 ATR**: un ATR
   horario es ~5× más chico que uno diario, así que el mismo 0,20% pesa 5× más en esas
   unidades. **A 1h manda el costo; a 1d manda el ruido.** Hay que elegir la resolución
   donde el efecto buscado sea grande contra `max(ruido, costo)`, y las dos se calculan
   **antes** (corrida 12).
9. **Y estirar el horizonte tampoco es una palanca: es un empate.** A 90d el costo anual
   cae 13× respecto de 7d, y la precisión cae casi exactamente lo mismo. **El efecto bruto
   detectable se queda en ~31%/año en todo el rango de 7d a 90d** (corrida 13). Antes de
   proponer "probemos a un horizonte más largo", esa cuenta ya está hecha.

---

## 2. LAS DIRECCIONES QUE QUEDAN

Eran tres. **Las tres se cerraron el 29-ago** —§2.0.A opciones, §2.0.B listados, §2.0.C
patrones de gráfico— y quedan arriba porque lo que las mató son resultados de método que
aplican a todo lo que venga.

**Con §2.0.C cierra la décima familia estándar y con §2.0.D el último hueco de horizonte.
El mapa de direcciones ya no tiene huecos — pero sí tiene una RESOLUCIÓN, y hay que
declararla (§5).**

**No queda ninguna dirección abierta.** Lo que queda es:

| qué | dónde | estado |
|---|---|---|
| **dislocación entre venues** | §2.4.D | **una medición**, muestreando |
| **forward test del fade** | §1.1 | 19-oct, decisión escrita de antemano |
| **chequeos del radar** | §1.1 | ~8-sep y ~14-oct |

> **Si abrís esto en frío: no hay una dirección nueva para elegir.** Hay tres cosas con
fecha y una medición corriendo. El resto está cerrado con número.

---

### 2.0.A Volatilidad de alts — **CERRADA 29-ago (corrida 8)**

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

### 2.0.B Eventos de listado en Binance — **CERRADA 29-ago (corrida 9)**

**La idea.** Un listado tiene timestamp exacto, efecto documentado, y **no sale del
precio**. Era la última familia de estudio de evento con mecanismo plausible.

**Dos sorpresas buenas antes del cierre, y las dos hay que llevárselas:**

1. **No hace falta scrapear el blog. La primera vela de un par ES el listado**, exacta y
   verificable (BTCUSDT 2017-08-17, SOL 2020-08-11, PEPE 2023-05-05, SRM deslistado
   2022-11-28 tras FTX). El handoff daba por hecho que había que scrapear o comprar datos.
2. **La muestra no tiene sesgo de supervivencia.** `exchangeInfo` **sí** devuelve los
   deslistados, con `status == "BREAK"`. **734 pares USDT existieron; 249 están muertos.**
   Tras excluir por clase de activo quedan **579 eventos, 544 útiles, 266 semanas, y
   32,1% de ellos son símbolos hoy deslistados.**

> ⚠️ **Filtrar `exchangeInfo` por NOMBRE los da a todos por vivos y dice que el 0,1% se
> deslistó.** Hay que filtrar por **`status`**. Ese error, sin detectar, convierte el
> estudio en un falso positivo garantizado: los listados que se hundieron desaparecen de
> la muestra y el drift post-listado sale positivo por construcción.

**Lo que la mató: la σ, no el n.** El exceso de retorno de un par recién listado tiene
**σ = 23,8 pp a un día** — una moneda que debuta se mueve ±24% contra el mercado en su
primer día.

| h | eventos | semanas | **MDE (ATR)** | **MDE (%)** | años para detectar 1% |
|---|---|---|---|---|---|
| 1d | 544 | 266 | **0,472** | 4,2% | **90** |
| 3d | 543 | 266 | 0,880 | 7,6% | 296 |
| 7d | 544 | 266 | 0,660 | 5,7% | 165 |
| 30d | 544 | 266 | 1,352 | 12,0% | 742 |

*(umbral preregistrado 0,10 ATR. Binance tiene 9 años de listados.)*

**Lo que sí queda descartado, dicho con precisión:** un efecto de más de **±4,2% a un día**
no está. Eso mata la versión fuerte ("comprar todo listado nuevo es un negocio obvio") y
**nada más**. Un efecto de 1-2% —que sería 2-4× el costo y muy operable— queda debajo del
ruido y no se puede ni afirmar ni negar.

**Lo único que quedó vivo de esta dirección:** medir **el anuncio** en vez del listado, con
el timestamp del blog, **restringido a activos que ya cotizaban en otro venue**. Ahí hay
serie de precio previa y la dispersión es la del activo maduro, no la del debut. Exige el
scrapeo que esta corrida evitó y el n baja mucho, pero es una corrida distinta y viable.

---

### 2.0.C Patrones de gráfico — **CERRADA 29-ago (corridas 11 y 12)**

**Era la última familia estándar sin tocar.** Hombro-cabeza-hombro, doble techo/piso,
triángulos: estructura sobre decenas de barras, una forma funcional que no estaba en
ninguna corrida anterior.

**Primero la compuerta de potencia (corrida 11), que es la advertencia que las corridas 8
y 9 dejaron pagada. Y PASÓ** — la primera en tres. La idea que la hizo barata: el MDE de
este estimador **no depende de qué patrón sea, sino de cada cuánto dispara**, así que la
curva se mide con máscaras al azar **sin escribir un solo detector**.

| | frontera (tasa mínima medible) |
|---|---|
| 1d, H=1 / H=3 / H=5 | 0,200% / 0,500% / **2,000%** |
| 1h, H=4 / H=24 | **0,050%** / 0,500% |

Tasas medidas: doble techo 0,76%, doble piso 0,65%, HCH 0,46%, HCH inv 0,43%, triángulo
0,19%. **A 1h los cinco pasan con holgura** (0 brazos "no se pudo medir" de 120). A 1d
solo en horizontes cortos.

**Después el efecto (corrida 12): CERO, y es un cero MEDIDO.** 180 brazos a 1d, 120 a 1h,
**ninguno sobrevive y ninguno pasa FDR**.

**Lo que cierra la familia no es el cero: es POR QUÉ.** Excesos antes de costos:

| el mejor de… | 1h |
|---|---|
| **`CTRL ruptura arriba` — sin pivotes, sin tolerancias, sin figura** | **+0,2532** |
| el mejor patrón de gráfico real | +0,1545 |
| el mismo detector con **pivotes barajados** | +0,0758 |
| máscara al azar | +0,0291 |

> **Una ruptura pelada del máximo de 60 barras le gana a los cinco patrones.** Y el
> detector con la estructura destruida —mismos parámetros, pivotes al azar— llega tan
> lejos como el real. Lo poco que los patrones capturan **es el breakout**; la geometría
> no aporta nada medible.

**Dos brazos casi engañan, y las dos veces lo que los mata estaba escrito antes:**

- **`hch_inv` corto, 1d, H=5**: +0,2158, **p = 0,0245**, y **aguanta `sin_top3`**. Muerto
  por tres cosas independientes: es la **dirección invertida** (HCH invertido es alcista y
  este brazo gana yendo corto); **la compuerta de la corrida 11 ya lo había descalificado**
  —dispara al 0,182%, y su MDE a su propia tasa y horizonte es 0,2245, o sea que su
  exceso está **dentro de su propia banda de ruido**—; y no pasa FDR.
- **`CONTROL azar 2` largo, 1d, H=5**: exceso +0,1103, **p = 0,0495**, `sin_top3` +0,0475.
  **Ruido puro con p < 0,05 que aguanta la compuerta de concentración.** Con 180 brazos es
  exactamente lo esperable, y es la razón de que el FDR vaya sobre el lote entero.

**Lo que NO queda dicho:** que ninguna implementación posible funcione. Sí queda medido que
**la estructura no aporta sobre la ruptura** —dos controles independientes—, y eso es un
resultado sobre la forma funcional, no sobre un juego de umbrales.

---

### 2.0.D Horizontes largos (> 1 semana) — **CERRADA 30-ago (corrida 13)**

**Era el último lugar del mapa donde la respuesta era "no se probó" y no "no está".** El
veredicto de la corrida 4 dice, textual, *"a horizontes de 4h a 7d"*. Más allá de una
semana no se había medido nada, y en la literatura el momentum transversal se define a
1-12 **meses**.

**La hipótesis tenía un argumento real a favor:** el término de costo del harness no
depende del horizonte, así que se paga 52 veces al año a 7d y **4 veces** a 90d. Era lo
contrario del problema que mató a 1h en la corrida 12.

**Se midió, y la compensación resulta EXACTA:**

| horizonte | barras | **MDE %/año** | costo %/año | **BRUTO necesario** |
|---|---|---|---|---|
| **168h (7d)** | 255 | 22,0 | 10,43 | **32,4** ← calibración |
| **720h (30d)** | 58 | 30,2 | 2,43 | **32,6** |
| **2160h (90d)** | 19 | 29,8 | 0,81 | **30,6** |

> **El costo cae 13× y la precisión cae casi lo mismo: el efecto bruto necesario se queda
> quieto en 30,6-32,6%/año, con 6% de dispersión.** Alargar el horizonte no es una palanca,
> es un empate. Era la última perilla del mapa y no gira nada.

**Barrido de `k` (la única otra perilla):** `k=20` estaba bien elegido a 7d y algo corto a
30d. El mejor caso de las dos perillas juntas —`k=40` a 30 días— da **26,4%/año**, todavía
2,6× el umbral preregistrado de 10%.

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
una **medición**, no una construcción. **Con §2.0.A y §2.0.B cerradas, esto pasó a ser lo
PRIMERO de la lista** — lo único que queda que se mide en vez de construirse.

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

> **Contar el n no alcanza: hay que medir la σ, y decir cuál de las dos falló.** Corrida 6:
> 257 semanas, MDE ±0,065 ATR, **concluyó**. Corrida 9: **266 semanas**, MDE 0,47-1,35 ATR,
> **no pudo**. Más n, veredicto opuesto. Y la distinción importa para el que sigue: un "no
> se pudo medir" por **n** se reabre si aparece el dato (corrida 8); uno por **σ** no se
> reabre con más datos, solo con otro estimador (corrida 9).

> **Un estudio de eventos se filtra por ESTADO, no por nombre.** `exchangeInfo` devuelve los
> deslistados con `status == "BREAK"`; filtrar por nombre los da a todos por vivos. En un
> estudio de listados ese error tiene la forma exacta del efecto buscado y **fabrica un
> falso positivo garantizado**.

> **Antes de elegir resolución, calcular el COSTO en las mismas unidades que el ruido.**
> A 1h el costo de una vuelta es **0,155 ATR** y el MDE **0,029**: la corrida solo podía
> encontrar un efecto 5× más grande que el que su propio ruido permitía detectar. Bajar el
> ruido no sirve si el piso de costo sube más rápido.

> **Un control tiene que poder GANAR, o no es un control.** La corrida 12 midió que una
> ruptura pelada —sin pivotes, sin tolerancias, sin figura— **le gana a los cinco patrones
> de gráfico**, y que el mismo detector con los pivotes barajados llega tan lejos como el
> real. Sin esos dos controles el cero se habría leído como "el mercado es eficiente" en
> vez de "lo que el patrón detecta es el breakout".

> **El FDR va sobre el LOTE ENTERO, con los controles adentro.** Medido: con 180 brazos,
> una **máscara al azar** dio **p = 0,0495** y **aguantó `sin_top3`**. Mirando un brazo por
> vez, eso es un descubrimiento.

> **Todo MDE se reporta ANUALIZADO, no en ATR por tenencia.** Comparar un MDE de 24h con
> uno de 90d en "ATR por tenencia" no significa nada: la misma unidad mide cosas distintas.
> Y anualizar es lo que dejó ver que el "0 de 4.140" de la corrida 4 descartaba **32%/año
> bruto y no 10%** (corrida 13).

> **Generar ancho, no pre-filtrar.** Las compuertas son sobre conclusiones, no sobre
> explorar. El prior propio no aporta; el harness mata barato.

---

## 4. Herramientas — qué usar para qué

| archivo | qué hace |
|---|---|
| **`banco/ranking.py`** | **el harness bueno.** Ranking transversal por barra: sin corte pooled, sin trampa de `SEM_N_MIN`, control por barra, sin solape. Seis compuertas + FDR |
| **`banco/correr_velas.py`** | **el harness de EVENTOS.** Control por barra para máscaras booleanas. `correr_graficos.py` es el ejemplo de cómo enchufarle un detector nuevo |
| `banco/velas.py` | 15 patrones de velas con sus umbrales canónicos fijados. El patrón a copiar para preregistrar un detector |
| `banco/klines.py` | `load_panel(..., tf=, full=, pin=, syms=, mercado=)`. **`mercado="fut"`** trae perpetuos; `tf="1d"` trae diarias |
| `banco/onchain.py` | CoinMetrics Community (gratis, sin key). Une por `AssetEODCompletionTime`: **sin lookahead y sin lag fijo** |
| `banco/futuros.py` | funding alineado al tablero. Entra en el **retorno**, no en el costo |
| **`banco/libro_perp.py`** | **costos reales del libro**, spot y perp en el mismo instante y apareados |
| `banco/test_unlocks.py` | estudio de evento esparcido: permutación + bootstrap de símbolos |
| **`banco/correr_listados.py`** | **`--nula` mide supervivencia, n post-join, MDE en ATR Y en %, y los años que harían falta.** El patrón para cualquier estudio de evento nuevo. Su caché tiene la **diaria completa de los 734 pares USDT que existieron**, deslistados incluidos |
| `opciones/iv_rv.py` | DVOL de Deribit vs realizada futura. Cerró BTC/ETH; **su barra de error está ahora medida en la corrida 8** |
| **`opciones/viabilidad.py`** | **foto de los 3 venues de opciones**: volumen, OI y spread ATM por subyacente, todo a nocional USD. El patrón de "medir si el instrumento existe ANTES de medir el efecto" |
| **`opciones/potencia.py`** | **n, σ, MDE y años necesarios** para un estimador dado, con calibración contra un efecto conocido y la ρ que decide si poolear sirve |
| `banco/correr_onchain.py` | el patrón de `--nula`: contar el n y el MDE antes de nada |
| **`banco/horizonte_largo.py`** | **MDE anualizado por horizonte, con el costo en la misma unidad.** La compuerta que hay que correr antes de proponer cualquier horizonte nuevo |
| **`banco/potencia_graficos.py`** | **la curva MDE(tasa, horizonte) con máscaras al azar.** Dice si una familia se puede medir ANTES de construirle el detector |
| `banco/graficos.py` | 5 patrones de gráfico con sus tolerancias fijadas, sin lookahead, y su versión con **pivotes barajados** (el control de estructura) |
| `banco/correr_graficos.py` | el lote de la corrida 12: control de ruptura simple, pivotes barajados, azar, y FDR sobre todo junto |
| **`banco/dislocacion.py`** | **filo ejecutable entre Binance/OKX/Bybit** con control de tamaño y de skew. `--recolectar` y `--analizar` |
| `banco/libro.py` | camina el libro. `--mercado fut` para perpetuos |

**Los preregistros** (`banco/PREREGISTRO_*.md`) tienen las reglas escritas antes de cada
corrida y los resultados debajo de la línea. `TRANSVERSAL` (4 corridas), `FUTUROS` (5 y
5b), `ONCHAIN` (6), `VELAS` (7), **`OPCIONES` (8)** y **`LISTADOS` (9)**.

---

## 5. Lo que este repo mide, dicho sin vueltas

Después de **trece corridas** y **las diez familias estándar completas**, el resultado es
éste, y conviene decirlo con la precisión que la corrida 13 midió:

> **No hay un efecto direccional GRANDE —del orden de 30%/año bruto o más— en la
> información que está en el precio, en las 200 monedas más líquidas, a horizontes de 4h a
> 90 días.**

**Lo que NO está establecido, y hasta la corrida 13 sonaba como si lo estuviera:** que no
haya un efecto **modesto**. Con 5 años de historia, el mejor estimador transversal del repo
tiene una resolución de **26-32%/año bruto**. Un edge real de 8-15%/año **habría sido
invisible en las trece corridas**.

**Y el motivo no es un defecto del método: es el largo de la muestra.** Con rebalanceo
semanal hay 255 observaciones independientes en 5 años, y el error estándar de la media no
baja de ahí por más features que se prueben. Para llevar la resolución a 10%/año harían
falta **~7× más observaciones**, o sea ~35 años.

> **Eso NO es una invitación a seguir buscando: es lo contrario.** Dice que la pregunta
> "¿hay un edge modesto?" **no tiene respuesta alcanzable con estos datos**, y que agregar
> features sobre la misma ventana no mueve el error estándar ni un poco. Lo único que lo
> mueve es el tiempo.

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

- **No siempre es que no haya efecto: a veces no hay con qué medirlo, y hay dos maneras
  distintas de que pase.** Las corridas 8 y 9 murieron las dos por potencia, en extremos
  opuestos:
  - **corrida 8** (volatilidad de alts): el instrumento **existe** y el spread **es
    barato** —lo contrario de lo que el prior decía— pero hay **18 meses de historia** y
    harían falta 23 años. **BTC, con la misma ventana, tampoco sería detectable** (MDE
    27,1%/año). Muere por **n**, y se reabre si aparece el dato.
  - **corrida 9** (listados): el n es **excelente** —544 eventos, 266 semanas, 32%
    deslistados, sin sesgo de supervivencia— y aun así no concluye, porque una moneda que
    debuta se mueve **±24% contra el mercado en su primer día**. Muere por **σ**, y no se
    reabre con más datos: harían falta 90 años.

  Las dos veces, la compuerta de potencia corrida ANTES ahorró la sesión entera.

**Lo que sí sobrevive todo lo que se le tire es la MAGNITUD** — cinco diseños, cuatro
regímenes, 100% de las semanas en el mejor caso. El problema nunca fue medirla: es que
**no hay instrumento para cobrarla**.

Y la corrida 8 le puso un número a esa frase. La única forma conocida de cobrar magnitud es
vender volatilidad; el instrumento **sí existe** en SOL, XRP y HYPE, y el spread **sí deja
margen**. Lo que falta es la historia para saber si pagan. **Ya no es "no hay instrumento":
es "no hay con qué decidir si conviene usarlo".**
