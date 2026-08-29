# PREREGISTRO — corrida 8: ¿existe el instrumento para vender volatilidad en alts?

> Escrito el **2026-08-29**, **antes de mirar un solo número** de los tres venues.
> Dirección **§2.1 de `HANDOFF_TRES.md`** — la primera de las tres.
> Código: `opciones/viabilidad.py`. Resultados **debajo de la línea**, no arriba.

---

## 1. Qué se pregunta, y por qué esta es la pregunta

Vender volatilidad es **lo único que este repo encontró que funcionó de verdad**:
+20,96%/año en BTC sobre 5,3 años, DD máx −11,3%, sobrevive sacar los 3 mejores meses, y
tiene **mecanismo** (prima de seguro) en vez de ser un patrón encontrado buscando. Se midió
**solo en BTC/ETH** (`opciones/iv_rv.py`), donde Deribit es eficiente y el premio ya se
arbitró: de +16,6% de la prima en 2021-22 a **+9,9% en 2024-26** en BTC, y **+0,5%** en ETH.

La hipótesis de esta corrida: **en alts la competencia es mucho menor, así que la prima
debería estar menos comprimida.**

**Pero esto NO es un test estadístico. Es un test de VIABILIDAD**, y va primero por una
razón: *puede que el instrumento no exista*. Medir una prima que no se puede cobrar es
gastar una sesión para nada. La misma lógica que en 2.4.B (market making sobre el perp),
donde la aritmética mató el ítem sin gastar una corrida.

---

## 2. La regla de parada — escrita ANTES de mirar

Se abre §2.1 (o sea: se pasa a medir la prima) **solo si se cumplen LAS DOS condiciones**.
Si cualquiera falla, el veredicto es **cerrado**, y no se mide ninguna prima.

### (A) Instrumento — que haya mercado

> **≥ 3 subyacentes que NO sean BTC ni ETH**, en cualquiera de los tres venues
> (Deribit, OKX, Bybit), que cumplan **las dos** a la vez:
>
> - volumen de opciones de las últimas 24 h ≥ **USD 1.000.000** de nocional, y
> - open interest ≥ **USD 5.000.000** de nocional.

**Por qué esos números y no otros.** Son deliberadamente bajos: un solo straddle ATM a 30d
sobre una alt con σ=60% mueve ~USD 15k de prima por cada USD 100k de nocional. Un mercado
que no rota USD 1M por día no absorbe un programa sistemático ni con capital de juguete, y
un OI de USD 5M significa que **el libro entero del subyacente** es más chico que la
posición de un solo participante mediano. Si no se llega ni a esto, no hay nada que medir.

**El OI va junto al volumen a propósito:** el volumen de 24 h de un venue chico se infla con
un solo trade de bloque. El OI es un stock, no un flujo, y no se fabrica en un día.

### (B) Costo — que se pueda cobrar

> Para esos mismos subyacentes: la mediana del **spread ATM relativo**
> (`(ask − bid) / mid`) del vencimiento más cercano a 30 días con **≥ 7 días de vida**,
> debe ser **≤ 30%**.

**Por qué 30% y por qué esa forma de leerlo.** Un programa de venta de volatilidad
sistemático vende al **bid** y (en el caso más favorable) **lleva a vencimiento**, así que
cruza una sola vez: el costo es **medio spread**, o sea ≤ **15% de la prima**. Ese 15% es
**del orden del premio total que BTC llegó a pagar en su mejor régimen** (16,6% de la prima
en 2021-22; 9,9% en 2024-26). Un spread por encima de eso se come el edge completo antes de
que exista.

**Se usa la lectura de UNA sola cruzada, la más generosa**, a propósito: si esto cierra, que
no cierre por un supuesto pesimista. Se reporta igual el round-trip (spread entero) como
referencia.

**Descartes explícitos, escritos antes:** no cuentan como cotización válida un `bid` de 0,
un `ask` sin `bid`, ni un mid ≤ 0. Un libro de un solo lado **no es un mercado**; contarlo
como spread infinito o saltearlo da lo mismo para el veredicto, pero se reporta cuántos
instrumentos se cayeron por eso, porque *ese* número es parte de la respuesta.

### Si la primera foto PASA

La foto de hoy es el **filtro**, no la prueba. Si pasa, **antes** de gastar una sesión
midiendo la prima se hace el chequeo de **consistencia**: repetir la foto en **≥ 3 días
distintos** y verificar que los mismos ≥3 subyacentes siguen cumpliendo (A). El handoff pide
"volumen diario **consistente**", y un solo día no lo demuestra.

### Si la primera foto FALLA

**Cerrado, y se escribe el número que lo cerró.** No se busca un cuarto venue, no se baja el
umbral, no se argumenta que "con OI un poco más chico igual se puede". La regla se escribió
antes justamente para esto.

---

## 3. Cómo se mide (para que sea reproducible)

- **Deribit**: `/public/get_book_summary_by_currency` (por moneda, `kind=option`) da
  `volume`, `open_interest`, `bid_price`, `ask_price`, `mark_price` por instrumento. Las
  monedas listadas salen de `/public/get_currencies`.
- **OKX**: `/api/v5/public/instruments?instType=OPTION&instFamily=…` para el listado, y
  `/api/v5/market/tickers?instType=OPTION&instFamily=…` para `vol24h`, `bidPx`, `askPx`.
  El OI sale de `/api/v5/public/open-interest`.
- **Bybit**: `/v5/market/tickers?category=option&baseCoin=…` da en una sola llamada
  `bid1Price`, `ask1Price`, `volume24h`, `openInterest`, `markPrice`, `underlyingPrice`.

**Unidad: todo a nocional en USD.** Los tres venues reportan cantidades en unidades del
subyacente (contratos de 1 BTC, de 0,01 BTC, etc.) y algunos el volumen en la moneda base.
Se multiplica por el precio del subyacente y **se dice explícitamente qué convención usó
cada venue**, porque comparar un `volume` de Deribit (en cripto) contra un `vol24h` de OKX
(en contratos) sin convertir es exactamente el tipo de error que fabrica un falso positivo.

**ATM** = el strike con `|strike − spot|` mínimo dentro del vencimiento elegido.

---

## 4. Lo que esta corrida NO responde

- **No mide la prima.** Ni implícita ni realizada. Eso es el paso 2, y solo existe si esta
  corrida pasa.
- **No dice nada sobre BTC/ETH.** Eso ya está cerrado en `iv_rv.py` con su número.
- **No evalúa sintetizar la venta de vol con órdenes stop.** Eso está cerrado y pagado:
  regalás k·ATR por trade y falta la convexidad. Si no hay opciones, **no hay atajo**.

---

## 5. Reglas del repo que aplican acá

- La regla de parada se escribe ANTES de mirar. ✔ (§2)
- Todo se corre a dos costos → acá el costo **es** la medición. ✔
- El universo se filtra por clase de activo → se reportan aparte los subyacentes que sean
  stablecoins/FX o acciones tokenizadas, si aparecen. ✔

---

## 6. COMPUERTA (C) — potencia: se puede medir la prima?

> **Escrita el 2026-08-29, DESPUES de ver pasar (A) y (B) y DESPUES de descubrir que
> historia de vol implicita existe, pero ANTES de calcular un solo MDE o una sola prima.**
> Ese orden importa y por eso queda escrito: lo que se vio para escribirla es *cuanta
> historia hay*, no *cuanto vale la prima*.

**Por que aparece.** (A) y (B) preguntan si el instrumento existe y si se puede cruzar.
No preguntan si la prima se puede **medir**. Al buscar la serie de implicita para el paso 2
apareció que:

- **DVOL de Deribit —el indice que usa `iv_rv.py`— solo existe para BTC y ETH.** SOL, XRP,
  AVAX y DOGE devuelven la serie vacia; HYPE y TRX ni siquiera son parametros validos.
- La unica fuente publica de implicita historica para alts es el indice a 30d de **Bybit**
  (`/v5/market/historical-volatility`, horario). **Gotcha:** exige `quoteCoin=USDT` —sin
  ese parametro devuelve `SUCCESS` con lista vacia, que parece "no hay datos" y no lo es—
  y solo acepta ventanas de <= 30 dias, asi que hay que paginar hacia atras.

La regla del repo que manda aca es la misma que convirtio unlocks y la cola iliquida en
"no esta" en vez de "no se pudo": **contar el n post-join y calcular el MDE con la nula
real ANTES de estimar.**

### La regla de parada de (C)

El estimador del paso 2 es el de `iv_rv.py`: vender una straddle ATM a 30 dias por mes,
**no solapadas**. El n efectivo son **meses**, no dias — la serie horaria de Bybit tiene
~720 puntos por mes y **ninguno** de ellos es una observacion independiente.

> Para cada subyacente candidato: si el **MDE sobre el retorno neto anualizado** de esa
> straddle mensual, a 80% de potencia y alfa 0,05 a dos colas, es **> 10%/ano**, ese
> subyacente se declara **"no se pudo medir"**. Si quedan **menos de 3** subyacentes
> medibles, la direccion §2.1 se cierra por potencia.

**Por que 10%/ano y no otro numero.** Es la escala en la que se decidio BTC: +20,96%/ano en
la muestra completa, **+7,33%/ano en el regimen reciente**, contra un piso de stablecoins de
4-5%/ano y un costo de rebalanceo. Un efecto que valga la pena perseguir en alts tiene que
superar con claridad piso + costo, o sea ~15%/ano. Un MDE de 10%/ano detecta eso. Un MDE mas
ancho no distingue "tan bueno como la version muerta de BTC" de "el doble de lo que BTC
pago en su mejor momento", y entonces **cualquier resultado positivo seria ruido**.

**La direccion se declara antes:** la hipotesis es **IV > RV** (la prima es positiva y
vender volatilidad gana). Si el signo sale al reves, es un resultado en contra, no un
descubrimiento de que hay que comprar volatilidad.

**El MDE se calcula con la dispersion real de la serie**, no con una supuesta: se estima la
desviacion estandar del P&L mensual de la straddle con la realizada efectiva de cada
subyacente, y `MDE = 2,8 x sigma_mensual / sqrt(n_meses) x 12`.

### Lo que (C) NO hace

No mira el signo ni el tamano de la prima. Solo cuenta meses y calcula el ancho de la
barra de error. El numero de la prima se mira **despues** de que (C) diga quien es medible.

---
---

# RESULTADOS

> Corrido el **2026-08-29**. `opciones/viabilidad.py` (A y B) y `opciones/potencia.py` (C).
> Foto de los libros: 2026-08-29 19:27 UTC.

## VEREDICTO: **CERRADA POR POTENCIA.** El instrumento existe. La prima no se puede medir.

Las tres compuertas dieron cosas distintas, y esa es la parte interesante:

| compuerta | pregunta | resultado |
|---|---|---|
| **(A)** instrumento | ¿hay mercado? | **PASA**, justo — exactamente 3 |
| **(B)** costo | ¿se puede cruzar? | **PASA con holgura** — un orden de magnitud |
| **(C)** potencia | ¿se puede medir? | **FALLA por 4×**, y BTC también |

---

### (A) — el instrumento existe, y pasa justo

**Seis** subyacentes alt tienen opciones listadas en los tres venues: AVAX, DOGE, HYPE, SOL,
TRX, XRP. **Tres** cumplen volumen ≥ USD 1M/24h **y** OI ≥ USD 5M (nocional del subyacente):

| venue | sub | vol 24h | open interest | ATM spread | 2 lados |
|---|---|---|---|---|---|
| deribit | SOL | 2.994.137 | 122.969.129 | 3,9% | 63/124 |
| deribit | XRP | 1.401.494 | 72.508.477 | 3,5% | 39/74 |
| bybit | SOL | 6.514.143 | 11.851.786 | 0,8% | 60/68 |
| bybit | HYPE | 1.171.580 | 5.347.814 | 4,5% | 39/60 |
| deribit | HYPE | *914.756* | 30.786.596 | 1,5% | 45/66 |
| deribit | TRX | *883.254* | 15.135.082 | 5,5% | 28/60 |
| deribit | AVAX | *329.233* | *14.108.436* | 0,9% | 43/64 |
| bybit | XRP | *597.537* | *4.097.630* | 4,6% | 37/52 |
| bybit | DOGE | *196.796* | *1.132.685* | 12,3% | 22/36 |

*(en cursiva lo que no cruza su umbral)*

**Pasa con exactamente 3, y dos de los tres pasan por poco:** XRP cumple volumen por 1,4×;
HYPE cumple en Bybit por 1,17× en volumen y **1,07× en OI**. Solo SOL pasa cómodo. La regla
decía 3 y hay 3 — se respeta como está escrita, ni se afloja ni se aprieta. Si la corrida
hubiera terminado acá, el paso siguiente era el chequeo de consistencia de 3 días, que
existe justamente porque un margen de 7% en una sola foto no prueba nada.

**La estructura del mercado quedó verificada de paso**, y sirvió para cazar un error:
Deribit domina (USD 1,00 mil M/día y USD 31,8 mil M de OI en BTC), OKX hace USD 280 M y
Bybit USD 590 M.

> ⚠️ **Gotcha de unidades, que dio un número imposible y hay que llevarse:** el tamaño de
> contrato de OKX es **`ctVal` × `ctMult`**, no `ctVal` solo. `ctVal` viene `1` y `ctMult`
> `0,01` (BTC) o `0,1` (SOL). Usar `ctVal` solo infla el nocional **100×** y daba **USD
> 628.000 millones/día en opciones de BTC en OKX** — seis veces Deribit, o sea imposible.
> Lo que lo delató no fue el código: fue que **el número contradecía una estructura de
> mercado conocida.** Se corrigió usando `volCcy24h` y `oiCcy`, que ya vienen en unidades
> del subyacente.

---

### (B) — el costo NO es el problema, y esto contradice el prior

El spread ATM relativo del vencimiento a ~27 días:

| sub | spread ATM | = costo de vender al bid | umbral |
|---|---|---|---|
| SOL | **2,3%** | 1,2% de la prima | 30% |
| XRP | **3,5%** | 1,7% de la prima | 30% |
| HYPE | **4,5%** | 2,2% de la prima | 30% |

**Pasa por un orden de magnitud.** La intuición de que "las opciones de alts deben tener
spreads impagables" es **falsa**: cruzar cuesta 1-2% de la prima, contra un premio que en
BTC llegó a ser 16,6%. Si el ítem hubiera muerto, no habría muerto por el spread.

**El matiz que sí está:** solo **la mitad** de los instrumentos del vencimiento tiene
cotización de dos lados (63/124 en SOL de Deribit, 39/74 en XRP). El ATM cotiza fino; las
alas, la mitad de las veces, no cotizan.

---

### (C) — acá muere, y no por las alts

**No existe historia pública de volatilidad implícita para alts en la fuente canónica.**
El DVOL de Deribit —el índice que usa `iv_rv.py`— solo tiene datos para **BTC y ETH**. SOL,
XRP, AVAX y DOGE devuelven serie vacía; HYPE y TRX ni siquiera son parámetros válidos.

La única fuente que sí la tiene es el índice a 30d de Bybit, y alcanza para esto:

| sub | ventana | **meses** | σ/mes | **MDE** | umbral |
|---|---|---|---|---|---|
| BTC *(calibración)* | 2025-02 → 2026-07 | 18 | 3,42% | **27,1%/año** | 10% |
| ETH *(calibración)* | 2025-02 → 2026-07 | 18 | 4,41% | **34,9%/año** | 10% |
| **SOL** | 2025-02 → 2026-07 | 18 | 4,93% | **39,0%/año** | 10% |
| **XRP** | 2025-10 → 2026-07 | 10 | 5,32% | **56,5%/año** | 10% |
| **HYPE** | ~7 semanas | **0** | — | **∞** | 10% |

**Subyacentes medibles: 0 de 3.** La compuerta pedía 3.

> ⚠️ **Gotcha de la fuente:** `/v5/market/historical-volatility` de Bybit **exige
> `quoteCoin=USDT`**. Sin ese parámetro devuelve `retCode: 0, SUCCESS` con **lista vacía** —
> que se lee como "no hay datos" y no lo es. Además solo acepta ventanas de ≤ 30 días, así
> que hay que paginar hacia atrás.

#### La calibración es lo que vuelve al veredicto incontestable

**BTC, con esos mismos 18 meses, da MDE 27,1%/año.** El efecto de BTC está *medido y es
conocido* (+20,96%/año en la muestra larga) y **tampoco sería detectable**. O sea: no es que
las alts sean especiales. **Es que 18 meses de straddles mensuales no alcanzan para nada.**

Y con la muestra larga tampoco alcanza:

| | ventana | meses | σ/mes | MDE |
|---|---|---|---|---|
| BTC (DVOL completo) | 2021-03 → 2026-07 | **65** | 4,25% | **17,7%/año** |
| ETH (DVOL completo) | 2021-03 → 2026-07 | **65** | 5,55% | **23,1%/año** |

**Cuánta historia haría falta para un MDE de 10%/año:**

| sub | meses necesarios | **años** | hay |
|---|---|---|---|
| BTC | 132 | **11,0** | 1,5 |
| ETH | 220 | **18,3** | 1,5 |
| SOL | 274 | **22,9** | 1,5 |
| XRP | 319 | **26,6** | 0,8 |

*(con la σ del DVOL largo: BTC 17,0 años, ETH 29,0)*

**Vender una straddle mensual es un estimador con una relación señal/ruido de ~1/5 por mes.
Ninguna cantidad realista de historia lo arregla.**

#### Las tres salidas, medidas y tapadas

1. **"Delta-hedgear baja la varianza."** **No**: la σ que se usó ya *es* la del P&L
   delta-hedgeado. La fórmula `0,7979 × (IV−RV) × √T` es el P&L en vega de una straddle
   **con cobertura perfecta**; el término direccional ya está afuera. Los dos estimadores
   —dispersión de la realizada y dispersión de (IV−RV)— dan casi lo mismo (BTC: 3,42 vs
   3,62), así que el veredicto no depende de cuál se elija. Se usó el más ajustado de los
   dos, para que nada cierre por un supuesto pesimista.

2. **"Poolear varios subyacentes."** **Medido, y es la salida que peor sale.** Correlación
   del P&L mensual de la straddle entre BTC, ETH, SOL y XRP:

   | | BTC | ETH | SOL | XRP |
   |---|---|---|---|---|
   | **BTC** | 1,00 | 0,96 | 0,93 | 0,90 |
   | **ETH** | 0,96 | 1,00 | 0,96 | 0,88 |
   | **SOL** | 0,93 | 0,96 | 1,00 | 0,87 |
   | **XRP** | 0,90 | 0,88 | 0,87 | 1,00 |

   **ρ medio = +0,92.** Con `n_ef = k / (1 + (k−1)ρ)`: **4 subyacentes son 1,07
   independientes.** La barra se angosta **1,03×**, no 2×. MDE pooleado: **37,8%/año**.

   > **La volatilidad de cripto es esencialmente UN factor. Sumar monedas agrega nombres,
   > no información.** Esto es un resultado sobre el mercado, no sobre el método, y aplica
   > a cualquier cosa futura que quiera ganar potencia sumando activos.

3. **"Esperar y acumular."** Los años de la tabla de arriba son la respuesta. HYPE necesita
   ~23 años de historia y tiene 7 semanas.

---

### Lo que esta corrida le corrige al cierre anterior de `iv_rv.py`

`iv_rv.py` cerró BTC/ETH con dos números: **+20,96%/año** en la muestra completa y
**+7,33%/año** en el régimen reciente, y leyó la caída como "el premio se **compitió**".

Con la barra de error ahora medida (σ 4,25%/mes, n=65 → SE ≈ 6,3%/año):

- **+20,96%/año sí supera el ruido** (≈ ±12,4% al 95%). Ese número está.
- **+7,33%/año está adentro del ruido.** Con ~43 meses el intervalo es ≈ ±15%/año.

**El cierre sigue en pie** —no superaba el piso de stablecoins, y una barra de error ancha
es una razón para cerrar, no para reabrir—. Lo que cambia es **por qué**: no es solo que el
premio se comprimió, es que **a este estimador no le alcanza para distinguir +7% de cero**.
La frase "el edge se compitió" era una lectura más fuerte de lo que el dato aguanta.

---

### Lo que quedó pendiente y NO se hizo, a propósito

El **chequeo de consistencia de 3 días** de (A). Era obligatorio *antes de medir la prima*,
y la prima no se va a medir: (C) cerró la dirección. Correrlo ahora sería gastar tres días
en confirmar un instrumento que igual no se puede evaluar. `opciones/viabilidad.py` guarda
cada foto en `opciones/.snapshots/`, así que si alguna vez se reabre, la del 2026-08-29 ya
está tomada.

### Lo único que reabriría esto

Que aparezca una fuente con **historia larga** de implícita en alts (un dataset pago de
superficies, o Deribit extendiendo DVOL a SOL/XRP y publicando el histórico). Sin eso no
hay corrida posible, y **no es un problema de método: es que el dato no existe.**

