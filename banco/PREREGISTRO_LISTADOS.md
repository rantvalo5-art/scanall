# PREREGISTRO — corrida 9: eventos de listado en Binance

> Escrito el **2026-08-29**, **antes de contar un solo evento**.
> Dirección **§2.1 de `HANDOFF_TRES.md`** (era la 2.2 hasta que la corrida 8 cerró la de
> opciones). Código: `banco/correr_listados.py`. Resultados **debajo de la línea**.

---

## 1. Qué se pregunta

Un listado en Binance tiene **timestamp exacto**, efecto documentado en la literatura, y
**no sale del precio** — que es lo que lo distingue de las nueve familias que ya dieron
cero. Es la última familia de estudio de evento que queda con un mecanismo plausible
(atención, acceso, entrada de flujo minorista).

---

## 2. La fuente: la primera vela, no el blog

El handoff asumía que había que **scrapear el blog de anuncios** o comprar un dataset. No
hace falta: **la primera vela de un par en Binance ES el momento del listado**, exacta al
minuto, y sale del mismo endpoint de klines que ya usa todo el banco.

Eso es mejor que el blog en tres cosas: es exacto en vez de aproximado al día, no depende
de que el HTML no cambie, y **cubre todo el histórico**.

**Definición del evento, fijada acá:** la **primera vela del primer par USDT** de un activo
base en Binance spot. Un segundo par del mismo activo (p. ej. ya cotizaba contra BTC) **no**
es un evento nuevo; el evento es el debut del activo en el mercado que se está midiendo.

---

## 3. La condición que puede cerrar esto sola: SUPERVIVENCIA

> **`exchangeInfo` devuelve solo los símbolos que cotizan HOY.** Un activo que se listó y
> después se deslistó **no aparece**.

Ese es el sesgo más grave que puede tener este estudio, y es peor que el sesgo de universo
que el repo ya declara en `klines.universe()`: acá **es el sesgo con la forma exacta del
efecto que se busca**. Si la muestra son solo los listados que sobrevivieron hasta 2026, el
"drift post-listado" sale positivo **por construcción**, porque los que se hundieron después
del listado fueron deslistados y no están en la muestra. Sería un falso positivo garantizado.

> **Regla de parada, escrita antes de mirar:** si no se consigue el listado histórico de
> símbolos **incluyendo los deslistados**, el veredicto es **"no se pudo medir"** y se
> cierra. No se corre la versión con supervivientes solamente, ni "para ver qué da".
>
> Y si se consigue: **se reporta la fracción de eventos que corresponden a símbolos hoy
> deslistados.** Si esa fracción es < 5%, la fuente probablemente sigue estando sesgada y
> el veredicto vuelve a ser "no se pudo medir".

> **NOTA agregada al escribir los resultados:** la premisa de arriba resulto **FALSA**,
> y por suerte. `exchangeInfo` **sí** devuelve los deslistados, con `status == "BREAK"`.
> Lo que engaña es filtrar por NOMBRE en vez de por ESTADO. Ver los resultados: 249 de los
> 734 pares USDT están en `BREAK`. La regla de parada se mantiene tal cual escrita —lo que
> cambió es que la fuente que la satisface estaba mas cerca de lo que se creía—.

**Dónde buscarlo:** `data.binance.vision` publica los volcados históricos por símbolo, y su
listado incluye pares que ya no cotizan. Es el único camino sin dataset pago que se va a
intentar.

---

## 4. El estimador — control POR BARRA, y por qué

Se usa **el mismo estimador** de `correr_velas.py`, reimplementado en
`correr_listados.py` porque acá el evento no es una máscara sobre un panel existente: el
símbolo **no existe** antes de su primera vela, así que el tablero hay que armarlo con los
vivos en cada fecha. La fórmula es idéntica:

```
exceso(t) = media(y | simbolos donde disparo en t) - media(y | universo de t)
semana(w) = media de exceso(t) sobre las barras de w
estadistico = media de semana(w), cada semana pesando UNO
```

**El término de mercado es el sesgo principal de cualquier estudio de eventos.** Los
listados **se agrupan en el tiempo** —Binance lista más en un mercado alcista— así que
aparear por símbolo, como hace `lote.py`, dejaría el efecto de mercado adentro y el listado
"funcionaría" sin aportar nada.

**El n efectivo son las SEMANAS**, no los eventos: varios listados de la misma semana
comparten el mismo shock de mercado.

---

## 5. Lo que se declara ANTES de medir

**La dirección.** La literatura documenta un **salto en el anuncio y una reversión
posterior**. El salto **no es operable acá**: el par no existe antes de su primera vela, así
que no hay forma de comprarlo antes. Lo único medible y operable es lo de después.

> **Hipótesis declarada: el activo recién listado RINDE MENOS que el mercado en los días
> siguientes** (la reversión). Signo esperado: **negativo**.
>
> Si sale positivo y significativo, es un resultado **en contra de lo declarado**, y hay que
> tratarlo como tal — no como el descubrimiento de que hay que comprar listados. La corrida
> 7 midió por qué: 3 de sus 5 mejores brazos estaban invertidos, y elegido el signo después,
> los cinco contaban como aciertos.

**Los horizontes, los cuatro, fijados acá y todos adentro del FDR:** **1d, 3d, 7d y 30d**
después del cierre de la primera vela diaria. No se agrega un quinto después de ver los
resultados.

**La primera vela se DESCARTA como ventana de entrada.** El día del debut tiene un rango
que no es operable con una orden normal (arranca sin libro, sin referencia y con subastas).
La entrada se toma **al cierre de la primera vela diaria**.

**Dos costos, 0,20% y 0,50%**, como todo en este repo.

---

## 6. La regla de parada por potencia

Igual que la corrida 8, pero acá el umbral se ancla en las corridas que **sí** concluyeron.

> **Se calcula el MDE ANTES de estimar el efecto.** Unidad: **ATR**, la misma que usan las
> corridas 4, 5, 6 y 7, para que el número sea comparable. Unidad independiente: **semanas
> con al menos un listado**.
>
> `MDE = 2,8 x sigma_semanal / sqrt(n_semanas)`
>
> **Si el MDE > 0,10 ATR, el veredicto es "no se pudo medir" y se cierra sin estimar.**

**Por qué 0,10 ATR.** Las corridas que concluyeron llegaron a ±0,065 ATR (on-chain, la que
cerró derivados). Un estudio de eventos esparcidos no va a ser tan fino, pero 0,10 ATR sigue
estando **muy por debajo** de las magnitudes que este repo trató como decisivas: el screener
compra +3,12 ATR tarde y devuelve −0,94 ATR. Un efecto de listado más chico que 0,10 ATR no
cambiaría ninguna decisión aunque fuera real.

---

## 7. Lo que esta corrida NO hace

- **No mide el anuncio.** No es operable y no hay serie de precio antes del par.
- **No usa `base200`.** Ese universo es el ranking de hoy y tiene exactamente el sesgo que
  la §3 trata de evitar. El universo de control se arma con los símbolos vivos **en cada
  fecha**, no con los de hoy.
- **No mira futuros.** El evento es el listado en spot.

---
---

# RESULTADOS

> Corrido el **2026-08-29**. `banco/correr_listados.py --nula`.

## VEREDICTO: **NO SE PUDO MEDIR.** Y esta vez no es por falta de muestra.

La muestra es **buena** — mejor que la de la corrida que sí concluyó. Lo que mata es la σ.

| | corrida 6 (on-chain), **concluyó** | corrida 9 (listados) |
|---|---|---|
| n independiente | 257 semanas | **266 semanas** |
| MDE | **±0,065 ATR** | **0,47 a 1,35 ATR** |

**Mismo n, veredicto opuesto.** Ver §"lo que esto le corrige al método".

---

### La fuente: la primera vela, y salió mejor de lo esperado

No hizo falta scrapear nada. **La primera vela de un par ES el listado**, exacta y
verificable. Comprobado contra hechos conocidos:

| par | primera vela | contraste |
|---|---|---|
| BTCUSDT / ETHUSDT | 2017-08-17 | apertura de Binance ✔ |
| SOLUSDT | 2020-08-11 | ✔ |
| APTUSDT | 2022-10-19 | ✔ |
| PEPEUSDT | 2023-05-05 | ✔ |
| SRMUSDT | 2020-08-11 → **deslistado 2022-11-28** | tras la caída de FTX ✔ |

**734 pares USDT existieron alguna vez.** Menos 155 por clase de activo (stables, FX, oro,
tokens apalancados, acciones tokenizadas) = **579 eventos**, de los cuales **544 útiles**
(universo de control ≥30 vivos y ≥31 barras propias), entre **2019-04 y 2026-07**.

---

### La trampa de supervivencia: **existía, y por poco se cae adentro**

El preregistro (§3) puso esto como la condición que podía cerrar la corrida sola: si la
muestra fueran solo los listados **que sobrevivieron**, el drift post-listado saldría
positivo **por construcción**.

> ⚠️ **El primer conteo dijo que el 0,1% estaban deslistados, y era mentira.**
> `exchangeInfo` filtrado **por nombre** devuelve 734 pares USDT y los da todos por vivos.
> Filtrado **por estado** dice otra cosa: **485 `TRADING` y 249 `BREAK`**. Los `BREAK` son
> los deslistados, y son exactamente los que no pueden faltar.
>
> Lo que delató el error fue el mismo reflejo que en la corrida 8: **el número contradecía
> algo conocido.** "Binance nunca deslistó nada" es falso de entrada, así que el 0,1% era
> un bug, no un dato.

**Con el estado bien leído: 186 de 579 eventos (32,1%) son de símbolos hoy deslistados.**
Contra un umbral preregistrado del 5%. **La condición pasa con holgura y la muestra no
tiene sesgo de supervivencia.**

---

### Lo que la mata: la σ por evento

| h (días) | eventos | semanas | σ semanal (ATR) | **MDE (ATR)** | **MDE (%)** | veredicto |
|---|---|---|---|---|---|---|
| 1 | 544 | 266 | 2,750 | **0,472** | **4,2%** | no se pudo medir |
| 3 | 543 | 266 | 5,124 | **0,880** | **7,6%** | no se pudo medir |
| 7 | 544 | 266 | 3,847 | **0,660** | **5,7%** | no se pudo medir |
| 30 | 544 | 266 | 7,877 | **1,352** | **12,0%** | no se pudo medir |

*(umbral preregistrado: 0,10 ATR. ATR de mercado mediano en los eventos: 8,91%/día)*

**La columna en % está a propósito**, para que no haya que discutir si la unidad en ATR fue
justa: es la misma barra de error sin normalizar. Dice lo mismo.

**El origen del ruido, en una línea: el exceso de retorno de un par recién listado tiene
σ = 23,8 puntos porcentuales a un día.** Una moneda que debuta se mueve ±24% contra el
mercado en su primer día. Con esa dispersión, la media es inalcanzable.

**Cuánta historia haría falta** para detectar un efecto que valga la pena — o sea que supere
2× el costo de una vuelta, ~1,0% de exceso:

| h | σ/evento | σ/semana | semanas necesarias | **años** | hay |
|---|---|---|---|---|---|
| 1 | 23,8% | 24,4% | 4.676 | **90** | 266 |
| 3 | 38,9% | 44,3% | 15.408 | **296** | 266 |
| 7 | 38,0% | 33,1% | 8.600 | **165** | 266 |
| 30 | 87,9% | 70,2% | 38.599 | **742** | 266 |

**90 años de listados de Binance para el horizonte más favorable.** Binance tiene 9.

### Lo que sí queda descartado, dicho con precisión

El estudio **no** puede descartar un efecto operable. Sí puede descartar uno **enorme**:

> Si el listado produjera un exceso de más de **±4,2% a un día**, se habría visto. No está.

Eso descarta la versión fuerte de la historia —"comprar todo listado nuevo el primer día es
un negocio obvio"— y no descarta nada más. Un efecto de 1-2%, que sería 2-4× el costo y muy
operable, **queda por debajo del ruido y no se puede afirmar ni negar.**

---

### Lo que esto le corrige al método del repo

La regla escrita era: **"contar el n POST-JOIN y calcular el MDE con la nula real ANTES de
estimar"**. Se cumplió, y por eso esta corrida costó una tarde y no una semana.

Pero la corrida 9 muestra que **la mitad de esa regla que se usa en la práctica —contar el
n— no informa nada por sí sola**:

| | corrida 6 (on-chain) | corrida 9 (listados) |
|---|---|---|
| n independiente | 257 semanas | **266 semanas** |
| σ semanal | chica | **enorme** |
| MDE | ±0,065 ATR | 0,47-1,35 ATR |
| veredicto | **concluyó "no está"** | **"no se pudo medir"** |

**Más semanas, veredicto opuesto.** El n nunca fue el que decidía: decidía `σ/√n`. Contar
eventos y sentirse tranquilo porque son muchos es el error que esta corrida deja medido.

Y junto con la corrida 8, quedan **dos formas distintas de morir por potencia**:

- **corrida 8**: σ normal, **n imposible de conseguir** (18 meses de historia; harían falta 23 años).
- **corrida 9**: **n excelente** (266 semanas, 544 eventos, sin sesgo de supervivencia),
  σ imposible de vencer (90 años al ritmo actual).

Las dos terminan en "no se pudo medir", y **decir cuál de las dos fue es parte del
veredicto**: la primera se reabre si aparece el dato, la segunda no se reabre con más datos.

---

### Lo que reabriría esto

**Nada que sea cuestión de esperar.** Solo un estimador con mucho menos ruido por evento:

- **Condicionar el evento** en algo que corte la dispersión (tamaño del listado, sector,
  si el activo ya cotizaba en otro venue). Cada condicionante **multiplica las hipótesis**
  y hay que meterlo en el FDR — o sea que hay que preregistrar la lista completa antes.
- **Medir el anuncio en vez del listado**, con el timestamp del blog. Ahí sí hay una serie
  de precio previa **si el activo ya cotizaba en otro exchange**, y la dispersión sería la
  del activo maduro, no la del debut. **Eso sí es una corrida distinta y viable**, y es lo
  único de esta dirección que quedó vivo. Ojo: exige el scrapeo que esta corrida evitó, y
  el n cae a los activos que ya cotizaban en otro lado.

### Un detalle chico para el que siga

`LUNAUSDT` aparece con listado 2020-08-21 y estado `TRADING`: el ticker se reusó entre
LUNA (hoy LUNC) y LUNA2. Son pocos casos y no mueven la compuerta, pero si alguna vez se
corre el lote, los tickers reusados hay que separarlos a mano.

