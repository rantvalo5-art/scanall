# PREREGISTRO — skew de opciones y prima del radar: los dos colectores

> Escrito el **2026-09-19**, con **n = 0 filas** en las dos series. No hay ningún dato
> que mirar todavía, y ése es exactamente el momento en que esto tiene que escribirse.
>
> Dirección §4.1 y §5.3 de `HANDOFF_FUENTES_NUEVAS.md`. Código: `opciones/cadena.py`,
> `opciones/juntar_skew.py`, `opciones/prima_radar.py`.
>
> **Esto no preregistra una corrida: preregistra una ESPERA.** Las dos series tardan
> meses o años en ser medibles, y el único propósito de este documento es que, cuando
> lleguen, la pregunta y la regla ya estén fijadas y nadie las elija después de ver el
> número.

---

## 1. Por qué se junta algo que no se puede medir

El repo tiene una regla que mató doce familias y funciona: **no construir infraestructura
para una candidata que no puede cruzar el umbral de ~30%/año bruto** (`HANDOFF_FUENTES_NUEVAS.md`
§3). Esto parece violarla y no la viola, porque hay una asimetría que solo aplica a dos
tipos de dato:

> **La cadena de opciones vencida NO EXISTE en ningún venue.** Verificado contra Deribit
> el 2026-08-31: `get_instruments(expired=true)` devuelve 56 instrumentos de **un solo**
> vencimiento, y pedir velas de uno vencido da `status: no_data`. No se puede comprar, no
> se puede bajar, no se puede reconstruir.

O sea que la decisión de hoy no es *"¿mido esto?"* sino *"¿existe este dato dentro de tres
años?"*. Para todo lo demás que el repo midió —klines, funding, OI, on-chain— la respuesta
era sí gratis, y por eso ninguna corrida anterior necesitó un colector. Acá es no.

El costo de arrancar: **~40 líneas en un cron que ya existe** más uno nuevo de 2h. El costo
de no arrancar: dentro de tres años se está exactamente donde hoy.

---

## 2. Las dos series, y por qué son preguntas distintas

### 2.1 `skew_diario/` — la inclinación, una vez por día

El risk reversal a 25 delta, `rr25 = IV(put 25Δ) − IV(call 25Δ)`, más la mariposa y la IV
ATM, para el vencimiento más cercano a 30 días. Seis monedas (BTC, ETH, SOL, XRP, DOGE,
HYPE) en Bybit y las tres que también están en OKX.

**Por qué es otra clase de variable.** Las doce familias medidas son, todas, patrones en
precios pasados. El rr25 es el precio que el mercado **paga hoy** por protegerse en una
dirección: una cotización de asimetría, no un patrón inferido. En FX y en acciones es *la*
variable canónica de posicionamiento direccional. Es el único de los cuatro candidatos del
handoff que el filtro no mató de entrada — y lo dejó pasar como **"quizá, y solo en las
colas"**, cuando el seguro direccional se encarece de golpe.

**El delta lo sirve el venue.** Bybit (`markIv`, `delta`) y OKX (`markVol`, `delta`) dan el
delta calculado, así que el rr25 sale eligiendo el instrumento más cercano al objetivo
**sin interpolar el smile**, que es la parte que normalmente ensucia esta medición.

### 2.2 `prima_radar/` — el precio de la magnitud, cada 2 horas

La foto de lo que costaba el **straddle del vencimiento más corto** en cada barra del
radar: IV ATM, prima en % del subyacente, y la IV a 30d de contexto.

**Qué agujero tapa.** El único hallazgo vivo del repo es que se puede predecir MAGNITUD, y
no se puede cobrar, porque magnitud no dice dirección. Lo que paga por movimiento sin
dirección es un straddle. Lo que falta para cerrar ese circuito **no es otro predictor**:
es el precio del straddle en el momento del disparo, que se evapora cada 4 horas.

**Medido el 2026-09-19 sobre 23 días de `radar_runs`** (752 elegidos, 94 barras):

| universo | elegidos con opciones listadas |
|---|---|
| desplegado (169 símbolos, la cola) | **3 de 752 = 0,4%** |
| calibración (los 46 del pin `deriv46`) | **75 de 752 = 10,0%** |

El 10,0% es **exactamente la tasa base** (5 de 46 nombres tienen opciones), o sea que el
radar no elige preferentemente nombres con instrumento — pero tampoco los evita, y 75
eventos en 23 días es una tasa medible. El 0,4% del universo desplegado es, de paso, un
segundo argumento **independiente del de los costos** de por qué esa cola no es un negocio:
no tiene con qué cobrarse.

**La referencia de precio, tomada hoy** (Bybit, straddle a ~0,44 días):

| | BTC | ETH | SOL | XRP | DOGE | HYPE |
|---|---|---|---|---|---|---|
| straddle, % del subyacente | 0,52% | 0,80% | 1,20% | 1,48% | 1,33% | 1,78% |
| IV ATM | 18,6% | 28,7% | 43,0% | 52,3% | 47,8% | 63,2% |
| **ancho del libro, puntos de IV** | **0,67** | **1,39** | **6,55** | **21,46** | **22,25** | **18,73** |

Ésa es la barra que el filo de magnitud tendría que superar, y está escrita antes de
medirlo.

### ⚠️ Y la fila del ancho del libro ya dice algo, antes de juntar un solo dato

En BTC el spread es **0,67 puntos sobre una IV de 18,6%** — 3,6% de la IV, del orden de lo
que la corrida 8 midió (1-2% de la prima) para vencimientos de 30 días. En las alts es otra
cosa: **21,5 puntos sobre 52,3% en XRP, 22,3 sobre 47,8 en DOGE**. Eso es ~40% de la IV, y
para una opción ATM la prima se mueve casi proporcional a la IV: cruzar cuesta del orden de
**un quinto de la prima por pata**.

Dicho de otro modo, y conviene que esté escrito ahora y no después de ver un resultado
lindo: **en las cuatro alts, el costo de entrar y salir se come el filo antes de que exista
la pregunta.** Si esta dirección vive en algún lado, es en **BTC y ETH**, donde el libro es
angosto — y ahí el straddle corto cuesta 0,52% y 0,80%, que es mucho comparado con lo que
un filo de magnitud a 4h podría rendir.

Esto **no** cambia la decisión de juntar: son las mismas ~40 líneas y el dato sigue siendo
irrecuperable. Cambia la expectativa, que es lo que hay que fijar antes.

---

## 3. La pregunta de `prima_radar`, fijada ahora

> Cuando el radar pone un nombre en el top-8, ¿ese nombre se mueve **más de lo que la
> opción ya cobraba**?

**El estadístico.** Por cada barra en que el radar elige un nombre con opciones, con la
foto de prima más cercana (±30 min):

```
pnl = |S(t+4h) − K| − prima        (K = strike ATM, todo en % del subyacente)
```

**Es un estimador CONSERVADOR a propósito**: liquida a valor intrínseco e ignora el valor
temporal que le queda a la opción a las 4h, así que subestima el resultado. Si aun así da
positivo, el signo no es un artefacto del estimador. Al revés no vale: un negativo acá no
prueba que no haya nada.

**El control va POR BARRA**, como en todo el repo: los mismos 5 nombres en las barras en
que el radar **no** los eligió. La diferencia elegido − no elegido es el número que decide.
Nunca "el straddle rinde": eso es una apuesta sobre el nivel de la volatilidad, que es otra
pregunta y ya está cerrada (corrida 8).

**Los costos van siempre, y son dos**: cruzar el spread de la opción, medido en la corrida
8 en **1-2% de la prima**, y el spread en IV entre `bid1Iv` y `ask1Iv`, que el colector
guarda para poder cobrarlo después en vez de estimarlo.

---

## 4. Las dos compuertas, y CUÁNDO se puede mirar

Primero (C) y (P), como en las corridas 14, 15 y 16. **Ninguna de las dos se puede correr
hoy**: con n = 0 no hay σ, y sin σ no hay MDE.

### (C) POTENCIA — la fecha no se fija por calendario, se calcula

`MDE = 2,8 · σ / √n`, con σ medido sobre el propio dato acumulado y n **de-solapado por
barra** (dos disparos a menos de 4h comparten futuro).

- **`prima_radar`**: a 3,26 eventos/día, n = 200 cae alrededor del **2026-11-20**. Ésa es
  la fecha en que se corre **la compuerta**, no la medición. Si el MDE supera el costo de
  ida y vuelta del straddle, el veredicto es **"no se pudo medir"** y se sigue juntando.
- **`skew_diario`**: la aritmética ya está hecha y es desalentadora, y va escrita para que
  nadie se ilusione: dentro de un año son 12 meses de historia, que es **exactamente donde
  murió la corrida 8** (18 meses de implícita para SOL → MDE 39%/año contra un umbral de
  10%). **Esto no es medible antes de ~3-4 años.** Antes de **2028-09** no se corre nada.

### (P) LA PREMISA — antes del signo

Para `prima_radar`: **el filo de magnitud tiene que existir en estos 5 nombres**. El +0,511
se midió sobre los 46 del pin; que sobreviva en el subconjunto de 5 no está dicho en ningún
lado. Si la premisa falla, el resto no se mira — y falla distinto de que falle la potencia:
dice *"la idea era mala"*, no *"el dato no alcanza"*.

---

## 5. Lo que este preregistro NO autoriza

- **Tocar el radar.** Está corriendo un forward test preregistrado que acaba de replicar
  (+0,593 contra un MDE de ±0,476). Ni `n_surge`, ni `k`, ni el universo.
- **Elegir el vencimiento después de ver el resultado.** La regla está en el código: el
  más cercano a 30 días para el skew, el más corto disponible para la prima, y a igualdad
  de distancia el que tenga más instrumentos. Por calendario, nunca por IV.
- **Barrer deltas.** Es 25Δ porque es la convención del mercado, no porque se probaron
  varios. Probar 10Δ, 25Δ y 35Δ y quedarse con el que dé lindo es fabricar tres brazos y
  reportar uno.
- **Poolear las seis monedas sin verificar la ρ.** El molde es
  `opciones/potencia_transversal.py`: si el factor común no se removió, el n efectivo es
  mucho más chico que el nominal.
- **Rellenar huecos.** Si un día el venue no devuelve cadena, esa fila no existe. No se
  interpola: una serie con huecos tapados por defaults es peor que una serie corta.

---

## 6. La expectativa honesta

Lo más probable, y con bastante margen, es que **las dos terminen en "no se pudo medir"**.
Es lo que le pasó a las corridas 8, 9, 14 y 16, todas por σ o por n, y no hay ninguna razón
para creer que ésta sea distinta.

Lo que cambia es el costo del error. Si se junta y no sirve, se perdieron ~40 líneas y unos
centavos de Actions. Si no se junta y hubiera servido, la respuesta **no se puede recuperar
nunca** — y ésa es la única razón por la que esto existe.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

_(en blanco: n = 0 el 2026-09-19. La primera compuerta que se puede correr es la de
`prima_radar`, alrededor del 2026-11-20.)_
