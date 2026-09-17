# PREREGISTRO — corrida 16: flujos de ETF spot (BTC/ETH), solo la compuerta de potencia

> Escrito el **2026-08-31**, **antes de calcular un solo σ**. Lo único que se corrió antes
> es un `GET` a Farside para verificar que la tabla existe y se puede parsear: se contaron
> etiquetas HTML, no se leyó un número de flujo.
>
> Dirección §4.3 y §5.2 de `HANDOFF_FUENTES_NUEVAS.md`. Código: `banco/potencia_etf.py`.
> Resultados **debajo de la línea**.

---

## 1. Qué se pregunta

Las creaciones y redenciones diarias de los ETF spot de BTC y ETH **no son un patrón
inferido de precios**: son compras y ventas reales, publicadas, con historia desde
**enero de 2024**. Es la tercera de las cuatro fuentes que `HANDOFF_FUENTES_NUEVAS.md` §2
verificó que **no están en el repo** (cero hits de ETF en todo el árbol).

> **La pregunta: ¿el flujo neto de la semana pasada predice el retorno de la semana que
> viene, en cualquiera de las tres formas de §3?**

### Prior: BAJO, y el motivo se dice antes de bajar un dato

Los flujos **los publica todo el mundo con un día de retraso** y son de las cosas más
masticadas del retail. Si predijeran al 30 %/año, sería el trade más famoso del mercado.
`HANDOFF_FUENTES_NUEVAS.md` §3 ya lo puso en la lista de "no cruza el filtro".

**Y el problema de tamaño, escrito antes de mirar nada:** son **dos nombres** y **~20
meses**. La corrida 9 tuvo **266 semanas** y **no pudo concluir**.

**Por qué se corre igual:** porque cuesta **media tarde**, no construye nada, y el resultado
—cierre o no— vale lo mismo. Esta corrida **no mide el efecto**: calcula `n` y `σ` y decide
si el efecto es medible. Es la regla que ahorró las corridas 8, 9, 13 y 14 enteras.

---

## 2. El dato, y sus dos límites, dichos antes

**Fuente: Farside Investors**, tabla diaria pública. Verificado el 2026-08-31 (se contaron
etiquetas, no se leyeron valores):

| | |
|---|---|
| `farside.co.uk/bitcoin-etf-flow-all-data/` | **HTTP 200**, `<table class="etf">`: 682 `<tr>`, 9494 `<td>` |
| estructura | una columna por emisor (IBIT, FBTC, BITB, ARKB, …, GBTC) + **`Total`**, en millones de USD |
| primera fila | **11 Jan 2024** |
| `lxml` / `html5lib` / `bs4` | **no están instalados** → `pandas.read_html` falla |
| salida | parser con **`html.parser` de la stdlib**. **No se instala ninguna dependencia** |

**ETH arranca más tarde** (los ETF spot de ETH se lanzaron en julio de 2024), así que su
serie es más corta que la de BTC y eso baja el `n` del diseño B. Se reporta medido.

### ⚠️ Límite 1 — LOOK-AHEAD DE REVISIÓN, que es propio de esta fuente

Del handoff §4.3: **las tablas de flujos se revisan hacia atrás.** La tabla bajada hoy **no
es lo que se veía ese día**, y no hay forma de reconstruir la versión point-in-time desde el
sitio. Por lo tanto, y declarado acá antes de medir:

> **Cualquier medición construida sobre esta tabla tiene look-ahead de revisión y es un
> TECHO, no una estimación.** Si (C) llegara a pasar, el preregistro de la medición tiene
> que resolver esto primero.

Como (C) es lo único que corre en esta corrida, y (C) **no usa los valores de flujo** (§4),
este límite no muerde acá. Se escribe igual, porque es la parte que se olvida.

### ⚠️ Límite 2 — la reserva OOS cae JUSTO EN EL MEDIO

La reserva **2024-08-01 → 2025-08-01** (`PREREGISTRO_ANCHO.md`, virgen tras la corrida 15)
parte la historia de los ETF casi por la mitad. Si (C) pasara, la medición se queda con
~7 meses antes y ~12 después. Se dice ahora para que nadie lo descubra en el medio.

---

## 3. Los tres diseños, fijados acá

**Barra: la semana, no solapada.** Es la unidad independiente y es la que cuenta para el n.
La señal es el flujo neto acumulado de la semana **ya cerrada** — observable, sin lookahead
de timing (el de revisión es otra cosa y está en §2).

| | diseño | P&L por barra | σ que manda | patas de costo |
|---|---|---|---|---|
| **A** | timing directo | ± retorno semanal de BTC (y de ETH aparte) | σ del retorno semanal del activo | 1 |
| **B** | long-short BTC vs ETH | ±(r_BTC − r_ETH) | σ de la diferencia | 2 |
| **C** | el flujo condicionando el panel transversal | el spread top-k − universo de `ranking.py`, con el lado dado por el signo del flujo | σ semanal de la **nula real** del harness | 1 |

**Se gatean los TRES a propósito.** Una compuerta tiene que correr sobre el caso **más
favorable**, o un fallo no cierra nada: si el diseño de σ más baja tampoco puede, ninguno
puede. **C es el que decide**; A y B están para mostrar la escala.

**Costo:** `COSTO_PCT` (0,20 %) por rebalanceo × 52 semanas = **10,43 %/año**, la misma
convención de la corrida 13. B paga el doble por tener dos patas (igual que la cartera de la
corrida 14).

> **Y una aclaración que evita un error de lectura clásico:** una estrategia que está en el
> mercado solo parte del tiempo tiene menos σ — **y menos retorno esperado, en la misma
> proporción**. El MDE **por unidad de capital desplegado no cambia**. Por eso la σ del
> diseño A es la del activo y no hace falta saber con qué frecuencia dispara la señal.

---

## 4. LA COMPUERTA, que es todo lo que corre en esta corrida

### El hallazgo que hace que esto cueste media tarde

> **(C) no necesita los datos de flujo.** `MDE = Z·σ/√n`. La σ del P&L de una posición ±1
> sobre BTC **es la σ de BTC** — no depende de cuál sea la señal. Y `n` es un hecho de
> calendario: semanas desde el 11-ene-2024. Los dos salen de precios **ya cacheados**.

### (C) POTENCIA

```
MDE = 2,8 · sigma_semanal / sqrt(n_semanas) · 52        [%/ano]
```

> **Si el MDE del retorno neto anualizado supera los 10 %/año, el diseño se declara "no se
> pudo medir".** Si los **tres** diseños fallan, **la dirección se CIERRA.**

El umbral de 10 %/año **no es un parámetro libre**: es el mismo de las corridas 8, 13 y 14.
Aflojarlo después de ver el número sería fabricar el resultado.

Y hay que **decir cuál de las dos falló** (regla de la corrida 9, que tuvo 266 semanas y aun
así no pudo, porque decidía `σ/√n`):

- **por n** → se reabre esperando. Se reporta cuántas semanas harían falta.
- **por σ** → no se arregla con más datos. Se reporta qué σ haría falta.

### (P) LA PREMISA — que el mecanismo esté ahí

El mecanismo declarado es *"no es un patrón inferido: son compras y ventas reales, grandes
como para mover el precio"*. Eso es **verificable**, y se verifica:

```
mediana( |flujo neto diario| ) / volumen spot en USD del mismo dia
```

> **(P) pasa si esa fracción supera el 1 %.** Por debajo, el argumento de "compras reales"
> no está haciendo el trabajo que se le atribuye, y la dirección se cierra ahí — **aunque el
> MDE diera lindo**, que es la lógica que estrenó la corrida 14.

> **El denominador es el volumen spot de Binance solamente**, que es una fracción del
> volumen global. O sea que la fracción sale **más grande de lo que es** y la premisa recibe
> el beneficio de la duda. Es a propósito: una compuerta tiene que ser generosa con la
> hipótesis para que un fallo signifique algo.

### Calibración obligatoria

La misma cuenta sobre un efecto **conocido**, o el número nuevo no se interpreta:
**reproducir el MDE de la corrida 13** (22,0 %/año neto, 255 barras de 168h) sobre el panel
completo, y mostrar que la única diferencia con el diseño C es `n`. Si no reproduce, lo roto
es el código y no el mercado.

> (La corrida 15 midió 25,1 %/año sobre 203 barras porque excluyó la reserva OOS. El número
> a igualar acá es el de la 13, que corre sobre el panel entero.)

---

## 5. Lo que este preregistro NO autoriza

- **Construir un detector, un backtest o un colector** de flujos antes de que (C) pase.
  Es la regla de `HANDOFF_FUENTES_NUEVAS.md` §5 punto 5.
- **Barrer definiciones de flujo** (neto / por emisor / acumulado a 5d / normalizado por
  AUM) "a ver si con alguna baja el MDE". La σ del P&L no depende de la señal (§4), así que
  eso no puede cambiar el veredicto — solo puede fabricar uno.
- **Barrer el horizonte.** La barra es la semana y está fijada.
- **Aflojar el umbral de 10 %/año** si el MDE da 11 o 40.
- **Mirar ningún retorno condicionado al flujo.** Si esta corrida imprime uno, dejó de ser
  una compuerta.
- **Tocar la reserva OOS** ni las tres fechas preregistradas.

---

## 6. La expectativa honesta

**Se espera que los tres diseños fallen, y que fallen por σ.** La aritmética a priori, hecha
antes de correr y con σ de memoria (BTC ~8 % semanal, ~58 % anualizada):

| diseño | MDE predicho | semanas para llegar a 10 %/año |
|---|---|---|
| A (BTC ±1) | ~100 %/año | ~13.600 (**261 años**) |
| B (long-short) | ~75 %/año | ~7.500 (144 años) |
| C (transversal) | ~29 %/año neto | ~1.150 (**22 años**) |

Si eso se confirma, el resultado es **una familia más cerrada por media tarde** — y "no se
pudo medir, falló por σ" es la respuesta correcta, no un fracaso.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

**Corrido el 2026-08-31** con `banco/potencia_etf.py` (15 s). Tabla: `banco/potencia_etf.csv`.
Log: `banco/etf.log`.

## Veredicto: **NO SE PUDO MEDIR. La dirección se CIERRA.** Falla por σ.

Los tres diseños de §3, en sus **cuatro instancias** (A corre sobre BTC y sobre ETH por
separado), fallan la compuerta.

Y es el mismo patrón que la corrida 14, que se lee distinto de un cierre doble: **la premisa
PASA y la potencia falla.** Dice *"la idea era buena y el dato no alcanza"*, no *"la idea era
mala"*.

### Calibración — el aparato reproduce un número ya publicado

| | medido acá | ya publicado |
|---|---|---|
| MDE de la nula, panel completo, 255 barras de 168h | **22,0 %/año neto** (σ 1,7468 ATR) | **22,0** (corrida 13; su CSV tiene σ 1,7474 / 1,7447) |

Reproduce al decimal. El número nuevo se puede interpretar.

### (P) LA PREMISA — **PASA**, y por bastante más de lo esperado

El mecanismo declarado era *"no es un patrón inferido: son compras y ventas reales, grandes
como para mover el precio"*.

| activo | \|flujo\| diario mediano | volumen spot mediano | **fracción** | umbral |
|---|---|---|---|---|
| BTC | **190,8 M USD** | 1,96 B USD | **8,85 %** | 1 % → **pasa por 9×** |
| ETH | 47,8 M USD | 1,24 B USD | **4,15 %** | 1 % → **pasa por 4×** |

Flujo acumulado desde el lanzamiento: **+54,7 mil M USD** en BTC (677 días) y **+13,0 mil M**
en ETH (539 días) — el orden de magnitud que se sabe que es, así que el parser leyó bien la
escala.

**Los flujos de ETF NO son marginales.** Casi el 9 % del volumen spot de Binance en BTC es
mucho, y el denominador está sesgado a favor de la premisa a propósito (§4). El mecanismo
está.

### (C) POTENCIA — **NO PASA ninguno de los cuatro**

| diseño | desde | n semanas | σ semanal | **MDE %/año** | costo | BRUTO nec. |
|---|---|---|---|---|---|---|
| A timing BTC (±1) | 2024-01-11 | 132 | 5,94 % | **75,3** | 10,40 | 85,7 |
| A timing ETH (±1) | 2024-07-23 | 104 | 9,59 % | **136,9** | 10,40 | 147,3 |
| B long-short BTC/ETH | 2024-07-23 | 104 | 6,45 % | **92,2** | 20,80 | 113,0 |
| **C transversal condicionado** | 2024-01-11 | 132 | 2,86 % | **36,4** | 10,43 | **46,8** |
| umbral preregistrado | | | | **10,0** | | |

**El más favorable falla por 3,6×.** Y los cuatro fallan **por σ**, que es la mitad que no se
arregla esperando:

| diseño | harían falta | o una σ de |
|---|---|---|
| A BTC | 7.481 barras (**141 años más**) | 0,789 (la medida es 5,94) |
| A ETH | 19.495 barras (373 años más) | 0,700 (9,59) |
| B | 8.832 barras (168 años más) | 0,700 (6,45) |
| C | 1.750 barras (**31 años más**) | 0,787 (2,86) |

> **Ninguna cantidad de paciencia razonable lo destapa.** Aun el diseño más favorable pide
> 31 años más de historia, y los ETF tienen 2,6.

### La predicción de §6 contra lo medido

Vale anotarlo porque es donde se aprende algo reutilizable.

| diseño | predicho a priori | medido | |
|---|---|---|---|
| A BTC | ~100 %/año | **75,3** | σ real 5,94 %/semana, no el 8 % que puse de memoria |
| B | ~75 %/año | **92,2** | subestimé: ρ(BTC,ETH) = **+0,756**, no el ~0,85 supuesto |
| C | ~29 %/año | **36,4** | subestimé: usé la σ del panel de 5 años |

Dos lecciones concretas:

1. **BTC se calmó.** σ semanal 5,94 % ⇒ ~42,8 % anualizada, contra el ~58 % que uno tiene en
   la cabeza de ventanas anteriores. La potencia mejoró y **aun así no alcanza ni de cerca**.
2. **La σ del harness NO es una constante del repo.** En la ventana de los ETF (2024-01 →
   2026-08) la nula da **2,0384 ATR** contra 1,7468 en los 5 años completos: la sección
   cruzada de estos dos años y medio es **17 % más ruidosa**. Cualquier cuenta de potencia a
   priori que reuse la σ de la corrida 13 sobre otra ventana va a salir optimista. Es la
   misma clase de error que la corrida 14 documentó con `σ·√(2(1−ρ))`.

### Nota de método corregida durante la corrida

La primera versión hacía arrancar **todos** los diseños el 2024-01-11. Pero los ETF de ETH se
lanzaron el **2024-07-23**, así que a los dos diseños con pata de ETH les estaba regalando
**26 semanas que no existen**. Corregido: cada diseño arranca cuando existe el flujo que
necesita. El efecto fue endurecer el resultado (A-ETH pasó de 118 a 137, B de 79 a 92), no
aflojarlo, y no cambió ningún veredicto.

### El veredicto, dicho entero

> **NO se puede medir el efecto de los flujos de ETF spot** con la historia que existe
> (2,6 años, 132 semanas independientes), en ninguna de las cuatro formas de §3. El diseño
> más favorable queda en **36,4 %/año** contra un umbral de 10.
>
> **Falla por σ, no por n.** No se reabre esperando: haría falta un orden de magnitud más de
> historia (31 años en el mejor caso), no un par de años.
>
> **NO está establecido que los flujos de ETF no predigan nada.** Está establecido que **con
> este dato no se puede saber**, que es una afirmación distinta y más débil.

**Y la premisa pasó**, así que lo que cierra no es el mecanismo: es la medibilidad. Si algún
día hubiera 30 años de ETF spot, la pregunta seguiría siendo razonable.

### Lo que quedó anotado y no muerde acá

- **El look-ahead de revisión (§2) nunca entró en juego**, porque (C) no usa los valores de
  flujo: la σ de una posición ±1 sobre BTC es la σ de BTC. Los flujos se usaron **solo** para
  (P), donde una revisión hacia atrás no cambia un orden de magnitud.
- **La reserva OOS 2024-08 → 2025-08 no se tocó** y sigue virgen. Habría partido la historia
  de los ETF casi por la mitad si se hubiera llegado a medir.
- **No se construyó nada**: ni colector, ni cron, ni backtest. Es la regla de
  `HANDOFF_FUENTES_NUEVAS.md` §5 punto 5, y esta corrida es exactamente el caso para el que
  está escrita.

### Comparación con la corrida 15, que es lo que ordena el mapa

| corrida | fuente | resolución alcanzada | qué pasó |
|---|---|---|---|
| 15 | macro / cross-asset | **35,5 %/año bruto** | se **midió**: 0 de 120 brazos. Cierre **por efecto**, acotado |
| 16 | flujos de ETF | **46,8 %/año bruto** | **no se pudo medir**. Cierre **por potencia**, falla por σ |

Son cierres distintos y conviene no confundirlos: en la 15 se miró y no había nada del tamaño
que el harness ve; en la 16 no se llegó a mirar.

### Código nuevo

| archivo | qué es |
|---|---|
| `banco/potencia_etf.py` | la compuerta: parser stdlib de Farside, (P), (C) sobre cuatro diseños, calibración. **No mira ningún retorno condicionado al flujo** |
| `banco/.etf_cache/` | caché de las dos tablas de Farside |
| `banco/potencia_etf.csv` | n / σ / MDE por diseño |
