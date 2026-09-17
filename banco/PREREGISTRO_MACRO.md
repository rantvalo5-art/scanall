# PREREGISTRO — corrida 15: macro / cross-asset como condicionamiento del panel cripto

> Escrito el **2026-08-31**, **antes de evaluar un solo brazo**. Lo único que se corrió
> antes de escribirlo es la descarga y la alineación (`banco/macro.py --cobertura`), que no
> mira ningún resultado: cuenta filas y mide desfase.
>
> Dirección §4.2 y §5.1 de `HANDOFF_FUENTES_NUEVAS.md`. Código: `banco/correr_macro.py`.
> Resultados **debajo de la línea**.

---

## 1. Qué se pregunta

Las catorce corridas midieron doce familias y **todas** son patrones en precio, volumen,
derivados u on-chain **de cripto**. `HANDOFF_FUENTES_NUEVAS.md` §2 verificó con grep que
macro / cross-asset no aparece en ningún lado del árbol.

**Por qué no lo cubre la familia "régimen", que ya está cerrada.** Aquellos eran **7
detectores INTERNOS a cripto** (BTC sobre su EMA, breadth, vol de mercado), medidos sobre
22 trimestres, 0 pasaron. Condicionar por una variable **de afuera del sistema** es una
familia distinta y nunca se probó.

> **La pregunta: ¿la exposición de una moneda a un factor de afuera (Nasdaq, dólar, oro,
> tasas, VIX) predice su rendimiento RELATIVO al resto del panel en la barra siguiente?**

Es transversal a propósito. La versión de serie de tiempo —"¿el Nasdaq de ayer predice
cripto mañana?"— es lo primero que probaría cualquiera con acceso a los datos, que son
todos, y además hereda entero el ruido del factor común que mató a la corrida 8.

### Prior: BAJO, y se dice antes de correrlo

Que BTC correlacione con el Nasdaq es un hecho **contemporáneo**, no predictivo. Y para el
brazo primario (§3) hay un argumento en contra que hay que escribir ahora y no después:
**cripto opera 24/7 y ya cotizó a través de toda la semana de rueda.** Para cuando la barra
se forma (domingo 00:00 UTC) la transmisión contemporánea **ya ocurrió**. Lo que queda es
continuación o sobrerreacción, y no hay razón fuerte para esperar ninguna de las dos.

**Por qué se corre igual:** cuesta una tarde, corre sobre `banco/ranking.py` sin construir
infraestructura, y el valor esperado está en **cerrar la familia**, no en encontrar un
negocio. Eso es honesto y es suficiente.

---

## 2. El dato, y su límite, dicho antes

**Fuente: Yahoo Finance**, endpoint público de charts, sin dependencias nuevas. Se probaron
antes las dos alternativas y **ninguna sirve desde esta máquina** (verificado el
2026-08-31, no de memoria):

| fuente | resultado |
|---|---|
| `yfinance` / `pandas_datareader` | no están instalados |
| FRED (`fredgraph.csv`, sin key) | **ReadTimeout** en las 5 series |
| Stooq (`q/d/l`) | HTTP 200 pero devuelve un **desafío de JavaScript**, no el CSV |

**Cinco factores, uno por eje económico** (descargados y verificados):

| corto | ticker | qué es | ruedas | desde |
|---|---|---|---|---|
| NDX | `^NDX` | Nasdaq 100 | 1570 | 2020-06-01 |
| DXY | `DX-Y.NYB` | índice dólar ICE | 1573 | 2020-06-01 |
| ORO | `GC=F` | futuro de oro COMEX | 1573 | 2020-06-01 |
| T10 | `^TNX` | rendimiento del 10 años | 1570 | 2020-06-01 |
| VIX | `^VIX` | volatilidad implícita del SPX | 1571 | 2020-06-01 |

> **No entra el 2 años.** Un factor por eje: `^TNX` ya ocupa el de tasas. Meter 2a y 10a es
> meter dos versiones de la misma variable y pagarlas dos veces en el FDR.

### El desfase, que es donde vive el lookahead

Regla, fijada acá:

1. El cierre de la rueda `D` se declara disponible recién a las **00:00 UTC de `D+1`**. El
   cash de EEUU cierra 20:00–21:00 UTC → la regla es conservadora por ≥ 3 h.
2. Después se rellena **hacia adelante**. Hacia adelante es información de ayer y está
   bien; **hacia atrás es lookahead** y no se hace en ningún lado.
3. Los retornos del factor se calculan **entre ruedas**, y los de cripto **sobre esos
   mismos intervalos**. Sin esto, el viernes→lunes de cripto (3 días) se compararía contra
   una sola rueda del Nasdaq y la beta mediría calendario.

**Y la grilla ayudó:** con `paso=168` sobre la ventana 2021-08-01 → 2026-08-01 las barras
caen **todas en domingo 00:00 UTC** (verificado: `dias de la semana: ['Sunday']`). Cada
barra ve la semana de rueda **entera y ya cerrada**, con dos días de margen, y el desfase
es idéntico en todas. Medido:

| | |
|---|---|
| barras | **255** (187 pares, 25.957 filas) |
| desfase mediano / p95 | **2 d / 2 d** (el cierre del viernes) |
| barras con hueco > 5 d | **0,00 %** |
| cobertura de la beta sobre las filas del tablero | **94,0 %** |

---

## 3. La construcción, fijada acá

**Panel:** `base200`, ventana **2021-08-01 → 2026-08-01**, `paso = horizonte = 168h`, sin
solape, `top-k = 20`, costo `COSTO_PCT` (0,20 % por rebalanceo = **10,43 %/año** a este
horizonte). Todo son los defaults de `ranking.py` y de la corrida 13; **no se barre nada**.

**168h y no otro horizonte**, elegido antes y por una razón: corrida 13 midió que los tres
son un empate en efecto bruto necesario (32,4 / 32,6 / 30,6 %/año), y 168h es el que deja
**255 barras independientes** contra 58 y 19. Con 19 bloques el bootstrap y la compuerta de
"≥ 60 % de semanas" no significan nada. No se prueba ningún otro horizonte.

### ⚠️ La reserva OOS se excluye de la ventana

`PREREGISTRO_ANCHO.md` declara la reserva **2024-08-01 → 2025-08-01**, y
`PREREGISTRO_TRANSVERSAL.md` §6 confirma que sigue **virgen**. La ventana de 5 años la
contiene. Por lo tanto:

> **Se descartan las barras cuya fecha caiga dentro de la reserva.** Quedan dos tramos
> disjuntos (2021-08 → 2024-08 y 2025-08 → 2026-08). El bootstrap de bloques por semana no
> necesita que sean contiguos.

Costo asumido: se pierden ~52 de 255 barras, o sea el MDE sube ~12 % (√(255/203)).
**Preferible a quemar la reserva**, que es lo único que queda para confirmar cualquier cosa
que sobreviva.

### Los scores

Beta móvil de **90 ruedas** (mín. 60) de cada par contra cada factor, estimada **solo con
pasado**. Por factor `F`:

| score | qué es | transversal? |
|---|---|---|
| `beta_F` | exposición del par al factor | **sí** (varía entre pares) |
| `beta_F × d_F` | esa exposición, **condicionada** por lo que hizo el factor en la semana de rueda que acaba de cerrar (`d_F` = log-retorno de 5 ruedas) | **sí** |

> **`d_F` sola NO es un score** y no entra como brazo: es constante dentro de la barra, así
> que su z-score transversal es 0 y no ordena nada. Solo existe como interacción. Y como
> `d_F` es un escalar por barra, `beta_F × d_F` produce **exactamente el mismo top-k que
> `signo(d_F) · beta_F`**: el brazo es *"rankeá por beta, pero dá vuelta el lado según lo
> que hizo el factor la semana pasada"*. Eso es literalmente el condicionamiento por una
> variable de afuera, y se dice así para que sea interpretable.

**Residualización contra `roc_168`.** El riesgo obvio es que una beta a 90 ruedas sea
momentum reempaquetado. Cada score entra también en su versión residualizada contra el
momentum del propio horizonte de la barra, con la misma regresión transversal por barra que
usa `ranking.py`. Se elige `roc_168` y no el `roc_24` default **porque la barra es de
168h**; se fija acá y no se prueba el otro (probar los dos sería barrer).

**El lote:** 5 factores × 2 formas × (crudo + residualizado) × 2 direcciones × 3 objetivos
= **120 brazos**, más 18 de control. FDR sobre el lote entero con los controles adentro.

### La dirección se declara ACÁ

El repo midió dos veces que el brazo más tentador estaba **invertido** (corrida 7: 3 de los
5 mejores; corrida 12: el mejor de todos). Entonces:

**Brazo primario — transmisión (`beta_F × d_F`): POSITIVA.** Si el factor subió, las
monedas más expuestas siguen. Prior débil por §1, y por eso es primario solo en el sentido
de que tiene un mecanismo escrito, no en el de que se espere que gane.

**Brazos de nivel (`beta_F`): la dirección de PRIMA DE RIESGO**, que no es la misma para
los cinco porque no todos son activos de riesgo:

| factor | dirección declarada | por qué |
|---|---|---|
| NDX | beta **alta** | exposición al activo de riesgo cobra prima |
| ORO | beta **alta** | ídem, activo real |
| DXY | beta **baja** | dólar arriba es risk-off: la exposición al riesgo es beta negativa |
| VIX | beta **baja** | ídem, VIX arriba es risk-off |
| T10 | beta **baja** | tasas arriba castiga a los activos de duración larga |

La dirección contraria de cada uno **se corre igual, marcada como exploratoria**, y paga su
lugar en el FDR. Correr las dos con el FDR adentro es honesto; elegir el signo después de
verlo no lo es.

### Los controles, que tienen que poder GANAR

Un control que no puede ganar no es un control (regla del repo: una ruptura pelada le ganó
a las cinco figuras de gráfico). Entran, en el mismo lote y bajo el mismo FDR:

- **3 rankings al azar** — la nula real.
- **3 brazos de cripto ya conocidos**: `roc_168 [bajo]` (el fade, la única familia viva),
  `atr_24` y `roc_24`. Si los macro no le ganan ni a estos, está dicho todo.

---

## 4. LAS COMPUERTAS, que corren ANTES de mirar un brazo

### (P) LA PREMISA — que haya una sección cruzada de betas, y no ruido de estimación

Todo el diseño supone que **las monedas difieren en su exposición a cada factor**. Si todas
tienen la misma beta, rankear por beta es rankear ruido de estimación y el argumento entero
es falso **aunque el MDE diera lindo**. Es verificable y se verifica primero:

```
sd_transversal(beta_F)  en cada barra        contra
se(beta_F)              el error estandar de UNA beta
```

La varianza transversal observada es `varianza_verdadera + varianza_de_estimación`.
Pedir `sd_transversal / se > 1,5` implica una dispersión verdadera de ~1,1 · se, o sea
claramente por encima del ruido.

> **(P) pasa para el factor F si la mediana por barra de `sd_transversal(beta_F) / se(beta_F)`
> es > 1,5.** Un factor que no lo cumple **se saca del lote antes de evaluarlo** y se anota
> por qué. Si no lo cumple ninguno, la corrida se cierra ahí.

### (C) POTENCIA — no se re-pregunta, se HEREDA; lo que se verifica es no PERDERLA

Hay que decir esto claro porque **corrige a `HANDOFF_FUENTES_NUEVAS.md` §4.2**, que dice
que macro *"muere por efecto, no por potencia, porque hay historia de sobra"*. **La
historia que ata no es la del macro: es la del panel cripto.** El MDE de `ranking.py`
depende de la **nula** (rankings al azar), y la nula no sabe con qué score se rankea. O sea:
**el MDE de un brazo macro es exactamente el mismo que midió la corrida 13.**

| corrida 13, 168h, 255 barras | |
|---|---|
| MDE neto | **22,0 %/año** |
| costo | 10,4 %/año |
| **efecto BRUTO detectable** | **32,4 %/año** |

Entonces esta corrida **no puede** resolver un efecto macro modesto, y eso se sabe **antes**
de correrla. Lo que (C) verifica acá es otra cosa:

1. **No perder potencia por alineación.** Las semanas efectivas de los brazos macro tienen
   que ser **≥ 0,90 ×** las de la nula sobre el mismo panel. Si caen más, el diseño perdió
   barras por cobertura (`NaN` de beta) y hay que arreglar la alineación, no seguir.
2. **Un piso absoluto, para que la corrida sea falsable.** La aritmética dice que sacar la
   reserva sube el MDE bruto de 32,4 a ~36 %/año. **Si el MDE bruto medido supera los
   45 %/año, se aborta como "no se pudo medir"** — ese margen tolera una sorpresa real pero
   no un diseño que perdió la mitad de las barras. El umbral se escribe acá, con la cuenta,
   y no se afloja después de verlo.

> ### Y la consecuencia, escrita antes: el cierre está ACOTADO
>
> Si no sobrevive nada, **el veredicto NO es "no hay efecto macro"**. Es:
>
> **"no hay un efecto macro transversal mayor a X %/año bruto en 187 pares y 4 años"**,
> con X = el MDE que reporte la nula.
>
> Es exactamente la resolución a la que están cerradas las otras nueve familias
> (`HANDOFF_FUENTES_NUEVAS.md` §1), ni más ni menos. Cerrar a esa resolución **es** el
> resultado que justifica la tarde. Decir más que eso sería mentir.

---

## 5. Solo si (P) y (C) pasan: las compuertas de la medición

Son las de `ranking.py` y ninguna se afloja después de ver un número.

1. **`spread > 0`** y **`spread_crudo > 0`** — el signo no puede depender de la
   normalización (artefacto de escala).
2. **Fuera del MDE del azar.**
3. **FDR (Benjamini-Hochberg, q = 0,10) sobre el LOTE ENTERO**, controles adentro.
4. **`sin_top3` y `sin_top1` > 0** — concentración.
5. **≥ 60 % de semanas con spread > 0.**
6. El p que decide es el de **bloques por semana**, no el binomial.

**Y si algo sobrevive:** va derecho a la reserva OOS **2024-08-01 → 2025-08-01**, que por
§3 quedó afuera de la ventana justamente para esto. Sin ese paso no se le cree nada.

---

## 6. Lo que este preregistro NO autoriza

- **Barrer la ventana de la beta** (90 ruedas está fijado; no se prueban 30, 60, 250).
- **Barrer el horizonte** ni el `top-k` ni el costo.
- **Agregar factores** después de ver el resultado de los cinco.
- **Probar el otro neutralizador** (`roc_24`) si `roc_168` no da.
- **Mirar la reserva OOS** salvo que algo pase las seis compuertas de §5.
- **Aflojar el umbral de 45 %/año** de §4, ni reinterpretar un cierre acotado como un
  cierre absoluto.
- **Volver a correr con `d_F` de otra longitud** (5 ruedas = la semana; está fijado).

---

## 7. La expectativa honesta

**Se espera que no sobreviva nada**, y que el resultado sea una familia más cerrada a la
resolución del harness. Si aparece un sobreviviente, lo más probable es que sea un artefacto
de escala o de concentración y que muera en §5; y si pasa §5, sigue sin creerse hasta la
OOS.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

**Corrido el 2026-08-31** con `banco/correr_macro.py` (34 s). Tabla: `banco/rank_macro.csv`.
Log: `banco/macro.log`.

## Veredicto: **la familia se CIERRA a la resolución del harness.** 0 de 120 brazos macro.

Las dos compuertas **pasaron** y la medición se hizo entera. Es un cierre por efecto, no un
"no se pudo medir" — pero **acotado**, y el acotamiento es parte del resultado.

### Calibración — el aparato reproduce estructura conocida

No estaba en el preregistro y se agregó al correr, por la regla del repo de que un número
que contradice una estructura de mercado conocida es un bug hasta que se demuestre lo
contrario. Es contemporánea y descriptiva: **no mira ningún retorno futuro.**

| factor | beta mediana del panel | % de filas con beta > 0 | corr. diaria con BTC | esperado |
|---|---|---|---|---|
| NDX | **+1,284** | 89,3 % | **+0,415** | + activo de riesgo ✔ |
| DXY | **−1,385** | 19,3 % | −0,158 | − dólar arriba = risk-off ✔ |
| ORO | +0,289 | 66,2 % | +0,096 | + activo real ✔ |
| T10 | −0,021 | 44,3 % | −0,005 | − tasas ✔ *(nominal)* |
| VIX | −0,223 | **4,2 %** | **−0,361** | − vol arriba = risk-off ✔ |

La alineación no está rota: cripto sale con beta +1,28 al Nasdaq y correlación diaria
+0,415, que es exactamente lo que se sabe de este mercado.

> ⚠️ **T10 cumple el signo pero es un factor NULO en esta ventana.** Beta mediana −0,021 y
> correlación −0,005. Contarlo en el "5 de 5" sería sobrevenderlo: lo honesto es decir que
> **cuatro factores tienen relación real con cripto y el quinto no tiene ninguna.** Esto
> importa para leer el resultado de abajo.

### (P) LA PREMISA — **PASA los cinco, y por lejos**

| factor | sd transversal de la beta | se(beta) | razón | sd verdadera implícita |
|---|---|---|---|---|
| NDX | 0,5519 | 0,0478 | **11,56** | 0,5499 |
| DXY | 1,2952 | 0,1632 | 7,94 | 1,2849 |
| ORO | 0,4852 | 0,0669 | 7,25 | 0,4806 |
| T10 | 0,2386 | 0,0320 | 7,45 | 0,2364 |
| VIX | 0,1140 | 0,0097 | **11,78** | 0,1136 |

Umbral preregistrado: 1,5. El más flojo lo pasa por **5×**. El ruido de estimación es
despreciable: la dispersión verdadera es prácticamente igual a la observada. **Las monedas
sí difieren en su exposición a los factores externos, y el ranking ordena algo real.**

### (C) POTENCIA — **PASA**, y el número es el que la aritmética anticipó

| | |
|---|---|
| barras (168h, sin solape, reserva afuera) | **203** de 255 |
| MDE largo / corto | 0,3628 / 0,3619 ATR = **25,1 %/año neto** |
| MDE **magnitud** | **6,3623 ATR** (18× el de largo/corto — ver la nota de método) |
| costo | 10,43 %/año |
| **efecto BRUTO detectable** | **35,5 %/año** |
| piso absoluto preregistrado | 45 %/año → **pasa por 9,5 pp** |

§4 predijo ~36 %/año sacando la reserva (32,4 × √(255/203)). Se midió **35,5**.

**(C.1) cobertura:** 195 semanas en los brazos macro contra 203 de la nula → razón
**0,961**, umbral 0,90. **Pasa.** La alineación no cuesta potencia.

### La medición: 0 de 120

138 brazos (40 scores macro × 3 objetivos + 18 de control), FDR q = 0,10 sobre el lote
entero con los controles adentro.

**Lo mejor que hubo, por objetivo:**

| objetivo | mejor brazo macro | spread | p | murió en |
|---|---|---|---|---|
| largo | `beta_VIX [bajo]` | +0,101 ATR | 0,233 | **dentro del MDE del azar** (±0,363) |
| corto | `beta_T10 × d_T10 ~ sin roc_168 [bajo]` | +0,330 ATR | 0,021 | **FDR** |
| magnitud | `beta_VIX ~ sin roc_168` | +7,50 ATR | 0,000 | **artefacto de escala** (atr_ratio 0,78) |

Tres cosas que vale anotar porque no eran obvias:

1. **El mejor brazo de toda la corrida está en el factor NULO.** `beta_T10 × d_T10 [bajo]`
   dio p = 0,021 — y T10 es el único de los cinco cuya correlación con cripto es −0,005.
   **Así se ve un falso positivo**, y es exactamente para lo que está el FDR: con 120
   brazos, un p de 0,02 es lo que se espera del azar.
2. **Y está en la dirección EXPLORATORIA, no en la declarada.** Para la interacción se
   declaró POSITIVA (transmisión) y el que asomó fue el espejo `[bajo]`. Es la tercera vez
   que el repo ve esto (corridas 7 y 12) y la tercera vez que declarar el signo antes evita
   cobrarlo.
3. **Los nueve brazos de magnitud que asomaron son todos artefacto de escala.** Eligen
   nombres 20-24 % más quietos que el universo (`atr_ratio` 0,76-0,81) y su spread crudo es
   **negativo**. La compuerta v3 de `ranking.py` —normalizar por `atr_base` y exigir el
   mismo signo sin normalizar— hizo todo el trabajo acá.

### El control que PUEDE ganar, y ganó

`CTRL roc_24` sobre **magnitud** sobrevive las seis compuertas:

| spread | crudo | atr_ratio | sin_top3 | sin_top1 | semanas > 0 | p bloques |
|---|---|---|---|---|---|---|
| **+6,47 ATR** | +0,036 | 1,30 | +1,86 | +2,20 | **70,9 %** | **0,0025** |

Es el efecto del radar, que el repo ya conocía. **Su valor acá no es el hallazgo: es la
prueba de que el instrumento no está ciego** en este panel, estas barras y este mismo lote
de FDR. Sin un control capaz de ganar, "0 sobrevivientes" no se distingue de "el aparato no
mide nada".

> Y el reverso, que acota el resultado: **`CTRL roc_168 [bajo]` —el fade, la única familia
> direccional viva del repo— NO sobrevive** (spread +0,181 ATR, p = 0,0775, muere en el
> FDR). Esta corrida **no puede detectar ni el efecto direccional que el repo ya sabe que
> existe.** Cualquier lectura de "0 de 120" tiene que pasar por ahí.

### Nota de método: un error de unidades en la compuerta del MDE

En la primera corrida se le pasó a `lote_rankings` un `mde` **escalar**, el de largo/corto
(0,363 ATR). Pero `y_magnitud = (runup − caida)/atr` no está en la misma escala que
`y_largo = ret/atr`: el MDE de magnitud es **6,36 ATR, 18× más grande**. O sea que la
compuerta de magnitud estaba prácticamente **inerte**.

Se corrigió: `ranking.lote_rankings` ahora acepta un dict **por objetivo** (el escalar sigue
funcionando igual, así que no cambia ninguna corrida anterior), y `correr_macro.nula_anual`
calcula los tres. **La corrección es más estricta, no más laxa, y no cambió ningún
veredicto** — los brazos de magnitud que asomaban ya morían antes, en el artefacto de
escala, que se evalúa primero. Se deja anotado porque el error es reutilizable: *el MDE hay
que calcularlo en la escala del objetivo, y magnitud no está en la de largo.*

### El veredicto, dicho entero

> **NO hay un efecto macro transversal mayor a ~36 %/año BRUTO** en 187 pares, 203 barras de
> 168h y 4 años de historia (2021-08 → 2024-08 y 2025-08 → 2026-08), usando NDX, DXY, oro,
> 10 años y VIX como exposición y como condicionamiento.
>
> **NO está establecido que no haya uno más chico.** Un edge macro de 8-15 %/año habría sido
> invisible acá, igual que en las catorce corridas anteriores. Eso pide años, no otro
> estimador.

Es la misma resolución a la que están cerradas las otras nueve familias
(`HANDOFF_FUENTES_NUEVAS.md` §1), ni más ni menos.

**Y lo que el cierre NO dice:** no dice que a cripto no le importe el macro. La calibración
muestra lo contrario — beta +1,28 al Nasdaq, correlación diaria +0,415, VIX negativo en el
96 % de las filas. Esa relación es real y es **contemporánea**. Lo que está medido es que
**explotarla transversalmente a 168h no vale más de 36 %/año bruto**, que es otra cosa.

### Corrección a `HANDOFF_FUENTES_NUEVAS.md` §4.2

El handoff decía que macro *"muere por efecto, no por potencia. Hay historia de sobra, así
que si da cero, el cero es informativo y la familia queda cerrada de verdad."*

**La primera mitad es cierta y la segunda no.** La historia que ata no es la del macro —que
tiene décadas— sino la del **panel cripto**, y el MDE de `ranking.py` sale de la nula, que
no sabe con qué score se rankea. **Un brazo macro hereda exactamente el MDE de la corrida
13.** El cero es informativo, sí, pero **a 36 %/año**, no "de verdad". Un cierre acotado es
un resultado; presentarlo como absoluto sería el mismo error que el repo evitó catorce veces.

### Lo que quedó sin tocar

- **La reserva OOS 2024-08-01 → 2025-08-01 sigue virgen.** Se excluyó de la ventana a
  propósito y no hizo falta gastarla, porque no sobrevivió nada.
- Las tres fechas preregistradas (radar ~8-sep y ~14-oct, fade 19-oct) no se miraron.
- No se barrió ningún parámetro: una ventana de beta (90 ruedas), un horizonte (168h), un
  neutralizador (`roc_168`), un `d_F` (5 ruedas), cinco factores.

### Código nuevo

| archivo | qué es |
|---|---|
| `banco/macro.py` | colector de las cinco series (Yahoo, cacheado), betas móviles y alineación con el desfase declarado. `--cobertura` reporta sin medir nada |
| `banco/correr_macro.py` | la corrida: calibración → (P) → (C) → lote → veredicto |
| `banco/ranking.py` | `lote_rankings` acepta `mde` por objetivo (ver la nota de método) |
