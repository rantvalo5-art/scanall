# PREREGISTRO — ranking TRANSVERSAL por barra (top-k), no máscara absoluta

> Escrito el **2026-08-27, ANTES de correr una sola línea de `ranking.py`.** La sección 7
> declara una fuga real: la elección de `atr_24` como brazo primario NO es ciega. Todo lo
> demás sí lo es. Si algo de este archivo cambia después de ver un número, el experimento
> no vale.

---

## 1. La pregunta, y por qué NO la contesta nada de lo ya cerrado

Todo el banco prueba **filtros**: una máscara booleana sobre cuantiles *pooled* del
panel-año, evaluada por win rate contra el umbral. `PREREGISTRO_RANKING.md` cerró
"¿el score ordena?" — pero midió si ordena **las alertas ya emitidas**, que comparten un
daño estructural común (+3,12 ATR de corrida previa, sección 9 de ese archivo).
Reordenar una población uniformemente dañada no puede funcionar: es mecánico, no
estadístico.

Esta pregunta es distinta en **tres** ejes, y ninguno se probó:

1. **Transversal, no absoluto.** Un corte por cuantil pooled (`roc_168 >= q80`) en una
   semana alcista selecciona *el universo entero*: la máscara es medio selector de
   tiempo. Un rank **dentro de cada barra** separa la sección cruzada del régimen por
   construcción.
2. **k constante.** El filtro dispara en ráfagas, así que la actividad correlaciona con
   el resultado — y ahí vive la trampa de `SEM_N_MIN`, que dio vuelta el veredicto del
   único sobreviviente de 735 brazos (descartaba el 66% de las semanas y el 41% de los
   trades, y lo que descartaba era la parte que perdía). Con top-k por barra **cada
   barra aporta exactamente k posiciones**, así que esa trampa **no puede ocurrir**. No
   es una compuerta aflojada: es un modo de falla eliminado por diseño.
3. **Control contra la MISMA barra.** El `wr_pareado` de `lote.py` aparea por *símbolo*.
   Nunca se apareó por *barra*, así que el término de mercado —−0,316 ATR, **34% de la
   pérdida** medida en la sección 9— nunca se neutralizó en un test de ranking. Acá se va
   por construcción: el estadístico es top-k **menos el universo de esa misma hora**.

## 2. Diseño

**Unidad de observación**: una barra. **Unidad independiente: la SEMANA.**

- Panel `pin="base200"`, 1h, 2025-08-01 → 2026-08-01 (la ventana estándar del banco).
- Rebalanceo cada `paso` horas, horizonte `H` horas, con **`paso >= H`** para que las
  barras **no se solapen**. Default `paso = H = 24`. Esto elimina de raíz los ~60 trades
  vivos simultáneos que inflan el n aparente en todo el resto del repo.
- En cada barra `t`: rankear el universo por `score(t)`, tomar los `k` primeros.
- Barras con menos de `MIN_SYMS = 30` símbolos válidos se descartan (no hay sección
  cruzada).

**Métrica primaria** — todo en unidades de `atr_24` del propio símbolo, porque la
sección 8B ya mostró que **~87% de cualquier efecto crudo es escala**, no ventaja:

```
spread(t) = media( y | top-k de t )  −  media( y | universo de t )
semana(w) = media de spread(t) sobre las barras de w
estadístico = media de semana(w), cada semana pesando UNO
```

**Tres objetivos `y`, declarados de antemano:**

| objetivo | `y` | qué probaría |
|---|---|---|
| `largo` | `ret_H / atr_24` | el ranking ordena hacia arriba |
| `corto` | `−ret_H / atr_24` | el ranking ordena hacia abajo (la asimetría 0,72) |
| `magnitud` | `(runup − caída) / atr_24` | el ranking ordena el ANCHO del camino |

`largo` y `corto` son el mismo test con el signo dado vuelta, y se reportan los dos
**siempre**: `HANDOFF_CIERRE` §4.2 documenta que medir una sola dirección ya produjo un
falso negativo en este repo.

**Brazos**: ~11 scores simples (las features de `lote.py`), sus versiones
**residualizadas contra `roc_24`** (que es la opción de sacar el sesgo de momentum), y
**3 controles al azar** con el mismo `k` y las mismas barras. Todos al mismo FDR.

**El control nulo es la pieza central.** Si ningún score se separa de tres rankings
aleatorios, la pregunta está contestada sin que nada tenga que "sobrevivir".

## 3. Compuertas — cableadas, veredicto por default CERRADA

1. **Semanas** — mínimo 8, o el bootstrap de bloques no corre (`_p_bloques` de `lote.py`
   devuelve 1.0 por debajo de eso; acá es un error explícito, no un p=1 silencioso).
2. **Signo** — el spread medio semanal tiene que ser > 0. Aportar "menos negativo" no
   cuenta: la sección 8 ya encontró 83 brazos que perdían menos y ninguno ganaba.
3. **Bootstrap de semanas** — cada semana pesa uno, 2.000 repeticiones, p < 0,05.
4. **Multiplicidad** — Benjamini-Hochberg q=0,10 sobre TODOS los brazos y los TRES
   objetivos a la vez. No una familia por vez.
5. **Concentración** — el spread sigue > 0 sacando los 3 símbolos que más aportan, **y**
   sacando el mejor solo.
6. **Consistencia** — ≥60% de las semanas con spread > 0.
7. **Costo** — se corre a 0,20% **y** a 0,50%. Un brazo que solo vive a 0,20% no cuenta
   (`libro.py` midió 1,5× a 6,3× ese número). El costo se aplica por rebalanceo sobre la
   pata top-k, convertido a unidades de ATR con el `atr_24` mediano de los seleccionados.

**No es criterio**: que la media sea positiva (la secuestra un par — pasó dos veces), que
el p binomial sea bajo (está inflado por diseño), o que un brazo "se vea prometedor".

## 4. Lo que este test NO puede decir

- **Nada sobre ejecución.** Un top-k transversal en 200 pares implica rebalancear 20
  posiciones por día; el costo entra como término, pero el impacto de mercado de entrar
  y salir de la cola **no está modelado**.
- **Nada sobre el lado corto en spot.** Si `corto` gana, el instrumento son perpetuos, y
  eso arrastra funding, que este test **no incluye**. Un `corto` ganador acá es una
  invitación a re-medir con `metricas.py`/`funding.py`, no un resultado operable.
- **Nada sobre el universo.** `pin="base200"` es el ranking de volumen de hoy: los
  deslistados no están, y eso sesga hacia mejor.
- **Un solo régimen.** La ventana es un bear brutal (BTC −45%, alts −70%). Ver la
  sección 6.

## 5. n y MDE — se cuentan ANTES de estimar

Regla del handoff, aplicada tal cual: **primero se corre `ranking.py --nula`**, que
computa **únicamente los controles aleatorios**, y de su dispersión semanal sale el MDE
al 80% de potencia. Recién con el MDE en la mano se mira un brazo real.

Si el MDE resulta más grande que el efecto que se busca, el veredicto es
**"no se pudo medir"**, que no es lo mismo que "no está".

## 6. Régimen — la compuerta que decide si esto vale algo

La asimetría hacia abajo (`simetria` 0,72 en `movers_estudio.csv`) **puede ser
íntegramente el bear**. Por lo tanto:

> **Un brazo `corto` que sobreviva las siete compuertas NO se cree** hasta correrlo sobre
> la reserva **2024-08-01 → 2025-08-01**, declarada en `PREREGISTRO_ANCHO.md`, **nunca
> mirada**, y que contiene tramo alcista. Si ahí cambia de signo, era el régimen.

Esa reserva no se toca en esta corrida. Si no sobrevive nada, no hace falta gastarla.

## 7. FUGA DECLARADA — lo que ya vi

**`atr_24` no es un brazo ciego.** Se eligió como primario después de ver
`banco/movers_estudio.csv`: lift 1,68 sobre la tasa base, 39/44 ventanas, y `simetria`
0,72 (p_up 17,3% contra p_dn 24,0%). Consecuencias, asumidas:

1. Los brazos de volatilidad (`atr_24`, `vol_24`, `vol_168`, `rango_168`) son
   **confirmatorios, no exploratorios**. Si sobreviven, van derecho a la reserva OOS de
   la sección 6 antes de creerles nada.
2. **El estadístico es distinto del que vi.** `movers.py` mide P(decil superior de
   recorrido | feature) — una tasa de acierto sobre una etiqueta cross-seccional. Acá se
   mide el **spread medio semanal de un top-k**, que es lo que se cobraría. Que lo
   primero dé 1,68 no implica nada sobre lo segundo: `HANDOFF_CIERRE` documenta cinco
   casos donde una feature concentraba movidas y no movía la expectativa
   ("forma, no expectativa").
3. Los brazos de momentum, los residualizados y **los tres controles al azar siguen
   ciegos**, y la regla de parada les aplica entera.
4. La reserva OOS de la sección 6 **no se miró** y no se mira en esta corrida.

## 8. Regla de parada — fijada ANTES de correr

1. **Si ningún brazo se separa de los tres controles al azar por más del MDE de la
   sección 5** → *el ranking transversal no ordena mejor que el azar*. La familia se
   cierra en una corrida y no se sigue buscando acá.
2. **Si no sobrevive ningún brazo las siete compuertas** (lo más probable) → cerrado.
   **Prohibido aflojar una compuerta y volver a mirar.**
3. **Si sobrevive `magnitud` pero no `largo` ni `corto`** → queda confirmado lo que el
   repo ya sospecha: se rankea movimiento, no dirección. Eso NO es operable sin
   convexidad (`[[project-dos-puntas-descartado]]`) y **no se opera**: se documenta y se
   pasa a la pregunta del instrumento.
4. **Si sobrevive `corto`** → sección 6, reserva OOS, sin excepción. Y aun sobreviviendo
   ahí, sigue faltando el funding.
5. **Si sobrevive un residualizado y no su versión cruda** → el sesgo de momentum era el
   que tapaba la señal. Es el resultado más interesante posible de esta corrida, y aun
   así va a la reserva OOS antes de tocar nada.

---

# RESULTADOS — corrida del 2026-08-27

Panel `base200`, 2025-08-01 → 2026-08-01, 1h. **48.575 filas · 335 barras · 145 pares ·
49 semanas**, paso 24h / horizonte 24h, **sin solape**. 23 rankings + 3 controles al
azar x 3 objetivos = **69 brazos**, todos al mismo FDR.

## La nula, corrida ANTES de mirar nada (sección 5)

| objetivo | sd semanal | spread del azar | **MDE (80% potencia)** |
|---|---|---|---|
| largo | 0,2560 | −0,1687 | **±0,1024 ATR** |
| corto | 0,2598 | −0,1680 | **±0,1039 ATR** |
| magnitud | 0,7006 | −0,0522 | **±0,2802 ATR** |

El spread del azar es exactamente el término de costo (0,20% / ATR mediano ≈ 0,167), o
sea que **el spread BRUTO de un ranking aleatorio es cero**, como tiene que ser. El
harness pasa su propio control. Y el MDE de ±0,10 ATR deja detectable de sobra el término
"momento" de −0,565 ATR de la sección 9 de `PREREGISTRO_RANKING.md`: **no estamos
subpotenciados**.

## Veredicto: 0 de 69

| | |
|---|---|
| spread ≤ 0 | 54 |
| **artefacto de escala** | **8** |
| muere en la corrección (FDR q=0,10) | 7 |
| **SOBREVIVEN** | **0** |

## La compuerta que hubo que agregar — y por qué apretar no es aflojar

La primera corrida dio **2 sobrevivientes** en magnitud (`dd_168` +0,7512, p=0,0005;
`dd_168 ~ sin roc_24` +0,7117, p=0,0015). Los dos eran falsos.

El objetivo `magnitud` se normaliza por `atr_24` — **y `atr_24` es uno de los scores que
se rankean**. Rankear por una variable contra un objetivo dividido por esa misma variable
es circular. Medido:

| ranking | ATR sel / ATR universo | spread NORMALIZADO | spread CRUDO |
|---|---|---|---|
| `dd_168` | **0,61×** | **+0,751** | **−0,028** |
| `atr_24` | **1,94×** | **−0,985** | **+0,058** |

`dd_168` elige nombres que se mueven 39% menos que el universo, así que dividir por su
propio ATR infla el cociente: el "hallazgo" era el denominador. Y `atr_24` es el error
espejo — un **falso negativo**.

Se cableó en `_spread_semanal` el cómputo del spread **sin normalizar** y la compuerta
**ARTEFACTO DE ESCALA**: si el signo depende de la normalización, el brazo no sobrevive.
Mató 8 brazos, incluidos los 2 sobrevivientes **y los 4 mejores de `largo`**.

> Esto se agregó DESPUÉS de ver los dos sobrevivientes, y se declara. Es admisible por la
> misma razón que el barrido de `n_min` de `PREREGISTRO_RANKING.md`: **es apretar una
> compuerta, no aflojarla**, y el resultado es más estricto. Aflojar después de mirar
> fabrica falsos positivos; apretar después de mirar solo puede matar los propios.

## Costo 0,50% (compuerta 7) — mismo veredicto

Re-corrido entero con el costo medido por `libro.py` en vez del supuesto del banco:
**0 de 69**. El spread del azar pasa de −0,169 a −0,421 ATR y el MDE queda en ±0,107,
o sea que la resolucion no cambia; simplemente todo el lote se corre hacia abajo. Los
mismos 8 brazos caen por escala.

## Objetivo LARGO — 0, y los 4 mejores eran escala

| ranking | spread | **crudo** | atrR | sem>0 | p | |
|---|---|---|---|---|---|---|
| `roc_168` | +0,0835 | **−0,00055** | 1,43 | 59% | 0,1095 | artefacto |
| `roc_168 ~ sin roc_24` | +0,0438 | **−0,00099** | 1,37 | 49% | 0,2510 | artefacto |
| `vol_24 ~ sin roc_24` | +0,0217 | −0,00232 | 1,81 | 51% | 0,3960 | artefacto |
| `vol_24` | +0,0105 | −0,00250 | 1,91 | 51% | 0,4500 | artefacto |

Ninguno tiene retorno crudo positivo. **El ranking transversal no ordena hacia arriba.**

## Objetivo CORTO — 0, y ni siquiera insinúa

Todos los brazos quedan **por debajo o al nivel del control al azar** (−0,1486). El
mejor, `rango_168 ~ sin roc_24` (−0,1262), no le gana a `CONTROL azar 1` (−0,1074).

**Esto cierra la lectura optimista de la asimetría 0,72** de `movers_estudio.csv`. Que
las movidas grandes sean 1,39× más probables abajo **no se traduce en un spread
transversal cobrable**: los nombres volátiles caen más *y* rebotan más, y el neto en un
top-k rebalanceado cada 24h es cero menos costo. Medir la asimetría de las colas no es lo
mismo que poder cobrarla. **La reserva OOS de la sección 6 no se toca**: no hay
sobreviviente que validar.

## Lo único que SÍ ordena, y hay que decir exactamente qué es

Sobre los 23 rankings, correlacionando cuán volátil es la selección de cada uno
(`atr_ratio`) contra lo que consigue **en crudo**:

| | Spearman | p | n |
|---|---|---|---|
| **magnitud cruda** vs `atr_ratio` | **+0,963** | 1,7e−13 | 23 |
| **retorno crudo** vs `atr_ratio` | **−0,746** | 4,4e−05 | 23 |

Casi perfecto, en las dos direcciones. Traducido:

> **No importa por qué rankees. Lo único que determina lo que conseguís es cuán volátil
> te queda la selección. Y la volatilidad te compra ancho de camino (+5,8 pp de recorrido
> a 24h con `atr_24`, contra −2,8 pp con `dd_168`) y te cuesta retorno.**

Es la formulación más nítida que produjo el repo de "detecta movimiento, no dirección", y
sale de un diseño que **no tiene** la trampa de `SEM_N_MIN`, ni el término de mercado
adentro, ni entradas solapadas, ni el corte pooled que confunde tiempo con sección
cruzada. Los cuatro defectos que se le podían achacar al veredicto anterior están
eliminados **por construcción**, y el veredicto no cambió.

Y confirma `movers.py` con un estadístico distinto: allá era P(decil superior de recorrido
| feature) = lift 1,68; acá es el spread de recorrido de un top-k. Los dos dicen que
rankear por volatilidad ordena movimiento.

**Pero también dice lo que eso vale**: `atr_24` tiene spread **−0,98 en unidades de su
propio ATR**. O sea que la selección se mueve 1,94× más que el universo **y aun así menos
de lo que su propio ATR ya anunciaba**. El ranking no agrega información sobre el nivel de
ATR: la volatilidad es persistente y eso ya está en el precio, gratis.

## Lo que queda cerrado y lo que no

- **CERRADO**: el ranking transversal por barra, en las tres direcciones, con el universo
  y la ventana del banco. No hay que volver a proponer "rankear por X" con estas features.
- **NO cerrado**: features que no salen del precio (la familia 4.3 del handoff). Este test
  agotó las 12 de `lote.py` y sus residualizadas, que son todas de precio.
- **NO se tocó**: la reserva OOS 2024-08 → 2025-08. Sigue virgen.
- **Consecuencia accionable**: cualquier producto que salga de acá tiene que cobrar
  **ancho de camino**, no dirección — y el repo ya cerró la vía de sintetizar convexidad
  con órdenes stop. Sin instrumento convexo, esto no es operable.

---

# CORRIDA 2 — el espacio que la corrida 1 no cubrió (2026-08-27)

La corrida 1 se presentó como si cerrara "el ranking transversal". **No lo hacía.**
Cubría una celda: OHLC × 1h × top-k × k=20 × 24h × spot. Tres agujeros concretos,
todos corregidos acá:

## Agujero 1 — solo se rankeó DESCENDENTE

`_spread_semanal` toma siempre los k más **altos**. "Los k más bajos de X" es una
selección **distinta**, no su espejo. La corrida 1 probó 23 rankings creyendo que cubría
23 ideas: cubría **23 de 46**. Ahora `scores(ambas=True)` genera las dos direcciones.

## Agujero 2 — se usó OHLC teniendo el panel ANCHO cacheado

412 archivos `_v2` en `.kline_cache/` con `o, v, qv, n, vb`. **`vb` es el volumen taker
comprador: la única variable del kline que no es precio**, y el docstring de `klines.py`
ya decía que el banco nunca la pudo probar. Se agregaron 6 features de flujo
(`desbal`, `desbal_24`, `desbal_168`, `ticket_rel`, `turnover`, `n_surge`).

**73 rankings × 3 objetivos = 219 brazos**, contra 69 de la corrida 1.

## Agujero 3 — el normalizador, corregido por segunda vez

La corrida 1 dividía por `atr_24` y agregó el chequeo del signo crudo. Insuficiente: el
`atr_ratio` comparaba contra el **universo**, y eso no ve cuando un ranking elige un
momento quieto **del propio símbolo**. Medido:

| ranking | ATR/universo | **ATR/PROPIO** | magnitud cruda |
|---|---|---|---|
| CONTROL azar | 1,01 | 1,00 | +0,0019 |
| `compresion [bajo]` | 0,96 *(parece limpio)* | **0,93** | **+0,0011** ← peor que el azar |
| `atr_24` | 1,94 | 1,67 | +0,0588 |

`compresion` está **definida** como `vol_24/vol_168`, así que rankearla por lo bajo
deprime el denominador por construcción. Había dado 4 "sobrevivientes" y al menos uno
era falso.

**Corrección v3**: se normaliza por `atr_base` — la **mediana móvil de 30 días** del ATR
del propio símbolo. Es pasado (sin lookahead) y el ranking **no la puede mover eligiendo
un momento**: para moverla tendría que elegir otro símbolo, que es justo la decisión que
se quiere medir. Se conservan las tres vistas (base, cruda, y los dos `atr_ratio`).

---

# RESULTADOS DE LA CORRIDA 2 — el resultado se parte en dos, limpio

| objetivo | brazos | spread ≤ 0 | artefacto | **SOBREVIVEN** |
|---|---|---|---|---|
| **largo** | 70 | **70** | 0 | **0** |
| **corto** | 70 | **70** | 0 | **0** |
| **magnitud** | 70 | 12 | 22 | **28** |

## Dirección: 0 de 140, y ni uno solo con spread positivo

Las dos direcciones, 35 rankings cada una, incluidas las de flujo y las residualizadas.
**Ningún brazo tiene spread positivo.** El mejor de los 140 es `roc_168` en largo con
−0,024. No es "murió en una compuerta": **no hay ni un candidato**.

Esto ahora sí es fuerte, porque cubre lo que la corrida 1 no cubría: las dos direcciones,
el order flow del kline, y con un normalizador que el ranking no puede elegir.

## Magnitud: 28 sobrevivientes, y `atr_24` era un falso negativo

Con el normalizador corregido, `atr_24` pasa de **−0,98** (corrida 1, circular) a
**+2,33** con 96% de semanas y p=0,0000. El falso negativo que sospechábamos era real.

| ranking | spread | crudo | sem>0 | p |
|---|---|---|---|---|
| `roc_168` | +2,812 | +0,0331 | 98% | 0,0000 |
| **`turnover`** | +2,650 | +0,0274 | **100%** | 0,0000 |
| **`n_surge`** | +2,566 | +0,0284 | 98% | 0,0000 |
| `roc_72` | +2,551 | +0,0328 | 98% | 0,0000 |
| `atr_24` | +2,334 | +0,0583 | 96% | 0,0000 |

**`turnover` y `n_surge` son features de FLUJO** — de las que se agregaron hoy y el banco
nunca había probado. `turnover` acierta en **49 de 49 semanas**.

## Lectura

> **Predecir CUÁNTO se va a mover una moneda respecto de su propia línea base funciona,
> y funciona robusto: 28 rankings, 94-100% de las semanas, sobreviven concentración y
> FDR. Predecir HACIA DÓNDE no funciona: 0 de 140, sin un solo candidato.**

Es la misma frase de siempre, pero por primera vez con las dos mitades **medidas en el
mismo diseño**, con las dos direcciones corridas y sin normalizador manipulable.

## Lo que NO se puede concluir todavía

1. **Los 28 son in-sample.** Ventana única, régimen único (bear). La §6 sigue vigente:
   reserva OOS 2024-08 → 2025-08, **sin tocar**.
2. **La magnitud sigue sin instrumento.** §8.3 del preregistro: sin convexidad no se
   cobra, y el repo ya cerró sintetizarla con órdenes stop.
3. **Sigue siendo UNA celda del espacio.** Falta: 5m (200 archivos cacheados), 4h/1d,
   derivados (`.metrics_cache/`, 5 años, 6 columnas y ninguna es precio), barridos de `k`
   y de horizonte, bandas, Δrank y combinaciones multi-feature.

---

# CORRIDA 3 — DERIVADOS (preregistro, escrito 2026-08-27 ANTES de correr)

> Nada de esta sección se escribió después de ver un número de derivados. Los resultados
> van debajo de la línea, como en las corridas 1 y 2.

## 1. Por qué esto no es más de lo mismo

Las corridas 1 y 2 midieron **17 features de precio + 6 de flujo del kline**. Las dos
mitades del resultado (dirección 0/140, magnitud 28/70) están condicionadas a **una sola
fuente de información: la vela de spot**.

`banco/.metrics_cache/` tiene otra fuente, ya en disco, que el banco nunca puso en un
ranking transversal: el dataset `metrics` de Binance Futures. **Seis columnas y ninguna
es precio:**

| columna | qué es |
|---|---|
| `oi`, `oi_usd` | open interest (contratos / USD) |
| `tt_cuentas`, `tt_pos` | ratio long/short de los traders top (cuentas y posiciones) |
| `ls_cuentas` | ratio long/short de todas las cuentas |
| `taker` | ratio de volumen agresor comprador/vendedor |

Los cuatro últimos son **posicionamiento**: quién está parado de qué lado. Es el insumo
clásico de una señal contraria direccional, y la dirección es exactamente la mitad que
falla. Que el precio no ordene no dice nada sobre si el posicionamiento ordena.

`metricas.py` ya resuelve la parte donde se cuela el lookahead: agrupa por hora tomando
el **último** valor del bin y lo etiqueta con `t[i]`, o sea lo que se sabe cuando la vela
cierra. Se usa `feat_metricas()` tal cual, sin tocarla.

## 2. La ventana cambia, y eso es a favor

El caché tiene 47 pares con **2021-08-01 → 2026-07-31 completo**: cinco años, horario.
Contra las 49 semanas de las corridas 1 y 2, acá hay **~260 semanas** — y la semana es la
unidad independiente, así que la potencia sube mucho. Además cubre el bull de 2021, el
bear de 2022, la recuperación 2023-24 y el bear actual: **la objeción de "esto es un solo
régimen" deja de aplicar**, que es justo lo que hacía falta.

**El precio a pagar, declarado:** el universo baja de 145 pares a ~46 (se saca
`USDCUSDT`, que es un par de stablecoin y en un ranking por volatilidad sería ruido
estructural). Es un universo **sesgado a sobrevivientes grandes** — son los que tienen
perp desde 2021 y siguen vivos. Sesga hacia mejor y hay que decirlo en cualquier
conclusión.

Con 46 pares, **`k` baja de 20 a 8** para mantener la selectividad (~17%, contra
20/145 ≈ 14% de las corridas anteriores). Se fija ahora, no después de ver resultados.

## 3. Brazos

17 features de `metricas._feat()` — `oi_chg_1h/4h/24h`, `oi_z`, `oi_rel_168`, y para cada
uno de `tt_cuentas`/`tt_pos`/`ls_cuentas`/`taker` su nivel, su cambio a 24h y su
percentil propio a 168h — **en las dos direcciones**, más sus residualizadas contra
`roc_24`, más los 3 controles al azar, x 3 objetivos.

> **Sobre comparabilidad**: el docstring de `metricas.py` avisa que los ratios en crudo
> **no son comparables entre monedas**. Por eso las versiones `_pct` (percentil contra su
> propia historia) son las que tienen sentido transversal; los niveles crudos se corren
> igual pero **un sobreviviente que sea solo de nivel crudo se trata como sospechoso de
> estar rankeando "qué moneda es", no "qué está pasando"**, y hay que mirarle el
> `sin_top3`.

## 4. Compuertas — las mismas, sin tocar

Las siete de la sección 3, más la de ARTEFACTO DE ESCALA de la corrida 1 y el
normalizador `atr_base` de la corrida 2. FDR sobre el lote entero. Costo 0,20% y 0,50%.
Nula corrida primero para el MDE.

## 5. Regla de parada — fijada ANTES

1. **Si ningún brazo de derivados supera el MDE contra los controles al azar** → el
   posicionamiento no ordena transversalmente y la fuente queda cerrada para esta forma
   de uso. **No se prueba una feature más de precio para compensar.**
2. **Si sobrevive algo en `largo` o `corto`** → es el primer resultado direccional del
   repo y **no se cree**: hay que partirlo por régimen (los cinco años dan al menos
   cuatro trozos con signo de mercado distinto) y exigir que **no cambie de signo**.
   Sobrevivir pooled sobre 5 años y morir en el bull de 2021 es el mismo autoengaño de
   siempre con más datos.
3. **Si sobrevive solo en `magnitud`** → confirma la corrida 2 desde otra fuente. Es
   informativo pero no cambia el problema del instrumento (§8.3).
4. **Si sobrevive un nivel crudo y no su `_pct`** → sospechoso de rankear identidad de
   moneda. Se exige `sin_top3` y `sin_top1` positivos, que ya están cableados.
5. **El universo de 46 sobrevivientes grandes se declara en cualquier conclusión.** No se
   puede extrapolar a la cola.
