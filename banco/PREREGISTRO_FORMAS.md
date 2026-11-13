# PREREGISTRO — dos formas de ranking para dirección: banda y Δrank

> Escrito el **2026-11-13**, **antes de calcular ninguna de las dos formas**. Es el último
> pendiente de `radar/HANDOFF_FORWARD_TEST.md` §7 ("formas distintas: banda, Δrank,
> multi-feature"). Multi-feature ya se cerró dos veces (`combo.py`;
> `PREREGISTRO_LIBRO_RADAR`). Si algo de este archivo cambia después de ver un número, el
> experimento no vale.

---

## 1. Por qué, y por qué el prior es bajo

La dirección dio **0 de 4.140** en el banco y **0 de 62** con el libro. Todos esos intentos
comparten la **forma**: top-k por el nivel de una feature, en una dirección o la otra.
`PREREGISTRO_TRANSVERSAL` (corrida 3, líneas 706-707) dejó escrito que lo único que quedaba
vivo era "otra resolución, otra fuente u otra forma". Resolución (5m, `MICRO` 0/192) y fuente
(libro) ya se probaron. Quedan dos formas:

- **Banda:** los nombres del **medio** de la distribución de una feature. Un top-k toma un
  extremo; si la relación feature → retorno es de U invertida (lo extremo revierte, lo
  moderado sigue), el extremo la esconde y el medio la muestra.
- **Δrank:** el **cambio de posición** de un nombre en el ranking de una feature, contra la
  barra anterior. Un nivel alto puede ser estructural ("esta moneda siempre tiene mucho
  OI"); que un nombre **suba** en el ranking es información nueva por construcción.

**El prior es bajo y hay que decirlo antes:** son las **mismas fuentes** que dieron 0, en
una forma distinta. Lo más probable es 0 de 210. El valor de correrlo es cerrar la lista
de §7, no la expectativa de encontrar algo.

**La barrera es el costo.** A 24h, el control al azar de la corrida 3 dio **−0,171 ATR**:
eso es lo que cuesta rebalancear a 0,20 %. El mejor brazo de dirección de la corrida 3
quedó en **−0,033**, o sea +0,138 por encima del azar y aun así negativo neto. Para
sobrevivir, una forma nueva tiene que hacer lo que ningún nivel hizo: superar esa barrera.

---

## 2. Diseño — el tablero de la corrida 3, sin cambios; cambia solo la forma

**Tablero:** el de `PREREGISTRO_TRANSVERSAL` corrida 3, sin tocar nada. Los 46 de
`deriv46`, `--derivados`, ventana **2021-08-01 → 2026-07-31** (el tablero arranca el
2021-10-14), `paso = horizonte = 24h`, `k = 8`, objetivos **`largo` y `corto`**. Se elige
24h y no 4h porque el costo pesa lo mismo en ATR por rebalanceo y el movimiento crece con
el horizonte: a 4h la barrera relativa es peor.

**Features:** las **35 de la corrida 3** —12 de precio, 6 de flujo, 17 de derivados—. No
entra `carry_acum` (se agregó el 2026-08-28, después de la corrida 3), ni on-chain, ni el
libro. Así el único cambio respecto de la corrida 3 es la forma.

**Las dos formas**, con el percentil `pct` de la feature calculado **dentro de la barra**:

| forma | score | variantes |
|---|---|---|
| **banda** | `−|pct − 0,5|` → los 8 más cerca de la mediana | una (es simétrica) |
| **Δrank** | `pct(t) − pct(t − 24h)`, mismo par | **subió** (los 8 de mayor Δ) y **bajó** (los 8 de menor Δ) |

- La banda **no tiene ancho que elegir**: son los 8 más cerca de la mediana. Probar
  "40-60" contra "30-70" sería un barrido.
- Δrank usa la barra anterior **del tablero** (24h atrás), sin ventana nueva. Si el par
  no estaba en la barra anterior, el Δ es NaN y no compite en esa barra.
- **Sin versiones residualizadas.** En la corrida 3 el residuo contra `roc_24` duplicaba
  los brazos y no cambió el veredicto de dirección. Acá duplicaría el FDR.

**Brazos:** 35 × (1 banda + 2 Δrank) × 2 objetivos = **210**, más los 3 controles al azar
por objetivo. FDR sobre los 210, como siempre (los controles no cuentan).

---

## 3. Compuertas — las de siempre, sin tocar

Las siete de `PREREGISTRO_TRANSVERSAL` §3: semanas ≥ 8 · signo > 0 · bootstrap de semanas
p < 0,05 · **Benjamini-Hochberg q = 0,10 sobre los 210** · sin top-3 y sin el mejor par ·
≥ 60 % de semanas positivas · costo **0,20 % y 0,50 %** (un brazo que solo vive a 0,20 %
no cuenta). Más la de **artefacto de escala** y el normalizador `atr_base`.

**Primero la nula** (`--nula`) con el mismo tablero, y el MDE de ahí. Como referencia, no
compuerta: la corrida 3 tuvo ±0,062 ATR con 251 semanas.

---

## 4. Régimen y fuera de muestra — solo si algo sobrevive

1. **Por año calendario (2021 → 2026), sin cambiar de signo en ninguno.** La ventana tiene
   bull, bear, recuperación y bear: un sobreviviente que viva de un régimen no cuenta.
2. **Fuera de muestra: 2026-08-01 → 2026-10-31** (13 semanas), con el mismo tablero
   (velas y métricas de futuros de ese tramo). **Antes de mirarlo**, la potencia:

       MDE_R = 2,8 · sd(spread semanal del sobreviviente en la principal) / √13

   Si `MDE_R` supera el spread de la principal, el tramo no puede confirmar nada: **no se
   mira** y lo que sigue es un forward test. Si alcanza, se exige **spread > 0 y
   bootstrap p < 0,05** sobre esas 13 semanas, a 0,20 % y a 0,50 %.

**La reserva del libro (2026-08 → 10 de `bookDepth`) no se usa:** este test no tiene
features del libro. Sigue intacta.

---

## 5. Regla de parada

1. **Si ningún brazo pasa las siete compuertas** → las dos formas quedan cerradas, y con
   ellas la lista de §7 de `HANDOFF_FORWARD_TEST.md`. **La dirección queda cerrada para
   estas fuentes en las tres dimensiones que se probaron: nivel, resolución y forma.** No
   se prueba otro ancho de banda, otro paso de Δ, otro k ni otro horizonte.
2. **Si sobrevive algo** → es el primer resultado direccional del repo y **no se cree**:
   §4.1, después §4.2. Aun pasando las dos, lo que sigue es un forward test con fecha y
   sin capital.
3. **Si sobrevive solo banda o solo Δrank de una feature y no sus vecinas** (por ejemplo,
   `roc_72` sí y `roc_24`/`roc_168` no) → se declara como sospechoso de barrido, igual que
   la corrida 3 trataba un horizonte aislado.

---

## 6. FUGA DECLARADA — lo que ya sé antes de correr

1. **Los niveles de estas 35 features**, en las dos direcciones: 0 de 276 en la corrida 3,
   todos negativos netos, el mejor −0,033. Los de posicionamiento (`tt_cuentas`,
   `ls_cuentas`, `tt_pos` [bajo]) quedaron arriba del resto en `largo`, todavía negativos.
2. **El tramo fuera de muestra no está virgen para dirección en general:** el chequeo del
   fade (2026-08-17 → 10-20) y el swing a 7d (hasta 10-23) usaron velas de ese período
   para **otras reglas**. Para banda y Δrank sobre estas features no se miró nada.
3. **No calculé ninguna banda ni ningún Δrank**, ni sé cuánto se solapan con el top-k de
   nivel.

---

## 7. Lo que hay que construir

- `ranking.py --formas`: reemplaza `scores()` por las dos formas sobre las 35 features de
  la corrida 3 (lista fija en el código, no "lo que haya en el tablero"), objetivos
  `largo` y `corto`, y reusa `lote_rankings` con sus compuertas.
- Un modo para el fuera de muestra que evalúe **solo las barras desde 2026-08-01** con el
  tablero construido hasta 2026-11-01, y que **solo corra con un sobreviviente** y con la
  potencia de §4.2 a favor.
- Se escribe en un PR aparte, se mergea, y **recién después** se corre.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

*(vacío: n = 0 el 2026-11-13)*
