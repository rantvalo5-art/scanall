# PREREGISTRO — Lead-lag entre alts (item 4.1)

> Escrito el **2026-08-26**, ANTES de mirar un solo resultado real. Lo unico que se
> corrio antes de escribir esto fue (a) el conteo de n post-join, (b) el test de
> lookahead y (c) la calibracion de la nula por desplazamiento circular — las tres
> cosas que el metodo del repo exige hacer ANTES de estimar el efecto, y ninguna de
> las tres revela el resultado.

---

## 1. La hipotesis

**H:** existe estructura de lead-lag ENTRE alts: el retorno reciente de un grupo de
monedas (definido por una caracteristica observable) predice el resultado de una
moneda DISTINTA, mas alla de lo que predice la propia historia de esa moneda.

**Two-sided, y por dos razones.** No hay prior direccional creible: la literatura
dice que las liquidas lideran a las iliquidas, pero en cripto el retail entra por las
chicas y el orden podria invertirse. Y el repo ya se comio una vez el costo de
escribir one-sided contra un prior contaminado (`PREREGISTRO_UNLOCKS_2.md`). El
diseno corre **las mismas hipotesis en las dos direcciones** (largo y corto, negando
`res`), que es como el banco implementa two-sided.

## 2. Por que no comparte causa de muerte con lo ya cerrado

- **No es precio por simbolo.** Las 450+ hipotesis de la familia precio son series de
  tiempo de UNA moneda (roc, dd, atr, obv). Aca la feature es el retorno de OTRAS.
- **No es transversal contemporaneo.** El rank de volumen de hoy ya se probo. Lo que
  nunca se miro es el DESFASE.
- **No es beta a BTC.** `beta_btc` e `idio_168` murieron en `lote_ancho.py`, pero eso
  es el mercado entero liderando. Aca los grupos son subconjuntos de alts y la senal
  es el SPREAD entre grupos, que es ortogonal al movimiento comun.
- **No es microestructura intra-vela.** Aquello era la forma del camino DENTRO de la
  hora de la propia moneda (0/192). Esto es entre monedas, a resolucion horaria.

## 3. Como se esquiva la matriz de 200x200

Probar cada par (i,j) x cada lag son ~200.000 hipotesis y ninguna seria operable.
Aca el lead-lag se mide **por grupo**: se ordena el universo por una caracteristica,
se parte en tercios y se pregunta si el retorno reciente de un tercio predice el
resultado de una moneda distinta (leave-one-out). Formulacion Lo-MacKinlay.

- **Caracteristicas (4):** `qv` (volumen en USD), `vol` (desvio de retornos),
  `amihud` (|ret|/volumen, iliquidez), `tk` (fraccion taker compradora).
  Todas sobre ventana movil de **168h que termina en t inclusive**.
- **Lags (4):** 1, 6, 12, 24 horas.
- **Grupos (3):** tercio alto, tercio bajo, y **spread hi-lo** (la senal de lead-lag
  propiamente dicha).
- **4 x 4 x 3 = 48 features.** Cada una entra con cola alta y cola baja, cruda y
  condicionada al quintil de volatilidad → **192 mascaras**, x2 direcciones =
  **384 hipotesis**.

**Leave-one-out obligatorio:** la media del grupo excluye a la propia moneda. Sin eso
la feature contendria el retorno reciente de la moneda misma, que ya esta medido y
haria que la hipotesis se confirme sola.

## 4. Datos y n POST-JOIN (contado antes de la regla)

- Universo **pineado** `base200` (reproducible), panel 1h ANCHO ya cacheado.
- Ventana **2025-08-01 → 2026-08-01**.
- **187 simbolos** con >= 2000 velas.
- **117.350 entradas**, de las cuales **111.330 resueltas** (94,9%).
- **49 semanas.** Entradas por semana: mediana 2.379.
- Win rate base **48,65%**. Win rate de break-even con costo 0,20%: **51,25%**.
  O sea que una hipotesis tiene que aportar **+2,60 pp** solo para no perder plata.

**El n efectivo NO es 111.330.** Las entradas se solapan (una cada 12h con horizonte
de 30 dias) y el regimen esta autocorrelacionado. El n efectivo son las **49 semanas**.
Esa es exactamente la leccion que costo `micro.py`: p_indep 1,1e-36 contra p_bloques
0,3845. **El p que decide es el de bloques.**

## 5. Validaciones hechas antes de correr

- **Sin lookahead, verificado por corrupcion:** se corrompio TODA la data posterior a
  un corte (x5 a x50 aleatorio en o/h/l/c/v/qv/n/vb) y se recomputaron las features.
  **48/48 identicas** en las 62.290 filas anteriores al corte. Es el mismo test que
  paso el detector forming (243/243).
- La entrada es `c[i]` con resultado desde `i+1`, asi que usar hasta `i` inclusive es
  correcto y no optimista.

## 6. LA REGLA DE PARADA — escrita antes de mirar

Las **seis compuertas de `lote.py` estan cableadas** y no se tocan: umbral, FDR
(q=0,10), pareado (no seleccion-de-moneda), sin-top3, sin-top1, y consistencia
semanal (>=60%). El p que decide es `p_bloques`.

Encima de eso, la compuerta de **look-elsewhere**:

> **SOBREVIVE el lote solo si el numero de hipotesis que cruzan las seis compuertas
> SUPERA ESTRICTAMENTE el maximo observado en la nula** por desplazamiento circular
> (5 repeticiones). Si empata o queda por debajo, el resultado es indistinguible de
> ruido y la familia se cierra.

- **Nula por DESPLAZAMIENTO CIRCULAR, no permutando filas.** Barajar destruye la
  autocorrelacion de features y resultados y hace la nula demasiado facil.
- Si sobrevive algo, antes de reportarlo: **`sin_top3` y concentracion por simbolo**
  (ya son compuertas), y ademas hay que mirar si vive en pocas semanas gordas — el
  modo de muerte de OI shock y de funding.
- **Si sobrevive algo, la promocion es a la ventana OOS virgen declarada en
  `PREREGISTRO_ANCHO.md`: 2024-08-01 → 2025-08-01**, que nunca se miro. No se declara
  ganador sin eso.

**No hay alternativas declaradas.** Si falla, falla, y el item 4.1 queda cerrado.

## 7. Resultados

### 7.1 Calibracion de la nula (look-elsewhere) — CORRIDA ANTES DE LA REAL

`py -3.13 -u leadlag.py --nula 5` (log: `leadlag_nula.log`).

| rep | largo | corto | total sobrevivientes (de 384) |
|---|---|---|---|
| 1 | 0 | 0 | **0** |
| 2 | 0 | 0 | **0** |
| 3 | 0 | 0 | **0** |
| 4 | 0 | 0 | **0** |
| 5 | 0 | 0 | **0** |

**media 0,00 — maximo 0.**

**=> LA BARRA QUEDA FIJADA: el lote real tiene que dar >= 1 sobreviviente.**

Lectura honesta de esta nula, anotada ahora y no despues: que 5 de 5 repeticiones
den exactamente 0 dice que las seis compuertas juntas son MUY estrictas — no que
sean magicas. Con 5 reps y 0 observados no se distingue una barra de 0 de una de
~0,6 (cota superior al 95%). Por eso **un solo sobreviviente seria evidencia
flaca**, y por eso la regla de la seccion 6 ya exige promoverlo a la ventana OOS
virgen antes de declarar nada. Dos o mas sobrevivientes si separan del ruido.

### 7.2 La corrida real — 0 de 384

`py -3.13 -u leadlag.py` (log: `leadlag.log`, detalle: `leadlag.csv`).

**0 sobrevivientes de 384. La barra era >= 1. => H NO SOBREVIVE. Item 4.1 CERRADO.**

**Subpotenciadas: 0.** Ninguna hipotesis se salteo por `n < 200`, o sea que las 384
fueron JUZGADAS, no omitidas. Esto no es "no se pudo medir".

#### Donde mueren

| donde | cuantas |
|---|---|
| no cruza el umbral | 235 |
| muere en la correccion (FDR q=0,10) | 149 |

De las **149 que cruzan** el umbral: 146 pasan el pareado, 141 pasan `sin_top3`.
O sea que **no mueren por concentracion ni por seleccion-de-moneda**. Mueren en la
multiplicidad, y mueren ahi porque el p de bloques es grande:

> **110 hipotesis con p_indep < 0,001. Solo 12 con p_bloques < 0,05.**
> La mejor: **p_indep 2,3e-44 y p_bloques 0,1735.**

Es **la misma firma que `micro.py`** (1,1e-36 vs 0,3845). Dos familias distintas,
misma enfermedad: el n efectivo son 49 semanas, no 111.330 entradas.

#### El diagnostico que importa: esto NO es lead-lag

Las 149 que cruzan el umbral **son TODAS del lado corto** (149 cortas, 0 largas) y
estan repartidas **uniformemente** entre las cuatro caracteristicas (tk 41, qv 38,
vol 36, amihud 34) y entre los cuatro lags (34 / 38 / 40 / 37).

Esa uniformidad es la prueba de que no hay estructura de lead-lag. **Si el desfase
fuera real se concentraria en celdas (caracteristica, lag) especificas** — por
ejemplo liquidas→iliquidas a 6h. Que aparezca por igual en TODAS las combinaciones
dice que lo que cruza no depende de la feature.

Lo que si depende es la direccion, y hay una explicacion aritmetica:

| | valor |
|---|---|
| win rate LARGO base | 48,65% |
| win rate CORTO base | 51,35% |
| break-even (costo 0,20%) | 51,25% |
| **sin deriva** (barreras +-8% son asimetricas en log) | **52,00%** |

El largo base esta **3,35 pp por debajo** de lo que daria un activo sin deriva: la
ventana 2025-08 → 2026-08 fue bajista. Eso deja al corto arrancando en **+0,10 pp**
sobre el break-even y al largo en **-2,60 pp**. Cualquier mascara con un sesgo
minimo cruza del lado corto y ninguna cruza del largo. **Es deriva del periodo, no
desfase entre monedas.**

Y la varianza semanal explica por que el p de bloques las mata: el win rate largo
por semana va de **15,4% a 83,8%** (mediana 49,4%). Con 49 semanas asi de
heterogeneas, +4,7 pp de media no se distingue de cero.

#### Que queda descartado y que no

**Descartado:** que el retorno reciente de un grupo de monedas —definido por
liquidez, volatilidad, iliquidez de Amihud o flujo taker, a 1/6/12/24h— prediga el
primer toque de +-8% de una moneda distinta, en el top-200, a resolucion horaria.

**NO descartado** (y conviene anotarlo para no sobre-leer): lead-lag a resolucion
mas fina que 1h; lead-lag por pares especificos (la matriz 200x200, que se esquivo
a proposito); y lead-lag fuera del top-200. La primera y la tercera caen en las
familias que el handoff ya lista aparte.
