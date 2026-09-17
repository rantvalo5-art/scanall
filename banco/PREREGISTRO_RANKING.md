# PREREGISTRO — ¿el score compuesto ordena, o alcanza con un indicador?

> Escrito el **2026-08-26**, con el motor (`banco/alertas.py`) cableado y validado en un
> piloto de 5 semanas del que se miraron **solo invariantes**: cobertura de features
> (19/19), tasa de resolucion (85,6%), alertas por semana (125), win rate de la LINEA
> BASE. **Ningun win rate por hipotesis, ningun retorno por brazo.** Las secciones 1-5
> son ciegas. La seccion 6 se cierra con los conteos de la seccion 5, que ya estan.

---

## 1. La pregunta, y por que no la contesta nada de lo ya cerrado

El score de produccion son 15 puntos: base + bonos escalonados de OBV/CVD/momentum/
distancia − penalizaciones de climax/entrada tardia/resistencia/repeticion, todo
disperso en seis secciones `scoring_*` de `config.json`. Son ~200 lineas de perillas.

Lo que el repo ya cerro es **otra** pregunta:

- [[project-swing-techo-condicional]] — con 36 features y modelos libres no hay
  informacion condicional sobre el retorno medio a 24h ni a 7d.
- [[project-swing-techo-oraculo]] — se detecta el 83% de lo que elegiria un oraculo,
  asi que el cuello nunca fue la deteccion.

Las dos preguntan **si existe senal**. Esta pregunta es distinta y mas barata: dado que
el bot ya emitio el stream de alertas, **¿el score las ORDENA mejor que un solo
indicador, o que el azar?**

Un resultado nulo aca no es redundante con lo anterior: si el score empata con el azar,
entonces las ~200 lineas de config no compran nada y colapsarlas es una mejora de
robustez medible — menos grados de libertad que sobreajustar, y menos brecha entre el
backtest y el vivo ([[project-replay-no-reemplaza-vivo]]).

## 2. Diseno

**Unidad**: una alerta del replay. NO la grilla de 12h de `primer_toque` — este test
vive *dentro* del stream, que es lo que se pidio.

**Resultado**: primer toque a 8% arriba / 8% abajo, horizonte 48h, camino recorrido con
velas de 1h desde la primera vela que abre DESPUES de la alerta. Empate en la misma vela
cuenta como perdida. Alertas sin horizonte completo se descartan (truncar sesgaria hacia
"no resuelto" al final de la ventana).

**Metrica secundaria**: mediana del retorno a 24h neta de costo. Va porque el win rate
depende de donde se pongan las barreras y la mediana no. La **media** se reporta al lado
pero NO decide: [[project-swing-trampa-concentracion]] ya la vio secuestrada por un solo
par dos veces, y [[project-swing-forma-no-expectativa]] dice que la media nunca se mueve.

**Brazos** (~51 sin cruces, ~750 con `--cruces`):

| familia | como | n |
|---|---|---|
| 19 features de kline/alerta, cola alta y baja | quintiles (20%) | 38 |
| el incumbente con sus cortes de produccion | `score>=13` BEST, `score>=11` STRONG | 2 |
| el incumbente por tipo de senal | EXPLOSION / HOLD / BREAKOUT / RIDING / PREBREAK | 5 |
| flags de produccion | vela cerrada, htf_1h_up, htf_4h_up | 3 |
| **control nulo** | tres mascaras aleatorias del 20% | 3 |
| cruces de a pares (opcional) | terciles, para que la interseccion llegue a N_MIN | ~700 |

Cortes por **cuantil**, nunca a dedo: elegir el umbral que mejor queda es look-elsewhere
con otro nombre.

**El control nulo es la pieza central.** Tres mascaras al azar del mismo tamano. Si el
`score` no se separa de ellas, la pregunta esta contestada sin necesidad de que nada
"sobreviva".

## 3. Compuertas

Las de `lote.py`, sin tocar: N_MIN=200, FDR q=0,10 (Benjamini-Hochberg sobre TODOS los
brazos a la vez), p de bloques por semana entera (el binomial esta inflado y se reporta
solo como referencia — [[banco-lote-harness]]), dardo pareado por simbolo, caida del
top-1 y del top-3 contribuyente, y ≥60% de semanas por encima del umbral.

**Costo**: se corre a 0,20% (el supuesto del repo) **y** a 0,50%.
[[project-costos-reales-libro]] midio 1,5x a 6,3x el 0,20%, asi que el 0,20% es el
mejor caso y hay que ver los dos.

## 4. Lo que este test NO puede decir

- **El nivel.** El stream es replay, que genera 1/5 a 1/3 de las alertas reales y da
  vuelta la media. La comparacion es RELATIVA y el sesgo es de modo comun entre brazos;
  el win rate absoluto de esta corrida no se reporta como si fuera el del bot.
- **Nada sobre el universo.** `klines.universe()` es el ranking de volumen de HOY: los
  deslistados no estan ([[project-swing-backtest-sesgo-universo]]).
- **Hasta 1h de camino** se pierde por recorrer el primer toque en velas de 1h con
  alertas en grilla de 15m. Identico para todos los brazos.

## 5. Conteo (medido en el piloto, ANTES de fijar la regla)

Ventana elegida: **26 semanas terminando 2026-08-01**, 200 pares.
Reserva OOS **intocada**: 2026-08-01 en adelante, mas el dump de vivo (269 alertas).

| | piloto (5 sem) | proyectado (26 sem) |
|---|---|---|
| alertas | 636 | ~3.300 |
| con horizonte completo | 623 | ~3.230 |
| resueltas (tocan barrera) | 533 (85,6%) | ~2.780 |
| alertas / semana | 125 | 127 |
| n de una cola del 20% | 106 ✗ | ~556 ✓ |
| n de un cruce de terciles | 58 ✗ | ~305 ✓ |
| MDE cola del 20% | ±9,5pp | **±4,2pp** |
| MDE cruce de terciles | ±12,9pp | **±5,7pp** |
| semanas para el p de bloques | 5 ✗ (min 8) | 26 ✓ |

## 6. Regla de parada — fijada ANTES de correr

El brazo de referencia es la mediana de los tres controles al azar. `MDE = 4,2pp`.

1. **Si `score>=13` y `score>=11` caen dentro de ±4,2pp de los controles al azar en
   `margen`** → *el score no ordena*. Se declara y se pasa a simplificar por robustez,
   no por rendimiento. No se sigue buscando en esta familia.
2. **Si ningun brazo sobrevive las seis compuertas** (lo mas probable) → la familia
   entera se cierra en una corrida. **Prohibido aflojar una compuerta y volver a
   mirar**: asi se fabrica un falso positivo.
3. **Si sobrevive un indicador simple y el score no** → se simplifica A ese indicador,
   pero NO se cree hasta replicar en la reserva OOS de 2026-08 + el dump de vivo. Si no
   replica, muere ahi.
4. **Si sobrevive el score y ningun simple** → la complejidad se gano el lugar; se
   documenta y no se toca `config.json`.
5. **Un brazo que sobreviva solo con costo 0,20% y no con 0,50% no cuenta**, porque el
   costo medido esta arriba de 0,20%.

Lo que **no** es criterio: que un brazo "se vea prometedor", que tenga p binomial bajo
(esta inflado por diseno), o que gane la media (la secuestra un par).

---

## 7. FUGA DECLARADA — lo que vi antes de tiempo

La seccion 2 dice "ningun retorno por brazo". **Eso se rompio.** Al correr el pipeline
completo sobre el piloto como prueba de humo (para no descubrir un crash tres horas
despues), la tabla de medianas imprimio 4 brazos que pasaban N_MIN:

| brazo | n | mediana 24h | vs dardo |
|---|---|---|---|
| linea base | 623 | −1,39% | — |
| INC vela cerrada | 623 | −1,39% | −1,45 |
| INC score>=11 STRONG | 382 | −1,46% | −1,02 |
| INC htf_4h_up | 469 | −2,10% | −2,54 |
| INC htf_1h_up | 435 | −2,28% | −2,50 |

Y la linea base: **mediana −1,39% contra media +2,23%**.

Esto **no es independiente** de la corrida principal: el piloto (2026-05-29 →
2026-06-26) cae DENTRO de la ventana de 26 semanas. Consecuencias, asumidas:

1. Los cuatro brazos de arriba quedan **contaminados**. No se los puede reportar como
   confirmacion de nada; si alguno sobrevive las compuertas, se lo trata como generado
   por esta mirada y hay que mandarlo a la reserva OOS antes de creerle.
2. Los 47 brazos restantes (las 38 colas por cuantil, los 3 controles al azar, los
   tipos de senal) **siguen ciegos** y la regla de parada les aplica entera.
3. La regla 1 de la seccion 6 —la que decide, `score` contra los controles al azar—
   **no esta comprometida**: los controles no se miraron y `score>=13` tampoco.

La leccion operativa, para la proxima: la prueba de humo se corre con la impresion de
resultados apagada, no con la muestra chica.

---

# RESULTADOS — corrida del 2026-08-27

Stream: 7 trozos de 4 semanas, **2.750 alertas**, 2026-01-17 → 2026-07-31 (28 semanas
contiguas, sin solape; 1 duplicada en un borde). 2.711 con horizonte completo, **2.337
resueltas (86,2%)**, 165 pares. Colas del 20% ~467 (MDE **4,5pp**, preregistrado 4,2).

## Regla 1 — DISPARA: el score no ordena

| brazo | n | win rate | margen | vs control |
|---|---|---|---|---|
| CONTROL azar 2 | 480 | 50,63% | −0,62 | — |
| CONTROL azar 3 | 482 | 49,38% | −1,87 | — |
| CONTROL azar 1 | 499 | 48,90% | −2,35 | — |
| **mediana de los 3 controles** | | **49,38%** | **−1,87** | — |
| INC score>=11 STRONG | 1.289 | 51,67% | **+0,42** | +2,29pp |
| INC score>=13 BEST | 263 | 50,19% | **−1,06** | +0,81pp |
| score alto (quintil) | 705 | 50,64% | −0,61 | +1,26pp |

Los tres brazos del incumbente caen **dentro de ±4,5pp** de los controles al azar. Por
la regla 1 de la seccion 6: **el score de 15 puntos no ordena mejor que una mascara
aleatoria**. Las ~200 lineas de `scoring_*` no compran ranking.

El unico numero positivo del incumbente, `score>=11` con +0,42pp, se desarma solo:
`margen_sin_top1` **+0,01pp** (lo sostiene un simbolo), `sem_ok` 53,6% (la compuerta
pide 60%), `p_bloques` 0,66. Es la trampa de concentracion otra vez.

## Regla 2 — DISPARA: 0 sobrevivientes

- **51 brazos simples**: 0 sobreviven, a costo 0,20% **y** a 0,50%.
- **735 brazos con cruces**: 1 sobrevive... y no aguanta la auditoria (abajo).

Linea base del stream: win rate 50,53% contra 51,25% necesario = **−0,72pp**. El stream
de alertas pierde plata a 8/8 en 48h antes de cualquier ranking.

## El unico sobreviviente, y por que muere

`rango_168 bajo + score alto` — n=454, win rate 55,95%, margen +4,70pp, vs dardo +7,58,
sin top-1 +4,05, `p_bloques` 0,0000, sobrevive FDR. Se veia bien.

La compuerta `SEM_N_MIN=20` lo estaba salvando, exactamente como advierte el comentario
de `lote.py`:

| | semanas | alertas | win rate |
|---|---|---|---|
| semanas CONTADAS (n>=20) | 10 | 269 | **63,94%** |
| semanas DESCARTADAS (n<20) | 19 | 185 | **44,32%** |

El filtro tira el **66% de las semanas y el 41% de los trades**, y lo que tira es la
parte que pierde. La regla dispara mucho en las semanas que gana y poco en las que
pierde, asi que el pooled (55,95%) es un artefacto de ponderacion.

Con la semana como unidad independiente —que es el diseno correcto— el veredicto se da
vuelta segun donde se ponga el filtro:

| n_min | semanas | media semanal | p_bloques | |
|---|---|---|---|---|
| >=20 | 10 | 63,54% | 0,0000 | sobrevive |
| >=15 | 13 | 60,19% | 0,0010 | sobrevive |
| >=10 | 19 | 57,40% | 0,0195 | sobrevive |
| >=5 | 27 | 51,03% | 0,5065 | **muere** |
| >=1 | 29 | **47,51%** | 0,8010 | **muere** |

Con todas las semanas pesando uno, la media semanal es **47,51%**, por debajo del
51,25% necesario. **Muere.** Esto no es aflojar una compuerta para que algo pase: es
apretarla, y el resultado es mas estricto, no menos.

**Veredicto: 0 de 735.**

## Lo unico consistente, y por que no es un hallazgo

En la MEDIANA a 24h hay una dosis-respuesta limpia: las alertas sobre cosas **quietas y
no extendidas** rinden mejor que sobre cosas que ya se movieron.

| feature | cola alta | cola baja | spread |
|---|---|---|---|
| vol_24 | −6,70% | −1,25% | **+5,46pp** |
| roc_72 | −5,86% | −1,05% | +4,81pp |
| ext | −6,50% | −2,06% | +4,44pp |
| roc_24 | −5,53% | −1,18% | +4,34pp |
| atr_24 | −5,64% | −1,46% | +4,17pp |

Los controles al azar cubren −1,74% a −3,18% (spread ~1,4pp), asi que 4-5pp es mas que
el ruido de control. Pero **no es un hallazgo**:

1. **No cruzo ninguna compuerta de win rate.** Las medianas no se gatearon.
2. Es casi seguro la asimetria de volatilidad ya medida tres veces
   ([[project-movers-asimetria-volatilidad]]): la alta volatilidad marca 2,33x la cola
   de abajo contra 1,68x la de arriba, o sea **medianas mas bajas por construccion**.
   Redescubrirla por cuarta vez no es informacion nueva.
3. Es la contracara de [[project-swing-entrada-breakout]]: el bot compra el techo de la
   vela de extension. `ext alto` es literalmente eso, y tiene la peor mediana.

## Numero que conviene no olvidar

Mediana de la alerta tipica a 24h: **−2,49%**. Media: **+0,40%**. La brecha es la cola,
otra vez. La alerta tipica pierde; el promedio lo sostienen unas pocas.

## Que queda hecho y que no

- **Hecho**: la familia "ordenar dentro de las alertas" queda cerrada. Ni el score, ni
  ningun indicador simple, ni ningun cruce de a pares ordena.
- **No se toco**: la reserva OOS (2026-08 en adelante + el dump de vivo). No hizo falta:
  no hay sobreviviente que validar.
- **Consecuencia accionable**: colapsar `scoring_*` no cuesta rendimiento medible,
  porque no hay rendimiento que perder. Es una mejora de robustez (menos grados de
  libertad, menos brecha replay-vivo), **no** de ganancia.

---

# SECCION 8 — bateria de mejora del scoring (2026-08-27, segunda corrida)

Cuatro familias sobre **las mismas 2.750 alertas** ya generadas. Multiplicidad: todas
al mismo FDR q=0,10, no una por una.

## A — gatear la MEDIANA (tapa un agujero de la corrida anterior)

La corrida 1 gateo win rate y reporto mediana **sin gatear**. Se construyo
`banco/gate_mediana.py` con las mismas compuertas adaptadas: estadistico = diferencia
pareada DENTRO de la semana (`mediana(brazo en w) − mediana(todas en w)`, que neutraliza
regimen sin tener que detectarlo), bootstrap de semanas, concentracion por conteo de
alertas, FDR, y **barrido de `n_min` incorporado** en vez de auditoria posterior.

Resultado crudo: **83 de 735 sobreviven**, a 0,20% y a 0,50%. Muy por encima del ruido
de FDR (~8 esperados). Pero:

1. **Todas las medianas siguen NEGATIVAS** (mejor: −0,56% contra −2,49% de linea base).
   Pierden menos, no ganan.
2. Los 83 son **el mismo eje**: todos `B + B` (vol bajo, compresion baja, roc bajo, atr
   bajo). FDR controla descubrimientos falsos, no cuenta 83 proxies correlacionados de
   un solo efecto.

## B — el control que decide: ¿ventaja, o solo movimientos mas chicos?

Los sobrevivientes tienen **MAD 3,1–4,2 contra 5,84** de la linea base: se mueven 40-45%
menos. Con deriva base negativa, menos dispersion acerca la mediana a cero **por
mecanica**, sin ninguna ventaja.

Test: medir el retorno en unidades de `atr_24` del propio simbolo.

**83 → 11 sobrevivientes.** ~87% del efecto era escala. Confirma la sospecha.

De los 11 que quedan, el componente dominante es **`ext B` (extension baja): 5 de 11**, y
emparejado con volatilidad **ALTA**: `atr_24 A + ext B`, `rango_168 A + ext B`,
`rs_168 A + ext B`. Otros 5 involucran features de mercado (`mkt_*`), o sea regimen, que
[[project-swing-regimen-familia-agotada]] ya cerro — se descuentan.

Lectura: **no es "elegir monedas quietas", es "elegir monedas volatiles pero entrar antes
de que la vela se extienda"**. Es el defecto de [[project-swing-entrada-breakout]],
ahora como lo unico que aguanta el control de volatilidad. Aun asi la mediana sigue
negativa (−0,58 unidades de ATR contra −0,94 de base).

## C — demora de entrada, con estadistico pareado y control de hora del dia

Primera version mal disenada (comparaba el brazo contra si mismo -> p=1 sin sentido).
Corregida: diferencia pareada por alerta, bootstrap por semana. Salida siempre 24h
DESPUES de entrar, para que todos los brazos tengan igual exposicion.

| demora | dif pareada | sem>0 | p |
|---|---|---|---|
| 1h | −0,023 | 52% | 0,5440 |
| 2h | +0,036 | 66% | 0,0655 |
| 4h | +0,084 | 79% | 0,0095 |
| 8h | +0,182 | 83% | 0,0000 |
| 12h | +0,185 | 76% | 0,0010 |
| **24h (CONTROL misma hora del dia)** | **+0,502** | 69% | 0,0000 |

**El control mata la interpretacion linda.** Demorar 24h mejora MAS que demorar 8h. No
existe una demora optima: la ventana inmediatamente posterior a la alerta es
sencillamente la peor, y cualquier corrimiento aleja de ella. Es
[[project-swing-mediana-vs-cola]] ("elige bien la moneda y mal el momento") medido con
un diseno mas limpio y cuantificado: **+0,50 unidades de ATR por esperar un dia entero**.

Por tipo, todos mejoran con demora salvo **EXPLOSION** (no mejora con ninguna) y
**PREBREAK**, que es mejor inmediato — coherente, es la unica senal PRE-ruptura, todavia
no hay extension que descargar.

Y de nuevo: a 24h de demora la mediana sigue en −0,641. Menos mala, no positiva.

## D — persistencia (el `repeat penalty` que nunca se midio)

| previas en 72h | n | mediana | vs 0 |
|---|---|---|---|
| 0 | 889 | −0,829 | — |
| 1 | 444 | −1,095 | −0,267 |
| 2 | 321 | −1,133 | −0,305 |
| 3-4 | 444 | −0,892 | −0,063 |
| >=5 | 613 | −0,895 | −0,067 |

**0 de 6 sobreviven.** "Primera alerta" da +0,11 con p=0,62 y 59% de semanas. No hay
dosis-respuesta (baja, sube, se aplana). El `repeat penalty` de config no esta capturando
nada medible.

## Veredicto de la bateria

- Lo unico que aguanta el control de volatilidad es **la extension al entrar** (`ext`),
  y el bot **la premia** en vez de penalizarla.
- **Nada da mediana positiva.** Todos los efectos son "perder menos".
- La demora no tiene optimo: el momento de la alerta es el peor y punto.
- Persistencia: nada.

**Lo que queda sin probar (prueba E, cara)**: invertir el signo de los componentes de
`scoring_*` que premian momentum/distancia, regenerar el stream y volver a medir. Es la
unica de la lista que necesita re-replay (~7h por variante). Las dos preguntas que
contestaria: ¿el `ext` bajo sigue vivo cuando el motor lo BUSCA en vez de encontrarlo por
accidente? ¿y alcanza para que la mediana cruce cero? Nada de lo medido aca sugiere que
si — pero es la unica via que queda que no se probo.

---

# SECCION 9 — POR QUE pierde (descomposicion, no veredicto)

Todo en unidades de `atr_24` del propio simbolo. Dardos = 40.000 sorteos de (simbolo,
momento) dentro de la misma ventana.

## Donde se va la plata — mediana del retorno a 24h

| componente | ATR | % de la perdida |
|---|---|---|
| [1] mercado (deriva de fondo del universo) | −0,316 | 34% |
| [2] eleccion de moneda | **+0,012** | ~0% |
| [3] eleccion del MOMENTO | **−0,565** | **60%** |
| [4] costo 0,20% supuesto | −0,070 | 7% |
| **total** | **−0,939** | |

Con el costo medido (0,50%) el termino de costo sube a −0,176 y pasa a ser ~18%, pero
**el momento sigue siendo el doble de grande que el costo y el mercado juntos... no**:
momento 0,565 contra mercado+costo 0,492. Sigue siendo el termino dominante.

**Esto corrige la conclusion del turno anterior.** Se dijo "el cuello es el costo, no la
senal". Es **falso**: a 0,20% el costo explica el 7% de la perdida y a 0,50% el 18%. El
termino dominante es el MOMENTO, con 60%. Atacar el costo (maker) no toca la causa
principal.

Tambien: **la eleccion de moneda aporta +0,012 ATR, o sea cero.** El bot no elige mal la
moneda — no elige, punto. Normalizado por volatilidad, las monedas que alertan rinden
igual que el universo.

## Que hace el precio alrededor de la alerta

Mediana normalizada, 0 = momento de entrada:

| tipo | −24h | −12h | **0** | +12h | +24h | +48h |
|---|---|---|---|---|---|---|
| TODAS | −3,12 | −2,12 | **0** | −0,35 | −0,71 | −0,94 |
| HOLD | −4,08 | −3,27 | **0** | −0,64 | −0,81 | −1,14 |
| RIDING | −3,70 | −2,67 | **0** | −0,42 | −0,98 | −1,03 |
| BREAKOUT | −3,01 | −1,83 | **0** | −0,39 | −0,77 | −0,92 |
| EXPLOSION | −1,84 | −0,91 | **0** | +0,14 | −0,35 | −0,95 |
| **PREBREAK** | **+0,89** | **+0,17** | **0** | **+0,01** | **0,00** | **−0,03** |

**La causa raiz, en una linea: el precio sube +3,12 ATR en las 24h ANTES de la alerta y
baja −0,94 ATR en las 48h despues.** El movimiento entero ocurre antes de que te avisen.
Comprás el techo de una corrida de 3 ATR y te comes toda la devolucion.

**PREBREAK es la excepcion y confirma el mecanismo**: es la unica senal que NO viene
precedida de una corrida (+0,89 = el precio estaba MAS ALTO 24h antes), y es la unica
cuyo camino posterior es plano (−0,03 a 48h). No pierde. Tampoco gana.

## Por que ningun scoring podia arreglarlo

Los 735 brazos fallaron por una razon mecanica, no estadistica: **el defecto es comun a
todas las alertas**. Reponderar cual alerta elegis no sirve cuando todas comparten el
mismo dano estructural (entrar 3 ATR adentro del movimiento). El score reordena opciones
uniformemente danadas.

Y explica por que la demora ayuda pero nunca alcanza: esperar deja que se descargue parte
de la extension, pero seguis con una moneda cuya deriva mediana es negativa (−0,316).

## Lo que esto implica para que probar

El unico arreglo estructural es **disparar ANTES del movimiento, no despues de
confirmarlo**. Es exactamente lo que hace PREBREAK, que es la senal mas chica del sistema
(89 alertas de 2.711 = 3,3%) y la unica sin el defecto.

La pregunta que sigue, y que NO esta contestada: ¿se puede subir el volumen de PREBREAK
—aflojar sus condiciones— sin que se contamine con el defecto de las demas? Su camino es
plano, no positivo, asi que la barra es alta. Pero es la unica familia del sistema que no
compra un techo.
