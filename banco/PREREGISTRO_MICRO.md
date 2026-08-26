# PREREGISTRO — microestructura intra-vela

> Escrito el **2026-08-25**, **antes de calcular un solo resultado**. El motor
> (`banco/micro.py`) esta cableado y validado sobre BTCUSDT, pero solo se miraron
> invariantes (rangos, coberturas, medianas de las features) — **ningun win rate, ningun
> retorno**. Este preregistro SI es ciego, a diferencia de `PREREGISTRO_UNLOCKS_2.md`.
>
> Las secciones 1-4 (diseno) se escriben ahora. La seccion 5 (n post-join y MDE) se llena
> con una corrida `--solo-conteo`, y **recien despues** se cierra la seccion 6 (regla de
> parada). Ese orden es la regla que salio del error de unlocks: contar antes de decidir.

---

## 1. Por que esta familia no comparte causa de muerte con lo ya cerrado

El repo cerro 450+ hipotesis de precio, volumen y forma de vela. La ultima
(`lote_ancho.py`, 0 de 86) midio **a resolucion horaria**: cuerpo, mechas, ratio taker,
efficiency ratio sobre 24 velas de 1h, volumen por unidad de movimiento, tamano medio de
trade. Todo eso sale del OHLCV de una vela de 1h.

**Lo que no sale de una vela horaria es como se recorrio esa hora.** Dos horas con el
mismo open, high, low, close y volumen pueden ser:

- un salto en una vela de 5m y once velas quietas, o una deriva pareja de doce;
- volumen en una rafaga contra volumen constante;
- cero cambios de signo contra once;
- varianza a 5m que sobrevive a la agregacion (tendencia) contra varianza que se cancela
  (rebote entre bid y ask).

Y tres de las features de aca **no son de precio sino de liquidez**: la autocorrelacion
lag-1 de retornos de 5m, el spread implicito de Roll (1984) y el Amihud a 5m. Eso importa
porque el [[project-swing-techo-condicional]] y el [[project-swing-techo-oraculo]] dicen
que en la informacion contenida en el precio no queda nada. La liquidez no sale del precio:
sale de cuanto cuesta moverlo.

## 2. El riesgo obvio, y la unica razon por la que este test dice algo

Casi todas estas features correlacionan con volatilidad. Y este repo ya midio **tres
veces** que la volatilidad ensancha las dos colas por igual y no da direccion
([[project-movers-asimetria-volatilidad]], [[project-swing-cola-simetrica]],
[[project-swing-mediana-vs-cola]]). Redescubrir "las volatiles se mueven mas" por cuarta
vez no seria un hallazgo, seria un artefacto.

Por eso **cada hipotesis se corre en dos versiones**:

- **cruda**: quintil superior/inferior de la feature sobre todo el panel;
- **condicional (`| vol`)**: quintil de la feature **dentro de cada quintil de `atr_24`**,
  asi la mascara tiene la misma mezcla de volatilidad que la linea base.

Verificado en el cableado (BTCUSDT): `roll_sp alto` tiene **1,29x** la volatilidad media
de la base, y su version `| vol` la baja a **1,04x**. El control hace lo que dice.

**La pregunta del preregistro no es "la forma del camino predice" sino "predice mas alla
de la volatilidad".**

## 3. Especificacion. UNA sola.

- **Universo**: `base200` pineado (el mismo de `lote.py` y `lote_ancho.py`), pares con
  >=2000 velas de 1h y >=576 velas de 5m en la ventana.
- **Ventana**: **2025-08-01 -> 2026-08-01**. La estandar del banco.
- **Entradas**: primer toque, barreras **+-8%**, horizonte **30 dias**, paso **12h**,
  costo **0,20%**. Los defaults de `lote.py`; no se tocan.
- **Resolucion de las features**: velas de **5m** (`full=True`), agregadas **por hora**.
  La fila de la hora H describe lo que paso **dentro de H**; la entrada se toma al cierre
  de H (offset -1, la convencion del repo). No hay lookahead.
- **Direcciones**: se corren **las dos** (largo y corto), con el q de FDR partido al medio
  (**0,05 por lado**) para que la correccion cubra el total. La cola de abajo midio mas
  fuerte que la de arriba en `movers.py`, asi que correr solo largo seria repetir el error
  de funding.

### Las 12 features base (todas imposibles de calcular con velas de 1h)

| # | feature | que mide |
|---|---|---|
| 1 | `efic_h` | \|desplazamiento\| / camino recorrido dentro de la hora. 1 = linea recta |
| 2 | `vr_h` | variance ratio: (suma r)^2 / suma(r^2). <1 reversion, >1 tendencia |
| 3 | `chop` | suma de rangos de 5m / rango de la hora. Cuantas veces se recorrio |
| 4 | `hhi` | Herfindahl del volumen de las 12 velas. 1 = parejo, 12 = todo en una |
| 5 | `cambios` | fraccion de velas que dieron vuelta el signo del retorno |
| 6 | `centro` | posicion (0-11) donde ocurrio el movimiento, ponderada por \|r\| |
| 7 | `tk_sd` | desvio del ratio taker comprador entre las 12 velas |
| 8 | `tk_frac` | fraccion de las 12 velas con taker comprador > 50% |
| 9 | `tam` | tamano medio de trade dentro de la hora |
| 10 | `amihud` | \|retorno\| por dolar operado, promediado a 5m. Impacto / iliquidez |
| 11 | `ac1_5m` | autocorrelacion lag-1 de retornos de 5m sobre 288 velas. <0 = rebote |
| 12 | `roll_sp` | spread efectivo implicito de Roll: 2*sqrt(-cov(r_t, r_t-1)) |

> **7, 8 y 10 no son repeticiones de lo muerto.** `lote_ancho` midio el **nivel** del
> ratio taker de una vela horaria (`taker`, `taker_24`) y lo mato. Aca se mide su
> **dispersion y consistencia entre las 12 velas de 5m**, que es otro objeto: un taker
> horario de 0,55 puede ser doce velas de 0,55 o seis de 0,9 y seis de 0,2. Y
> `vol_por_mov` era el ratio de los promedios de 24h; `amihud` es el promedio de los
> ratios a 5m, que es la definicion estandar y no es la misma cantidad.

Las 10 primeras se prueban **al cierre de la hora de entrada** y **promediadas a 24h**
(el regimen de microestructura del par, menos ruidoso). `ac1_5m` y `roll_sp` existen solo
como ventana de 288 velas. Total: **24 columnas**.

### Conteo de hipotesis

24 features x 2 colas (alto/bajo) x 2 versiones (cruda / `| vol`) = **96 hipotesis**,
x 2 direcciones = **192**. Se corren TODAS; ninguna se elige despues.

## 4. Las compuertas

Las seis cableadas de `banco/lote.py`, sin aflojar ninguna:

1. n >= 200;
2. cruza el win rate necesario ((8 + 0,20) / 16 = **51,25%**);
3. sobrevive Benjamini-Hochberg sobre el lote (q = 0,05 por lado);
4. le gana al **pareado** (mismo simbolo: es timing, no seleccion de moneda);
5. sobrevive sacar el **top-3** simbolos, y sacar el **top-1**;
6. **>= 60% de las semanas** por encima del umbral.

El p que decide es el de **bloques semanales**, no el binomial. Aca la compuerta semanal
**si aplica** (a diferencia de unlocks): son ~146.000 entradas en 52 semanas, no eventos
esparcidos, asi que `SEM_N_MIN = 20` se cumple holgado.

---

## 5. La muestra, contada POST-JOIN

Corrido con `py -3.13 -u micro.py --solo-conteo` (log `micro_conteo.log`).
**Ningun win rate fue calculado para escribir esto.**

| | |
|---|---|
| pares con 1h y 5m en la ventana | **187** de 200 |
| horas con features de microestructura | 1.540.881 |
| entradas de primer toque | 117.350 |
| **resueltas (la muestra del test)** | **111.330** (94,9%) |
| n por mascara de quintil | mediana **22.140**, minimo 15.701 |

**Cobertura por feature: 99,7% a 100%** en las 24 columnas. No hay ninguna que quede
inutilizable por huecos, ni ninguna que cubra un subconjunto raro del panel.

La compuerta semanal **si aplica** aca, a diferencia de unlocks: 52 semanas con miles de
entradas cada una, muy por encima de `SEM_N_MIN = 20`.

### Potencia, medida con la nula (antes de mirar el dato real)

Nula = **desplazamiento circular de la matriz de features dentro de cada simbolo**, con
corrimiento al azar por simbolo. Conserva la autocorrelacion de las features y la de los
resultados, y rompe solo la alineacion entre las dos. Una permutacion plana seria una nula
demasiado facil: destruiria la estructura temporal que hace que las compuertas cuesten.

Sobre un desplazamiento (`lote()` completo, 96 hipotesis con n>=200):

| estadistico | media | sd | p95 |
|---|---|---|---|
| `margen` (win rate − necesario) | −2,634pp | 0,655 | −1,776 |
| `vs_pareado` | −0,079pp | 0,451 | +0,591 |
| `sem_ok` (fraccion de semanas arriba) | 0,429 | 0,033 | 0,490 |

**Tres numeros que fijan lo que este test puede y no puede decir:**

1. **MDE = 1,31pp** (2 sigma sobre `margen`). Una feature que separe mas de eso se ve.
2. **La linea base esta 2,63pp POR DEBAJO del umbral.** O sea que ver la diferencia no
   alcanza: para cruzar la compuerta 2 una feature tiene que aportar **+2,6pp solo para
   llegar a cero**, y algo mas para cruzar. Detectar y ser operable son dos varas
   distintas, y la segunda es el doble de alta que la primera.
3. **La compuerta semanal es la que ata.** Pide `sem_ok >= 0,60` y la nula da p95 = 0,490:
   son mas de 3 sd. Es la compuerta que va a matar casi todo, como en el resto del banco.

### El look-elsewhere, calibrado

`py -3.13 -u micro.py --nula 5` (log `micro_nula.log`): **0 sobrevivientes de 192 en las
cinco repeticiones**. Las seis compuertas encadenadas son lo bastante estrictas para que
el azar no las cruce en este lote. Por eso la regla de abajo puede pedir literalmente
"al menos uno".

## 6. Regla de parada, exacta

**La familia se declara MUERTA salvo que se cumplan TODAS:**

1. **Al menos una hipotesis cruza las seis compuertas.** Justificado por la nula: 0 de 192
   en 5 desplazamientos, o sea menos de 1 falso positivo por cada 960 hipotesis-corrida.
2. **El sobreviviente es una version `| vol`**, o su contraparte `| vol` tambien cruza.

   > Un sobreviviente que existe SOLO en la version cruda **no cuenta como hallazgo**:
   > es volatilidad, que este repo ya midio tres veces y que ensancha las dos colas por
   > igual. Se reporta como confirmacion de lo ya sabido, no como resultado nuevo.
3. **Replica en la ventana OOS virgen 2024-08-01 -> 2025-08-01** (seccion 8), con la
   misma especificacion y sin re-ajustar nada.

Si sobrevive algo en 1 y 2 pero cae en 3, el veredicto es **"no replica"**, que es
distinto de "no hay nada" y se anota como tal.

Si no sobrevive nada, el veredicto es: **no hay informacion en la forma del camino
intra-hora, mas alla de la volatilidad, de tamano >= 1,31pp, en el top-200 por volumen, a
horizonte de 30 dias con barreras de +-8%.** Con los limites de la seccion 7 —
especialmente que la cola iliquida no esta mirada.

## 7. Los sesgos, declarados

1. **Sesgo de universo**: `base200` es el ranking de volumen de HOY. Los pares deslistados
   durante la ventana no estan y los que sobrevivieron son los que mejor les fue. Sesga
   hacia mejor. Es el mismo sesgo de todo el banco y no se puede arreglar desde aca.
2. **La cola iliquida no esta**: `base200` es donde la competencia es maxima. Varias de
   estas features (Amihud, Roll, `ac1_5m`) son **mecanicamente mas grandes abajo**, y ahi
   el modelo de costos de 0,20% sin slippage esta mal. Si algo aparece arriba, aparece en
   el terreno mas dificil; si NO aparece, **no se puede concluir que no exista abajo**.
3. **Costos**: 0,20% ida y vuelta sin slippage. Para features de iliquidez esto es
   optimista por construccion — justamente las horas de Amihud alto son las mas caras de
   operar. Un sobreviviente de esa familia hay que mirarlo con eso en la cabeza.

## 8. OOS

`PREREGISTRO_ANCHO.md` dejo intacta la ventana **2024-08-01 -> 2025-08-01** y nunca se
uso, porque no sobrevivio nada que promover. **Sigue virgen.** Si algo cruza aca, la
replicacion se hace ahi, y **solo para los sobrevivientes** (bajar 5m de otro ano completo
para 200 pares cuesta ~5 horas; para tres features no).

## 9. Lo que NO se permite, escrito antes

- cambiar el corte de quintil (0,20) despues de ver los numeros;
- reportar la version cruda de una feature cuya version `| vol` no cruza — eso es
  volatilidad disfrazada, que es el punto entero de la seccion 2;
- agregar features despues de la corrida;
- mover ventana, barreras, horizonte o paso;
- promover a OOS algo que no haya cruzado las seis compuertas.

---

# RESULTADOS (2026-08-25)

> Nada de lo de arriba se toco. Corrida: `py -3.13 -u micro.py --out micro.csv`
> (log `micro.log`, tabla `micro.csv`).

## Veredicto: FAMILIA CERRADA. 0 sobrevivientes de 192.

La compuerta 1 de la regla de parada pedia **al menos una** hipotesis cruzando las seis
compuertas. No hay ninguna. Las compuertas 2 y 3 no llegan a evaluarse.

Linea base: win rate **48,65%** sobre 111.330 entradas resueltas, contra 51,25% necesario.

## Donde mueren, que es lo unico interesante

**65 de las 192 cruzan el umbral. Las 65 mueren en el mismo lugar: la correccion por
multiplicidad, porque el p por bloques dice que no hay nada.**

Ninguna llega siquiera a las compuertas de pareado, concentracion o semanas. Mueren antes.

### La brecha p_indep / p_bloques, en su version mas extrema

| hipotesis | lado | margen | p_indep | p_bloques | sem_ok |
|---|---|---|---|---|---|
| `amihud_24 bajo \| vol` | corto | **+4,36pp** | **1,1e-36** | **0,3845** | 0,531 |
| `tk_sd_24 bajo \| vol` | corto | +3,98pp | 6,4e-33 | 0,3685 | 0,551 |
| `tk_sd_24 bajo` | corto | +3,97pp | 2,1e-33 | 0,3630 | 0,531 |
| `hhi_24 bajo` | corto | +3,79pp | 3,7e-30 | 0,2995 | 0,510 |
| `amihud bajo \| vol` | corto | +3,69pp | 1,6e-26 | 0,4155 | 0,490 |
| `hhi_24 bajo \| vol` | corto | +3,57pp | 1,1e-26 | 0,3365 | 0,510 |
| `hhi bajo` | corto | +3,51pp | 4,4e-26 | 0,3980 | 0,551 |
| `amihud_24 bajo` | corto | +3,42pp | 2,7e-22 | 0,4575 | 0,510 |

De las 65 que cruzan: **p_indep mediana 0,0001, p_bloques mediana 0,4495**.

> **37 hipotesis con p_indep < 0,001. CERO con p_bloques < 0,05.**

Es la demostracion mas limpia que dio este repo de por que el p binomial no sirve aca.
Un p de **1,1e-36** —que en cualquier paper seria un hallazgo de libro— se convierte en
**0,38** cuando la unidad de remuestreo pasa a ser la semana. Con n=22.000 entradas
solapadas y regimen autocorrelacionado, el n efectivo no es 22.000: es ~52.

### La compuerta semanal confirma lo mismo por otro lado

De las 65 que cruzan, la mejor consistencia semanal es **0,592** y la mediana **0,531**,
contra 0,60 que pide la compuerta. O sea: estas senales estan arriba del umbral en
**algo mas de la mitad de las semanas**, que es lo que se espera de una moneda al aire.
El margen de +4pp no viene de estar bien seguido: viene de unas pocas semanas gordas.

Misma causa de muerte que [[project-oi-shock-bajista-vivo]].

## Lo que el control de volatilidad SI demostro

**No es volatilidad disfrazada, y eso vale registrarlo.** De las 19 hipotesis crudas con
margen mayor a +1pp, la version condicionada al quintil de `atr_24` **retiene el 94% del
margen** (mediana). Varias mejoran: `amihud_24 bajo` pasa de +3,42 a **+4,36** al
controlar por volatilidad.

| hipotesis | lado | cruda | condicional | retiene |
|---|---|---|---|---|
| `tk_sd_24 bajo` | corto | +3,97 | +3,98 | 100% |
| `hhi_24 bajo` | corto | +3,79 | +3,57 | 94% |
| `amihud_24 bajo` | corto | +3,42 | **+4,36** | 127% |
| `cambios_24 alto` | corto | +3,22 | +3,28 | 102% |
| `hhi_24 alto` | largo | +2,85 | +1,02 | 36% |

O sea: la seccion 2 tenia razon en preocuparse, pero el resultado es el contrario del
temido. **La forma del camino intra-hora NO es un proxy de volatilidad** — es informacion
distinta. Simplemente no es temporalmente consistente.

(La excepcion es el lado largo: `hhi_24 alto` y `tk_sd_24 alto` pierden 55-65% al
controlar por vol. Ahi si habia volatilidad disfrazada. En el corto no.)

## La direccion, que es coherente y no sirve igual

Las ocho mejores son **todas del lado corto**, y cuentan una historia consistente:
`amihud bajo` (liquido), `tk_sd bajo` (flujo agresor parejo entre las 12 velas),
`hhi bajo` (volumen repartido, sin rafagas) → **las horas ordenadas y liquidas preceden
caidas**. El lado largo dice el reflejo: iliquidez y rafagas preceden subidas.

Es una historia mecanicamente plausible y **sobrevive el control pareado** (+3,25pp
`vs_pareado` para la mejor), asi que no es seleccion de moneda ni deriva del mercado.
Pero vive en pocas semanas, y por eso no se promueve. **Escrito para que la proxima
sesion no lo redescubra creyendo que es nuevo.**

## Lo que este resultado cierra, dicho con sus limites

**No hay informacion en la forma del camino intra-hora, mas alla de la volatilidad, de
tamano >= 1,31pp y temporalmente consistente, en el top-200 por volumen, a horizonte de
30 dias con barreras de +-8%.**

Los limites de la seccion 7 siguen en pie, y uno importa mas que los otros: **la cola
iliquida no se miro.** `amihud`, `roll_sp` y `ac1_5m` son mecanicamente mas grandes abajo,
y `base200` es donde la competencia es maxima. Que no aparezca arriba **no dice** que no
exista abajo — pero mirarlo requiere rehacer antes el modelo de costos, porque 0,20% sin
slippage es exactamente lo que esta mal en ese terreno.

La OOS 2024-08 -> 2025-08 **sigue virgen**: no se uso, porque no hubo nada que promover.
