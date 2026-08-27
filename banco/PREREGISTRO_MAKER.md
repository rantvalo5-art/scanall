# PREREGISTRO — ser el MAKER, no el taker (item 4.1 de `HANDOFF_SIGUIENTE.md`)

> **Escrito ANTES de bajar un solo `aggTrade`.** 2026-08-26. Si algo de este archivo
> cambia despues de ver un numero, el experimento no vale.

## La idea, y por que esta arriba en la lista

`banco/libro.py` midio hoy que cruzar el spread cuesta entre 0,230% y 1,261% segun banda
y tamano de orden — 1,5x a 6,3x el `COSTO_PCT = 0,20%` que el banco venia asumiendo. Eso
cerro la cola iliquida para el taker.

Pero **ese costo es el ingreso de otro**. Poner ordenes limite invierte el signo: en vez
de pagar el spread, lo cobras. Y encaja con el unico patron que funciono en este repo
(seccion 2.2 del handoff): **no direccional y con mecanismo**. Diez familias
direccionales dieron cero; vender volatilidad —no direccional, con mecanismo de prima de
seguro— dio +20,96%/ano durante 5,3 anos y murio por competencia, no por ser falsa.

## Lo que puede matarlo, y es lo que se mide aca

**Seleccion adversa.** Al maker lo llenan cuando el otro tiene razon. Compras en el bid
justo antes de que baje. La pregunta cuantitativa exacta:

> **Despues de un fill, ¿el mid se mueve en contra por menos que el medio-spread cobrado?**

Si no, el spread es una ilusion contable.

## La metrica que decide: spread realizado

Notacion. Para cada `aggTrade`:
- `d = +1` si el agresor fue el comprador (`m == false`, ejecuto contra el ask),
  `d = -1` si el agresor fue el vendedor (`m == true`, ejecuto contra el bid).
- El **maker toma el lado opuesto**: vende cuando `d=+1`, compra cuando `d=-1`.
  Su posicion post-fill es `-d`.

El maker transo en `p = mid(t) + d*(s/2)`, o sea cobra `s/2`. Su marca a mercado sobre la
posicion `-d` despues de un lapso `D` es `-d*(mid(t+D) - mid(t))`. Sumando:

```
RS(D) = s/2 - d*(mid(t+D) - mid(t))          [ medio-spread - seleccion adversa ]
      = d * (p - mid(t+D))                    [ algebraicamente identico ]
```

**La segunda forma es la que se computa** y es importante que sean iguales: no necesita
`mid(t)` ni `s`. Estimar el mid *en el instante del fill* es justo donde un mid
reconstruido de trades esta mas sesgado (el lado que acaba de imprimir esta fresco y el
otro rancio). La forma `d*(p - mid(t+D))` usa solo el precio del fill, su lado, y un mid
**futuro**, evaluado en un instante que NO esta condicionado a un trade. El sesgo de
rancidez ahi es simetrico.

`RS` se reporta en % sobre el mid futuro. **Es la ganancia bruta del maker por dolar
transado de un lado**, marcada al mid `D` despues. Un MM que corre plano cobra `RS` en
cada punta, o sea `2*RS` por round trip contra `2*fee_maker` de comision: la condicion es
la misma de un lado que del round trip.

### La headline es BALANCEADA POR LADO

```
RS_bal(D) = ( mean{RS | fills de compra del maker} + mean{RS | fills de venta} ) / 2
```

Motivo: una deriva del periodo entra con signo opuesto en compras y ventas del maker. Si
los fills estan desbalanceados (mas agresores compradores que vendedores, cosa comun), el
promedio pooleado se come esa deriva y la disfraza de spread capturado. Este repo ya se
engano varias veces con exactamente eso. **El pooleado se reporta al lado, como
diagnostico, no como veredicto.** Si `RS_buy` y `RS_sell` difieren mucho, es direccion
del periodo, no captura de spread.

## Los tres pisos de comision contra los que se compara

`RS` es bruto. La comision del maker es un **dato conocido, no estimado** (tarifario de
Binance, VIP 0):

| linea | maker por lado | de donde |
|---|---|---|
| spot sin BNB | **0,1000%** | spot VIP 0 |
| spot con BNB (-25%) | **0,0750%** | spot VIP 0 + descuento BNB |
| futuros USD-M | **0,0200%** | USD-M VIP 0 |

Se mide sobre **spot**, que es lo que el handoff especifica y donde vive todo el repo. La
linea de futuros se reporta porque es la unica que puede quedar viva: el fee de maker de
futuros es 5x mas barato. **Si la cruza, el paso siguiente NO es concluir que sirve — es
re-medir sobre `aggTrades` de futuros**, porque el spread y la seleccion adversa de un
perpetuo no son los del spot. Eso engancha con el item 4.2.

## La regla de parada, exacta

`D = 60s` es el horizonte que decide (los `D` de 1s, 10s y 300s se reportan como perfil
de decaimiento, no deciden nada).

**Se cierra el market making en spot si:**

```
mediana sobre los 20 pares de [ RS_bal(60s) - 0,0750% ]  <=  0
```

(se usa el fee con BNB, 0,0750%, que es el **mas favorable** de los dos spot: si no
alcanza ni con el descuento, tampoco sin el).

**Se cierra el item 4.1 entero si ademas:**

```
mediana sobre los 20 pares de [ RS_bal(60s) - 0,0200% ]  <=  0
```

**Se cierra igual, aunque los numeros crucen, si:**
- el resultado no aguanta `sin_top3` (sacar los 3 pares que mas aportan da vuelta el
  signo de la mediana), o
- el `p` de **bloques por dia** (14 dias = 14 bloques, cada dia pesando uno, bootstrap
  de 2.000 reps sobre la media diaria de `RS_bal`) es `>= 0,05` para la mediana de los
  pares, o
- la conclusion **no se sostiene con los dos estimadores de mid** (ver abajo), o
- la conclusion **no se sostiene** al quedarse solo con el primer fill de cada barrido.

**Lo que NO se permite**, escrito ahora para no poder inventarlo despues:
- mover `D` a otro horizonte porque 60s "dio feo",
- quedarse con la banda o el subconjunto de pares que cruza y reportar eso como
  resultado (la banda se reporta SIEMPRE entera, las cuatro),
- cambiar la ventana o pedir mas dias,
- comparar contra 0 en vez de contra el fee. `RS > 0` no es un hallazgo: el spread
  realizado bruto es positivo casi siempre en la literatura. La vara es el fee.

## Dos estimadores del mid, y los dos tienen que dar lo mismo

De `aggTrades` no sale el libro, asi que el mid se reconstruye. Dos formas independientes,
y la conclusion tiene que aguantar con las dos:

1. **`mid_bipunta`**: `(bid_est + ask_est)/2`, con `bid_est` = ultimo precio con `m=true`
   y `ask_est` = ultimo precio con `m=false` (forward-fill). Es el midpoint de las dos
   ultimas impresiones de lado opuesto.
2. **`mid_ultimo`**: el ultimo precio transado, sin importar el lado. Tiene rebote
   bid-ask, pero el rebote es media cero cuando el instante de evaluacion no esta
   condicionado al lado — y `t+D` no lo esta.

Si los dos dan el mismo veredicto, el numero no es un artefacto de la reconstruccion.
Si difieren, gana **el mas conservador** (el que da menos `RS`).

**Validacion cruzada del spread:** el spread cotizado implicado por la reconstruccion se
compara contra el que `banco/libro.py` mide del libro de verdad (`/api/v3/depth`) para los
mismos simbolos. No es una compuerta —el libro es de ahora y los trades son de la
ventana— pero un desacuerdo grande invalida la reconstruccion.

## Por que este piso es OPTIMISTA a proposito

Todo lo que no se modela juega **en contra** del maker, nunca a favor:
- **Posicion en la cola**: se supone que el maker se lleva el fill siempre. Un MM real
  esta detras de otros y solo cobra parte de esos fills — y le tocan preferentemente los
  malos.
- **Riesgo de inventario**: se supone que corre plano. No corre plano.
- **Competencia de latencia**: se supone que puede cancelar. No siempre.
- **Barridos**: la variante "solo el primer fill del barrido" es la favorable al maker
  (los fills profundos de un barrido son los mas adversamente seleccionados). Se reporta
  con y sin.

**Si este piso ya es negativo, esta cerrado y no hay nada mas que medir.**

## Datos

- **20 pares**, 5 de cada banda de `libro.py` (rank 1-50, 51-200, 201-400, 401-600),
  elegidos en posiciones fijas dentro de cada banda y **congelados en disco** (`pin`), asi
  la corrida es reproducible.
- **Ventana FIJA 2026-08-11 → 2026-08-25** (14 dias UTC completos).
- Muestreo: **3 ventanas de 10 min por dia y por par**, en instantes pseudoaleatorios con
  semilla fija. No hace falta la serie contigua: hace falta n y bloques. Cada ventana se
  baja con 90s extra de cola para poder evaluar `mid(t+300s)` sin mirar fuera.
- Fuente: `/api/v3/aggTrades` de spot, cacheado en `.aggtrades_cache/` como `.npz`.

**Sesgo de universo, declarado:** el ranking de volumen es el de HOY (mismo sesgo que
todo el banco). Para esta medicion importa menos que de costumbre —no se esta midiendo un
retorno, se esta midiendo microestructura— pero un par que hoy esta en rank 500 y hace dos
semanas era mas liquido tiene el spread de la ventana, no el de hoy. La comparacion contra
`libro.py` justamente sirve para ver cuanto se movio.

## Conteo de n y MDE, ANTES de estimar

La regla del handoff ("contar el n POST-JOIN y calcular el MDE con la nula real ANTES de
estimar") aplica tal cual. **Primero se cuentan los fills por par y por dia, y se calcula
el MDE al 80% de potencia sobre la dispersion diaria.** Si para un par el MDE es mas
grande que la distancia al fee, ese par no puede decidir nada y se marca `SIN POTENCIA`
en vez de dar un veredicto. Con pocos pares con potencia, el resultado global es
"no se pudo medir", no "no esta".

## Prior declarado

Lo espero **negativo en spot y por goleada**: el spread del top-50 ronda 1-3 bps, o sea
un medio-spread de 0,005-0,015%, contra un fee de maker de 0,075%. Ahi el fee solo ya se
come el spread entero varias veces. La unica zona con chance aritmetica es la **cola**,
donde `libro.py` midio spreads de 0,11-0,15% (medio-spread 0,055-0,075%) — y eso empata
apenas con el fee de spot **antes** de restar seleccion adversa. Contra el fee de futuros
(0,0200%) la cola si tiene margen aritmetico, y ese es el unico desenlace que dejaria algo
vivo: seria un resultado sobre **futuros**, no sobre spot, y habria que re-medirlo con
datos de futuros antes de creerlo.
