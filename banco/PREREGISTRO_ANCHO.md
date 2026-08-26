# PREREGISTRO — familias anchas (volumen, forma, transversal, ciclo de vida)

> **Escrito MIENTRAS corre el lote de descubrimiento, antes de ver un solo resultado.**
> 2026-08-24. Si algo de este archivo cambia despues de mirar los numeros, no vale.

## De donde salio

`klines()` se quedaba con `[t,h,l,c]` y tiraba apertura, volumen, quote volume, numero
de trades y volumen taker comprador. El banco mato 450+ hipotesis de precio sin haber
medido nunca una feature de volumen ni de forma de vela — que es justo sobre lo que esta
construido el screener en vivo. `klines(..., full=True)` lo destraba.

El lote de descubrimiento (`lote_ancho.py`) corre ~43 hipotesis x 2 lados sobre
**2025-08-01 → 2026-08-01**, 200 pares, pin `base200`.

## La ventana de prueba, elegida ahora

**OOS = 2024-08-01 → 2025-08-01.** Anterior, no posterior, por dos razones: los datos ya
existen (no hay que esperar) y ninguna corrida de este repo la miro nunca — el pin
`base200` y todos los lotes arrancan en 2025-08-01.

Riesgo conocido y aceptado: el universo `base200` es el top-volumen de HOY, asi que en
2024 hay supervivencia (pares que hoy son grandes y entonces no existian quedan afuera
por falta de velas). Eso mueve el NIVEL absoluto, no la comparacion contra la linea base
pareada, que es la compuerta que decide.

## La regla de parada, exacta

Una hipotesis que sobreviva las seis compuertas en descubrimiento se re-corre tal cual
—mismo corte de quintil, mismo lado, mismo target/stop/horizonte— sobre la OOS.
**Sobrevive de verdad solo si en OOS cumple las tres:**

1. `margen > 0` (cruza el umbral de win rate necesario con costos),
2. `vs_pareado > 0` (le gana a la linea base del MISMO simbolo),
3. `semanas arriba >= 55%`.

No se re-aplica FDR en OOS: son pocas hipotesis pre-especificadas, no un lote.

**Lo que NO se permite**, escrito antes para no poder inventarlo despues:
- reajustar el corte del quintil (0,20 / 0,80 son los del descubrimiento),
- cambiar target/stop/horizonte (8/8/30d, paso 12h),
- probar la OOS "por partes" y quedarse con el tramo que da,
- promover una hipotesis que en descubrimiento murio en FDR.

## Prior declarado

Este repo lleva ~15 lineas de investigacion cerradas. La causa de muerte mas comun no es
"no habia senal" sino **ventana, concentracion, o cola simetrica**. La expectativa
honesta aca es 0 sobrevivientes en OOS. La familia que tiene una razon mecanica para ser
distinta es el **flujo agresor** (`taker` = volumen taker comprador / volumen total): es
la unica feature DIRECCIONAL de una vela, y todo lo que murio hasta ahora murio por
ensanchar las dos colas por igual. Si algo sobrevive, lo esperable es que sea de ahi.
