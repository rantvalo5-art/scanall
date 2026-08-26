# PUENTE 2 — el replay con la cadencia REAL de produccion

> **Escrito ANTES de correr.** 2026-08-24. Preregistro NUEVO, como exige el cierre de
> `PUENTE.md`: aquel admitia una sola alternativa y ya se uso. Si algo de este archivo se
> edita despues de ver un resultado, el experimento no vale.

## Por que hay un intento 3, y por que no es "insistir hasta que de"

`PUENTE.md` cerro con B1 FALLA y anoto un defecto: se penso que produccion escaneaba un
universo mucho mayor (`general.TOP_N = 9999`) que el `--max-pairs 200` del replay. Se
declaro defecto y **no** se re-corrio. Correcto.

Al medir ese defecto, resulto ser **falso**, y aparecio la causa verdadera. Medido hoy
sobre `screener_pairs_snapshot`, que registra TODAS las corridas de produccion (incluidas
las que no alertaron — por eso es mejor que mirar los gaps entre alertas):

| | valor medido | fuente |
|---|---|---|
| cadencia de produccion | **2,00 min** (p10 1,83 / p90 2,15) | gaps entre `run_at` |
| pares por corrida | **191 a 223** | filas por `run_at` |
| `TOP_N = 9999` | no ata nada; ata `MIN_QUOTE_VOLUME = 600000` | `config.json` |

O sea: `--max-pairs 200` **ya era correcto**. Lo que estaba mal era la cadencia, y por un
factor grande — 5 min contra 2 min reales.

## La prediccion, hecha antes de correr

Alertas de extension **por corrida**, no por dia:

| corrida | corridas/dia | alertas/dia | por corrida |
|---|---|---|---|
| replay 15 min | 48 | 5,6 | 0,117 |
| replay 5 min | 288 | 9,7 | **0,0337** |
| en vivo (2 min) | 720 | 26,2 | **0,0364** |

El replay a 5 min y el vivo ya producen **la misma tasa por escaneo** (8% de diferencia).
Si la poblacion es solo cadencia, el replay a 2 min tiene que dar:

> **720 x 0,0337 = ~24 alertas/dia** (contra 26,2 en vivo)

**Esa es la prediccion cuantitativa de este preregistro.** Si sale eso, la brecha de
poblacion queda explicada y cerrada. Si sale muy por debajo, hay una causa que no es ni
cadencia ni universo, y eso es un hallazgo distinto (candidatos a mirar despues, no
ahora: cooldowns/anti-spam contra cadencia, `TOP_ALERT_COUNT=5` por corrida, y si el vivo
evalua velas en formacion que el replay no ve).

## La corrida — una sola

```
config.json tal como esta deployada
--max-pairs 200  --scan-interval-min 2  --end-date 2026-08-16  --weeks 8
alertas filtradas a >= 2026-06-26
```

Sin alternativas declaradas. **Si falla, falla.**

Nota de universo, declarada ahora: el snapshot solo cubre desde **2026-07-25** (retencion
~30 dias), asi que no se puede reconstruir el universo historico de los primeros 29 dias
de la ventana. El replay usa el ranking de volumen de hoy. Se midio el sesgo: el universo
que produccion tenia en 07-26 / 08-05 / 08-15 esta cubierto en **85,5% / 90,2% / 87,9%**
por el top-300 de hoy. Es un faltante de ~10-15%, en contra de la hipotesis, no a favor.

## Compuertas — las MISMAS de PUENTE.md, sin aflojar

- **B1 (primaria).** Media a 4h en condicion realista > 0 **y dentro del IC95 en vivo
  [+0,14%, +2,04%]**.
- **B2 (composicion).** Alertas de extension entre **0,5x y 2x** el vivo (13 a 53/dia),
  con EXPLOSION y BREAKOUT ambas presentes.
- **B3 (concentracion).** Las compuertas (a) media, (b) sin top-3 y (d) sin el peor
  simbolo, OK tambien en el replay.

Referencia en vivo (preexistente, no se toca): media 4h **+0,550%**, 1.363 alertas,
26,2/dia, 280 simbolos distintos, 2026-06-26 -> 2026-08-16.

## Que significa cada resultado

- **B1 OK + B2 OK** -> el replay reproduce el vivo cuando se lo corre a la cadencia real.
  El instrumento sirve, y recien ahi se habilita ir a buscar tramos alcistas a la historia.
- **B2 OK pero B1 FALLA** -> el mas informativo de todos. Significa que el replay genera
  la poblacion correcta pero **no el resultado**: la diferencia no es de muestreo sino del
  motor o de los datos. Cierra definitivamente el replay como sustituto del vivo, y esta
  vez con la causa aislada.
- **B2 FALLA por abajo** -> la cadencia tampoco lo explica; la prediccion de arriba estaba
  mal y hay una causa estructural sin identificar. **No se re-corre**: se pasa a
  diagnosticar cual, con la lista de candidatos ya escrita arriba.

## Costo declarado

La corrida de 200 pares a 5 min tardo ~90 min en 12 cores. A 2 min son 2,5x los escaneos
y mas alertas que procesar: estimado **4 a 6 horas**. Se lanza en background con
`py -3.13 -u` (sin `-u` la salida queda buffereada y no se ve el progreso).

---

# RESULTADOS (2026-08-25, agregados despues de correr)

> Lo de arriba no se toco.

| corrida | alertas/dia | por scan | media 4h | B1 | B2 | B3 |
|---|---|---|---|---|---|---|
| en vivo (2 min) | 26,2 | 0,0364 | **+0,550%** | — | — | — |
| replay 15 min | 5,6 | 0,117 | −0,926% | FALLA | FALLA | FALLA |
| replay 5 min | 9,7 | 0,0337 | −0,157% | FALLA | FALLA | FALLA |
| **replay 2 min (esta)** | **9,1** | **0,0126** | **−0,394%** | FALLA | FALLA | FALLA |

**LA PREDICCION DE ESTE PREREGISTRO FALLO.** Predije ~24 alertas/dia y dieron 9,1 — casi
identico a la corrida de 5 min, con 2,5x mas escaneos. La tasa por scan NO era invariante:
eran dos puntos que coincidian. La media tampoco siguio la tendencia (−0,926 → −0,157 →
**−0,394**, se dio vuelta). Las dos extrapolaciones se rompieron; `PUENTE.md` habia
advertido "no se extrapola" y tenia razon.

## La causa, y esta vez es mecanica y no estadistica

Cayo la rama pre-declarada "B2 FALLA por abajo -> no se re-corre, se diagnostica". El
diagnostico:

**El replay solo evalua velas CERRADAS.** `backtest.py:768` fija `candle_status: "closed"`
y cada scan toma `searchsorted(ts, side="right") - 1`. Un scan al minuto 2 y otro al
minuto 4 de la misma vela de 5m ven **datos identicos**. La informacion se actualiza 288
veces por dia (los cierres de 5m), asi que escanear 720 veces agrega duplicados, no
alertas. **Por eso satura en ~9/dia y por eso 2 min no le gano a 5 min.**

**El vivo tiene un detector que el replay no tiene.** `screener.py:717-762` corre
`EXPLOSION_FORMING` sobre velas de 5m en progreso. En `backtest.py`,
`analyze_forming_lateness` existe pero se llama en la linea 3117 **post-hoc sobre alertas
ya generadas**, para medir lateness; nunca inyecta `_is_forming=True` durante el scan. El
comentario de la linea 1844 lo dice: "futuro".

La composicion lo confirma: en vivo EXPLOSION/BREAKOUT = 792/571 = **1,39**; en el replay
206/255 = **0,81**. El faltante es 3,8x en EXPLOSION contra 2,2x en BREAKOUT —
desproporcionado justo en el tipo que tiene variante forming.

## Veredicto

La brecha **no es de parametros**. No es cadencia (2 min = 5 min), no es universo (200 vs
191-223 reales). Es una **capacidad ausente**: el replay no puede generar la clase de
alerta que mas pesa en la poblacion en vivo.

Corolario que vale para todo el repo: **ningun `--scan-interval-min` va a cerrar esto**, y
cualquier backtest que compare configs esta comparando sobre una poblacion a la que le
falta el grueso de las EXPLOSION reales.

Cerrarla de verdad = cablear `_check_forming_explosion` al loop de simulacion. Las piezas
estan: los klines de 1m ya se descargan (linea 292), la funcion esta escrita (1973), y el
punto de inyeccion existe (1859). Es trabajo de ingenieria con spec clara, no una pregunta
de investigacion.


---

# ADENDA (2026-08-26) — el detector forming SE CABLEO

> Esta seccion la agrega la sesion siguiente. Lo de arriba no se toco.
> Item 4.1 de `HANDOFF_UNLOCKS.md`, pedido por el usuario.

## Que se hizo

`EXPLOSION_FORMING` ahora corre **dentro del loop de simulacion**, no como post-proceso.
Piezas nuevas en `backtest.py`:

- **`build_forming_features()`** — arma la vela de 5m en progreso desde velas de 1m y
  devuelve el mismo dict que `_forming_data` de `screener.py:analyze()`.
- **bloque "EXPLOSION sobre vela FORMING" en `classify()`** — espejo del bloque
  homonimo de `screener.py`, incluidas sus dos asimetrias (no aplica bonus de OBV; el
  bucket sale de los umbrales `*_FORMING`).
- El 1m viaja dentro de `klines[sym]["1m"]`, asi que **no cambio ninguna firma**.
- `--no-forming-1m` apaga el detector (para poder medir el A/B).

El bloque `_is_forming` que ya existia en `classify()` —con el comentario "futuro" en la
linea 1844— quedo conectado sin tocarlo. Era exactamente el enchufe que faltaba.

## Cuatro validaciones

| test | resultado |
|---|---|
| espejo del screener sobre 305 instantes reales | identico salvo **1,4e-11** (ruido de float en BB) |
| **lookahead**: se corrompe TODA la data posterior a `ts` | **243/243 salidas identicas** |
| regresion: detector apagado vs `git HEAD` | **94 vs 94 alertas, identicas** |
| A/B: 200 pares, 1 semana, scan 2 min | solo cambia EXPLOSION; el resto intacto |

## Tres trampas que habrian roto esto en silencio

1. **`ta` calcula Bollinger con `ddof=0`**, no con el default de pandas. Con `ddof=1` la
   diferencia era 7,2e-3 — un sesgo constante del 2,6% (`sqrt(20/19)`) en una sola
   direccion. Nada habria fallado: el detector simplemente no habria sido el del screener.
2. **Con `--scan-interval-min` multiplo de 5 el detector NO PUEDE disparar nunca.** Todos
   los scans caen en el borde de la vela de 5m -> 0 minutos transcurridos. Con el default
   de 15 min, cablear el detector "no cambia nada" y la conclusion seria falsa. Produccion
   corre cada 2,00 min (medido en este mismo documento) y por eso el vivo si ve velas
   vivas. Hay un aviso ruidoso en `main()`.
3. **`_analyze_key()` no incluia `EXPLOSION_FORMING`** y la inyeccion ocurre en el PRIMER
   pase. Un `--compare` entre un cfg con forming y otro sin el habria reutilizado los
   mismos candidatos: comparacion falsa, sin error. Agregado a la clave.

## Resultado (200 pares, 1 semana, scan 2 min)

| | OFF | ON | |
|---|---|---|---|
| EXPLOSION | 51 | **94** | +84% |
| BREAKOUT | 146 | 146 | +0 |
| HOLD / PREBREAK / RIDING | 166 / 27 / 120 | 164 / 27 / 118 | ~0 |
| alertas totales | 510 | 549 | +7,6% |
| **EXPLOSION/BREAKOUT** | **0,35** | **0,64** | **x1,84** |

56 alertas forming (10,2% del total, 8,0/dia) sobre **37 simbolos distintos** — no es
concentracion. `max_gain_24h` medio +16,20% contra +13,39% de las cerradas; mediana
+8,17% contra +7,52%. Mejor, pero poco.

## OJO CON EL DENOMINADOR — la comparacion directa contra el 1,39 NO es valida

Casi reporto "cierra el 28% de la brecha" y **habria sido un numero mal construido.**

La corrida OFF de arriba da ratio **0,35** y **72,9 alertas/dia**. El replay de este
documento dio **0,84** (recalculado de `puente2.json`: 234/280) y **18,7/dia**. No es que
el cableado moviera la base: **`puente2.json` cubre 56 dias (2026-06-21 a 2026-08-15) y la
corrida de arriba cubre 1 semana.** La composicion depende del periodo, asi que comparar
mi 0,64 contra el 1,39 del vivo mezcla denominadores distintos.

Lo unico comparable entre ventanas son **multiplicadores**:

- lo que el cableado entrega: **x1,84** sobre el ratio EXPLOSION/BREAKOUT;
- lo que haria falta para igualar al vivo: 0,84 -> 1,39 = **x1,66**.

O sea el efecto es **del orden de magnitud necesario**, y es la primera evidencia de que
el diagnostico de este documento apuntaba al mecanismo correcto. Pero *no esta demostrado*
que cierre la brecha: para eso hay que correr el A/B **sobre la ventana de 8 semanas que
midio el vivo** y comparar contra 1,39 con el mismo denominador.

**Esa corrida quedo PENDIENTE**: se lanzo (`--weeks 8 --end-date 2026-08-15 --max-pairs
200 --scan-interval-min 2`) y se detuvo a poco de arrancar. Es el unico paso que falta
para adjudicar, y el comando esta escrito aca para no tener que reconstruirlo.

## Lo que este cableado NO arregla

La otra mitad del diagnostico sigue igual: **el replay solo ve velas cerradas**, asi que
dos scans dentro de la misma vela de 5m siguen viendo datos identicos para todo lo que no
sea EXPLOSION forming. La saturacion por cadencia (2 min = 5 min) **no se toca** con esto.
Lo que se agrega son alertas que antes no existian, no resolucion temporal para las demas.
