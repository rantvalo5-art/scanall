# 4.7 — PUENTE: validar el replay contra los 51 dias ya medidos

> **Escrito ANTES de correr nada.** Fecha: 2026-08-19. Todo numero que aparece aca
> como referencia ya existia (`project-fadear-extension-vivo`, `HANDOFF_CIERRE.md`).
> Si algo de este archivo se edita despues de ver un resultado, el experimento no vale.

## Por que

`fade/evaluar.py` lee `daytrader_outcomes` = alertas REALES del bot. Por eso la ventana
son 51 dias: es lo que lleva la tabla desde la separacion del 2026-06-25. El unico agujero
que le queda a 4.7 es que esos 51 dias son **un solo regimen bear**, y el handoff manda
esperar hasta 2026-10-19 / 2026-12-21 / 2027-12-13.

Pero `backtest.py` replaya historia por el mismo `classify()`, y `calculate_outcomes()`
emite los mismos campos que la tabla (`price_15m` / `price_4h` / `price_24h`,
`entry_price`, `symbol`, `signal_type`, `alerted_at`). Si el replay reproduce el numero
en vivo sobre la ventana conocida, **se puede ir a buscar el tramo alcista a la historia
en vez de esperarlo 14 meses**.

Ese "si" es todo el experimento. Este archivo lo define antes de mirar.

## Lo que hay que reproducir (medido en vivo, ya publicado)

Condicion realista = fill `price_15m`, solo simbolos con perp, costo 0,40%:

| | 4h | 24h |
|---|---|---|
| media | **+0,550%** | +1,435% |
| IC95 semanal | **[+0,14%, +2,04%]** (p=0,008) | cruza cero |
| compuertas | 5/5 OK | (e) FALLA |

Poblacion: **1.357 alertas** de extension, 2026-06-26 -> 2026-08-16 (51 dias) = **26,6/dia**.

## Compuertas del puente — decididas ahora

- **B1 (primaria).** La media a 4h del replay, en condicion realista, tiene que ser > 0
  **y caer dentro del IC95 en vivo [+0,14%, +2,04%]**.
- **B2 (composicion).** Tasa de alertas de extension entre **0,5x y 2x** la de vivo
  (13 a 53 por dia), con EXPLOSION y BREAKOUT ambas presentes.
- **B3 (concentracion).** Las compuertas (a) media, (b) sin top-3 y (d) sin el peor
  simbolo tienen que dar OK tambien en el replay. Son las que no dependen del n.

## Que pasa con cada resultado

- **B1 FALLA** -> **STOP.** El replay no puede pararse en lugar de las alertas en vivo.
  4.7 vuelve al calendario de octubre y este camino se cierra. **No se retoca nada
  para que pase.**
- **B1 OK, B2/B3 fallan** -> parcial. El resultado alcista se reporta como direccional,
  no como "cruzo las compuertas".
- **B1, B2, B3 OK** -> se habilita el paso 2.

## Una sola corrida

Config `config.json` tal como esta deployada, `--max-pairs 200`,
`--scan-interval-min 15` (default del backtest), `--end-date 2026-08-16 --weeks 8`,
alertas filtradas a `>= 2026-06-26` para que la ventana sea la misma.

**Unica alternativa admitida, y se declara ahora**: el bot en vivo no corre cada 15
minutos (el cron del workflow esta comentado; lo dispara cron-job.org, y la memoria
`daytrader-next-session-plan` dice "cada 2 min"). Si **B2 falla por abajo**, se permite
**UNA** re-corrida a `--scan-interval-min 5` y esa es la definitiva. Declararlo ahora es
legitimo; declararlo despues de ver el numero, no.

## Paso 2, si el puente pasa — tambien decidido ahora

Mismo pipeline sobre dos tramos alcistas reales:

- **W1**: 2024-02-01 -> 2024-03-31 (BTC ~43k -> ~71k)
- **W2**: 2024-10-15 -> 2024-12-15 (BTC ~67k -> ~101k)

Las mismas cinco compuertas, mas el **control pareado mismo-simbolo/hora-al-azar**, que
segun `project-swing-entrada-breakout` es **inmune al sesgo de universo** — y el sesgo
de universo es el problema grande de replayar 2024 con los pares listados hoy.

**Regla de parada: si la media a 4h agrupando las semanas alcistas es <= 0, 4.7 muere.**

---

# RESULTADOS (2026-08-19, agregados despues de correr)

> Lo de arriba no se toco. Esto es lo que dio.

| corrida | alertas/dia | media 4h | media 24h | B1 | B2 | B3 |
|---|---|---|---|---|---|---|
| en vivo (referencia) | 26,6 | **+0,550%** | +1,435% | — | — | — |
| replay `--scan-interval-min 15` | 5,6 | **−0,926%** | −0,335% | FALLA | FALLA | FALLA |
| replay `--scan-interval-min 5` (la declarada) | 9,7 | **−0,157%** | +0,235% | FALLA | FALLA | FALLA |

**VEREDICTO: B1 FALLA. STOP.** El replay no reproduce la medicion en vivo. El paso 2
(ventanas alcistas W1/W2) **no se corre**, y 4.7 vuelve al calendario de octubre.

## Lo que se aprendio igual

**El replay no genera la misma poblacion.** En vivo entran 26,6 alertas de extension por
dia; el replay hace 5,6 a 15 min y 9,7 a 5 min. Es entre un quinto y un tercio.

Al acercar la cadencia del replay a la de produccion, la media a 4h se movio
−0,926% → −0,157%, o sea **hacia** el numero en vivo. Es consistente con que buena parte
de la brecha sea de poblacion y no del efecto. **No se extrapola** — anotarlo es honesto,
usarlo como evidencia seria justo el error que este archivo existe para prevenir.

**Defecto de especificacion encontrado DESPUES de las dos corridas:** produccion tiene
`general.TOP_N = 9999` (escanea todo par USDT sobre 600k de volumen); el replay corrio con
`--max-pairs 200`. No son el mismo universo, y eso explica parte del faltante de alertas.
Se anota como defecto del preregistro, **no** como permiso para una tercera corrida: la
regla admitia una sola alternativa y ya se uso. Cualquier reintento necesita un preregistro
NUEVO escrito antes de mirar.

## Lo que este resultado NO dice

**No refuta 4.7.** La media negativa del replay se midio sobre una poblacion distinta —un
tercio del tamano y con otro universo—, asi que no es una medicion de las alertas reales.
4.7 sigue exactamente donde estaba: VIVO a 4h, sin confirmar, esperando octubre.

Lo que si murio es **el atajo**: no se puede ir a buscar el tramo alcista a la historia
con este instrumento. El costo de averiguarlo fueron ~2 horas de CPU, y evito correr las
ventanas de 2024 sobre un motor que no reproduce la respuesta conocida — lo que habria
dado un numero con cara de evidencia que no lo era.
