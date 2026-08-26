# PREREGISTRO — shock de OI + regimen bajista, en corto

> **Escrito ANTES de tocar los datos de prueba.** 2026-08-21.
> Si algo de este archivo cambia despues de ver un resultado, el experimento no vale.

## De donde salio

Corrida exploratoria del 2026-08-21 (`banco/regimen.log`): al partir el lote de 60
celdas por regimen de BTC, **una sola sobrevivio las seis compuertas**:

| | |
|---|---|
| celda | `oi shock -2z`, lado CORTO, regimen BAJISTA |
| n | 2.172 |
| win rate | 57,37% (umbral 51,25%) |
| vs linea base pareada | +4,9 pp |
| sin top-3 simbolos | +4,8 pp |
| semanas arriba | **79%** |
| p por bloques semanales | 0,0000 |

**Esto NO es un hallazgo todavia.** Salio de partir en cuatro un lote que ya se habia
medido entero: 60 celdas -> 240. Que una se vea bien es esperable por azar. Este
archivo existe para decidir si es real ANTES de mirar.

## La regla, exacta y ejecutable

- **Universo**: pares USDT spot que tengan perpetuo en Binance Futures.
- **Regimen** (se sabe en el momento, no en retrospectiva):
  `close_1h(BTCUSDT) < EMA168(close_1h(BTCUSDT))` — BTC abajo de su promedio semanal.
- **Senal**: `oi_z < -2`, donde `oi_z` es el z-score sobre ventana de 168h del cambio
  horario del open interest en USD (`sum_open_interest_value`).
- **Accion**: **SHORT** al cierre de la vela en que se cumplen las dos condiciones.
- **Salida**: triple barrera simetrica +8% / -8%, maximo 7 dias.
- **Costo**: 0,20% ida y vuelta (`COSTO_PCT` del banco).

## Datos de prueba — no usados hasta ahora

Los simbolos **41 a 100** del ranking de volumen, **excluyendo los 40** del universo
`metricas40` que produjo el hallazgo. Misma ventana 2021-08-01 -> 2026-08-01.

## Criterios de aprobacion — los SIETE, todos obligatorios

1. `n >= 200`
2. win rate **>** umbral (51,25%)
3. **p por bloques semanales <= 0,10** (es UNA hipotesis preespecificada, no un lote:
   la multiplicidad ya se pago en la fase de descubrimiento)
4. le gana a la **linea base pareada del mismo simbolo** (`vs_par > 0`)
5. margen **sin los 3 simbolos** que mas aportan `> 0`
6. margen **sin el mejor simbolo solo** `> 0`
7. **>= 60% de las semanas** arriba del umbral

**Si falla CUALQUIERA de los siete, se cierra.** No se re-corre con otro universo, otra
ventana, otro umbral de z ni otro horizonte. Una sola corrida.

## Limitacion declarada de antemano

Esto es out-of-sample en la **seccion transversal** (monedas nuevas), **no en el
tiempo**: son las mismas semanas. Y las cascadas de desapalancamiento son de mercado
entero — cuando pasan, las viven todas las monedas el mismo dia. Asi que si el efecto
fuera un artefacto de unas pocas semanas particulares, **este test no lo detectaria**.

Se considero usar 2020-09 -> 2021-08 como semanas nuevas y se **descarto antes de
correr**: ese tramo es el bull de 2020-21, donde la condicion "BTC abajo de su promedio
semanal" casi no ocurre, y ademas pocos alts tenian perpetuo. La muestra seria minima.

Consecuencia: aprobar aca **no alcanza para poner capital**. Alcanza para decidir si
merece un forward test en semanas nuevas de verdad.

---

# ANEXO — auditoria del 2026-08-22. Los criterios 3 y 7 no midieron lo que dicen

> Escrito DESPUES de la aprobacion, y por eso este anexo no afloja nada: aprieta.
> Aflojar una compuerta despues de ver numeros fabrica falsos positivos. Auditar si
> una compuerta fue inadvertidamente indulgente es lo contrario.

## El defecto

`lote.py:157` (dentro de `_p_bloques`) y `lote.py:232` (la compuerta semanal) descartan
las semanas con **menos de 20 senales**:

    wr = np.array([... for _, g in S.groupby("semana") if len(g) >= 20])   # :157
    sem = sem[sem["n"] >= 20]                                              # :232

Es una constante generica del harness, no una decision de esta hipotesis. Y aca **no es
neutral**, porque la actividad correlaciona fuerte con el resultado:

| | semanas | trades | win rate |
|---|---|---|---|
| semanas que CUENTAN (n>=20)   |  36 (18%) | 1.009 (45%) | **68,68%** |
| semanas DESCARTADAS (n<20)    | 169 (82%) | 1.237 (55%) | **46,40%** |
| umbral necesario              |           |             | 51,13% |

Los criterios 3 y 7 —los dos que certifican **consistencia en el tiempo**— se midieron
sobre el 45% de los trades. Justo el 45% que gana.

## El veredicto depende entero de ese 20

| filtro | semanas | %trades | crit.7 (%sem) | crit.3 (p) | |
|---|---|---|---|---|---|
| n>=1  | 205 | 100% | 46% | **0,9990** | FALLA |
| n>=3  | 174 |  98% | 51% | 0,9353 | FALLA |
| n>=5  | 145 |  94% | 55% | 0,7045 | FALLA |
| n>=10 |  81 |  74% | 72% | 0,0047 | pasa |
| n>=20 |  36 |  45% | 86% | 0,0000 | pasa |
| n>=30 |  11 |  18% | 100% | 0,0000 | pasa |

No esta al filo: sobre la muestra entera el p da **0,9990**, o sea lo contrario. La
constante heredada cayo justo del lado que aprueba.

## Lo que SI aguanta

Concentracion en el eje TIEMPO — el chequeo que este repo siempre corrio por simbolo y
nunca por semana:

| | win rate | margen |
|---|---|---|
| todo                          | 56,41% | +5,28pp |
| sin las top-3 semanas         | 54,63% | +3,50pp |
| sin las top-10 semanas (/205) | 51,72% | +0,59pp |
| sin los top-3 simbolos        | 55,37% | +4,24pp |

No se muere de un punado de semanas: las 5 mejores son el 8% de los trades. Decae pero
sobrevive, apenas.

## Consecuencia

La regla **ancha** —"shortear cuando el OI colapsa y BTC esta bajo su EMA semanal"— no
queda certificada: si la operaras entera tomarias los 2.246 trades, y el 55% de ellos
vienen de semanas que pierden. El 56,41% agregado lo sostienen las semanas de cascada.

Lo que los datos sostienen es una regla **mas angosta**, que el filtro del harness
introdujo sin que nadie la escribiera. Esa version angosta se preregistra aparte, en
`PREREGISTRO_CASCADA.md`, porque fue sugerida por los datos y necesita su propia prueba.

**Este preregistro (el ancho) queda CERRADO.** Reproducir con:

    py -3.13 auditar_umbral20.py
    py -3.13 conc_temporal.py
