# PROXIMOS CHEQUEOS — lo unico que hay que hacer

> Una apuesta viva, sin capital. Se prueban solas mientras pasa el tiempo.
> Este archivo es la lista completa. Si no hay nada vencido, no hay nada que hacer.

## Las apuestas y en que regimen se prueban

| | que es | necesita | estado |
|---|---|---|---|
| **4.7** | shortear las senales de extension (EXPLOSION/BREAKOUT) a 4h | semanas **ALCISTAS** | +0,550%, 51 dias de bear, sin confirmar |
| ~~**OI shock**~~ | shortear el shock de OI en cascada | — | **CERRADO 2026-08-22**, ancho y angosto: 46% de semanas, p 0,92 |

Eran complementarias (una esperaba alcista, la otra bajista). Con el OI shock cerrado
**queda una sola, y solo junta evidencia en tramos alcistas** — el de agosto sirve.

---

## Calendario

### 2026-08-24 — daytrader en el tramo alcista, segunda pasada  ⬅ EL PROXIMO

```
py -3.13 archivar_outcomes.py
py -3.13 dt_diag_regimen.py
py -3.13 dt_diag_pareado.py
```

Descriptivo, no hay compuerta ni apuesta: es para ver si el bull rescata al day
trader. **La primera pasada (08-22) dice que no.** Sobre 08-17 -> 08-20 las senales
activas dieron +4,03% a 24h — pero el dardo pareado (misma moneda, promedio de todas
las horas de la misma ventana) dio **+5,41%**: la alerta queda **−1,38pp** abajo y le
gana al dardo solo el 32,4% de las veces. El +4% es beta del mercado, entero. Sin el
top-3 de simbolos queda −0,55%. A 4h sigue plano.

**Por que hay que volver el 08-24 y no alcanza con lo del 08-22:** son 4 dias y 204
alertas activas — menos de una semana, y la unidad de este repo es la SEMANA. Y falta
justo lo mejor: el 08-21 (BTC +24%) y el 08-22 tienen **1.366 alertas sin forward**,
con la tasa disparada a x4,5 (901 alertas en medio dia). El tracker llena una vez por
dia ~04:00 UTC con ~2 dias de rezago, asi que maduran el 08-23/24.

Los dos scripts se cortan solos donde el forward esta completo, y el pareado **se
niega a dar veredicto** con menos de 4 semanas.

### 2026-10-19 — 4.7, chequeo temprano

```
cd fade && py -3.13 evaluar.py
```

Si la media a 4h se dio vuelta, se cierra 4.7 y listo. Refutar es mas rapido que
confirmar: por eso este chequeo vale y los otros son opcionales.

**Vale mas que antes:** el tramo alcista arranco el 2026-08-17 (BTC +20% en 7 dias),
asi que estas 9 semanas SI traen el regimen que faltaba. Ya no es "mas de lo mismo".

### ~~OI shock~~ — CERRADO 2026-08-22

Las dos versiones murieron en los mismos dos criterios, los que miran el TIEMPO:

- **ancha**: los criterios 3 y 7 se habian medido sobre el 45% de los trades
  (`lote.py` descartaba semanas con <20 senales, y esas ganaban 68,68% contra 46,40%).
  Sobre todo: p 0,9990 y 46% de semanas.
- **angosta (cascada)**: preregistrada con umbral derivado, corrida una vez sobre
  `metricas40`. p 0,9210 y 46% de semanas. `banco/PREREGISTRO_CASCADA.md`.

El agregado era de lo mejor del repo (+7,06pp contra el dardo pareado, aguantando
concentracion por simbolo) y aun asi no sirve: vive en pocas semanas gordas.
**No se reabre** — ni otro umbral, ni otro universo, ni otra ventana.

Lo que dejo util: `banco/lote.py` ahora tiene `sem_n_min` parametrizado (commit
`f4ce68d`), y el chequeo de concentracion hay que correrlo tambien en el eje TIEMPO,
no solo por simbolo.

---

## Queda UNA apuesta viva

### 2027-08 — reabrir 4.4 (vender volatilidad), solo si

Correr `opciones/iv_rv.py`. **Unica condicion que lo reabre:** IV/RV sostenido arriba
de ~1,30 **y** el premio arriba del 15% de la prima. Si no, sigue cerrado.

---

## Las reglas que no se tocan

- **Nada de capital** hasta que una aguante su forward test.
- **No aflojar una compuerta despues de ver los numeros.** Es como se fabrica un falso
  positivo, y este repo tiene ~43 hipotesis muertas que lo confirman.
- **No creerle a un p-valor que supone independencia.** La unidad es la SEMANA.
- **"SOBREVIVE" no es "funciona".** Es "no la pude matar en esta ventana".
- El piso de stablecoins (5-10% anual, sin drawdown) es el rival y hoy va ganando.
