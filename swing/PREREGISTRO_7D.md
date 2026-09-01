# PREREGISTRO — el swing medido en SU horizonte (7d)

> Escrito el **2026-09-01**, **antes de correr `dt_vivo.py` con la columna nueva**. El
> backfill del PR #32 todavía está llenando `price_7d`, así que el número no existe
> mientras esto se escribe.
>
> Motivo: `dt_vivo.py --sistema swing` midió los cuatro horizontes cortos y los cuatro
> caen dentro del MDE. Resultados **debajo de la línea**.

---

## 1. Qué se pregunta

El swing opera en **1h/4h/1d/1w** y hasta el PR #32 su filler solo llegaba a **24h** — los
horizontes del daytrader. Lo medido hasta ahora es si la entrada se va en contra enseguida,
**no su tesis**.

> **La pregunta: ¿el swing produce un retorno neto medible a 7 días?**

Un solo horizonte, un solo estadístico, sin barrido.

### Prior: BAJO, y con dos motivos que apuntan en direcciones opuestas

**En contra** — los cuatro horizontes cortos dan `+0,03 / +0,07 / −0,05 / −0,25`, todos
dentro del MDE, y el score no ordena (ρ = +0,0009 a 4h). Nada de eso *anticipa* un efecto a
7d, pero tampoco lo apoya.

**A favor** — el argumento que justifica gastar la tarde: para un sistema de tendencia, el
efecto crece ~lineal con el tiempo y el ruido ~√t, así que la relación señal/ruido **mejora**
con el horizonte. Y el costo es fijo por trade: a 7d los 0,20 % pesan 7× menos que a 24h.
Es exactamente la simetría opuesta a la de la corrida 13, donde alargar el horizonte era un
empate porque el costo caía a la par de la precisión.

**Y el que ordena todo:** la asimetría MFE/|MAE| del swing es **1,12** (favorable), contra
**0,91** del daytrader. Es la única cifra de toda la sesión que apunta a favor de algo.

### ⚠️ FUGA DECLARADA — lo que ya vi

Esta hipótesis **no es ciega** y hay que decirlo, como en `PREREGISTRO_TRANSVERSAL` §7 y
`PREREGISTRO_SALIDA` §1. Antes de escribir esto ya vi, del mismo conjunto de alertas:

1. Los cuatro horizontes cortos, su MDE y sus semanas positivas.
2. MFE +1,21 %, MAE −1,08 %, asimetría 1,12.
3. Que el score no ordena y que los buckets no son monótonos.

Consecuencias asumidas:

- **Agregar un quinto horizonte después de ver que los otros cuatro no dan es la maniobra
  que el banco prohíbe.** Lo que la salva —y solo eso— es que 7d **no es un horizonte más
  de un barrido**: es el horizonte declarado del sistema (`swing/screener.py` §1: "swing
  opera en timeframes altos 1h/4h/1d/1w"), y estaba ausente por un bug de instrumentación,
  no por elección.
- Por eso **7d es el único horizonte que se agrega, nunca se prueban 2d ni 14d**, y el
  umbral se fija abajo antes de mirar.
- Las mismas alertas ya fueron miradas a 24h. Esto **no es una muestra nueva**: es un
  estadístico nuevo sobre la misma muestra. Si da positivo, **no alcanza** — pide
  confirmación hacia adelante, y por eso §4 fija una fecha en lugar de correrlo hoy.

---

## 2. El dato

`screener_outcomes` ya tenía `price_7d`, `max_high_7d`, `min_low_7d` y `complete_7d` — 3.483
filas, todas en `NULL`. El PR #32 escribió el código que las llena y **rellena hacia atrás**,
así que el registro a 7d no arranca en cero: cubre desde el **2026-06-03**.

**Lo que el backfill NO arregla:** las alertas son las mismas que ya miré. El backfill da
profundidad de horizonte, **no independencia**.

---

## 3. La construcción, fijada acá

**Universo:** idéntico al de `dt_vivo.py --sistema swing`. `signal_type` en `BREAKOUT`,
`PREBREAK`, `COILING`, deduplicado a una por (par, señal) cada 24h. No se toca el filtro.

**El estadístico:**

```
r_7d = (price_7d / entry_price - 1) * 100 - 0,20        [% neto, long only]
```

### ⚠️ El bloque es de 14 DÍAS, no de una semana

Y este es el punto de método que hace falta pensar antes y no después. En todo el repo el
bloque es la **semana**, porque el resultado se resuelve bien dentro de ella. **A 7d ya no:**
una alerta del lunes se resuelve el lunes siguiente, o sea que su resultado cae **dentro de
la ventana del bloque que sigue**. Bloques semanales solapados hacen que el bootstrap
subestime la varianza y **regale significancia**.

> **El bloque es de 14 días** — el doble del horizonte, que es la regla estándar. Cada
> bloque pesa uno y el p que decide es el de bloques, nunca el binomial sobre 1.900 alertas.

**Y eso es lo que fija la fecha, porque cambia el n disponible:**

| | |
|---|---|
| registro al 2026-09-01 | 90 días = **6,4 bloques** |
| mínimo del repo | **8 bloques** (`SEM_MIN`) |
| n objetivo | **10 bloques** = 140 días de registro |
| → se mira el | **2026-10-21** |

Con la retención en 550 días (PR #32) el registro llega entero a esa fecha. Verificado, que
es la regla de `HANDOFF_FUENTES_NUEVAS.md` §6.

**La dirección se declara acá: se espera POSITIVA.** El swing es long-only. Si sale negativa
y supera el MDE, eso es un resultado —el sistema destruye valor a su propio horizonte— y
**no** se reinterpreta como una señal short: sería otra hipótesis.

---

## 4. LAS COMPUERTAS

### (C) POTENCIA — primero, antes de mirar `r_7d`

```
MDE = 2,8 · sigma_por_bloque(r_7d) / sqrt(n_bloques)        [% por trade, neto]
```

**La cuenta a priori, escrita antes de correr.** A 24h se midió `σ_semanal = 2,67`. Con
`σ ~ √t`, a 7d sería `2,67 · √7 ≈ 7,1` por semana, y agregando a bloques de 14 días
`≈ 7,1/√2 ≈ 5,0`. Entonces:

```
MDE ≈ 2,8 · 5,0 / √10 ≈ 4,4 % por trade
```

> **Hace falta ~4,4 % neto por trade a 7 días.** Es un umbral alto pero no absurdo para un
> swing que apunta a 10-15 % por operación.

**Si el MDE medido supera el 6 %, se declara "no se pudo medir"** y no se concluye nada. Ese
margen sobre los 4,4 tolera que la σ real sea peor que la extrapolación —la corrida 14
predijo 10,8 %/año y midió 28,4 justamente por confiar en una cuenta así— sin dejar la
compuerta abierta a cualquier número.

Y hay que **decir cuál de las dos falló**: por *n* se reabre esperando (la retención ahora lo
permite), por *σ* no.

### Las compuertas de la medición, si (C) pasa

1. **Bootstrap de bloques de 14 días.** Cada bloque pesa uno.
2. **Sin el mejor bloque** y **sin el mejor par**, para concentración.
3. **≥ 60 % de bloques positivos.**
4. El signo tiene que sobrevivir **sin el mejor par**.

---

## 5. Lo que este preregistro NO autoriza

- **Mirar `price_7d` antes del 2026-10-21.** El backfill puede terminar en horas; la fecha
  no depende de eso, depende de tener 10 bloques.
- **Probar otro horizonte.** 7d es el del sistema; 2d, 3d y 14d no se corren.
- **Volver a bloques semanales** si con bloques de 14 días no alcanza el n. Ese fue el
  motivo entero de §3.
- **Aflojar el umbral de 6 %** ni el mínimo de 10 bloques.
- **Reinterpretar un resultado negativo** como señal short.
- **Barrer el bucket o el `signal_type`** buscando el subconjunto que dé. El universo está
  fijado en §3.

---

## 6. La expectativa honesta

**Lo más probable es "no se pudo medir", y por σ.** La extrapolación de 4,4 % ya es un
umbral alto, y basta con que la σ real sea 1,4× la estimada para pasarse de 6 %.

Si eso pasa, el resultado es que **el swing no es medible a su propio horizonte con la
historia que existe** — pero ahora, con la retención en 550 días y el filler arreglado, sí
se acumula. Sería un "esperá", no un "no".

Y si `r_7d` sale positivo y supera el MDE, **sigue sin ser un negocio**: es la primera
señal en toda la sesión que apunta para arriba, sobre una muestra que ya fue mirada (§1), y
lo que corresponde es un forward test con fecha, no aumentar el tamaño.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

*(vacío hasta el 2026-10-21)*
