# PREREGISTRO — una regla de salida para el daytrader, y una sola

> Escrito el **2026-11-28**, **antes de que el dato exista**. La columna que decide esto
> (`regla_ret_4h`) **todavía no se guarda**, y el código que la calcula va en un PR
> **posterior** a este documento, para que quede a la vista que la regla se fijó antes.
>
> Motivo: `PREREGISTRO_SALIDA.md` PASÓ (C) el 2026-11-27: P = 0,624, 11 de 11 semanas
> por encima de 0,55. Ese resultado no es un negocio. Su §6 dice que es **permiso para
> escribir esto**. Resultados **debajo de la línea**.

---

## 1. Qué se pregunta

> ### ¿Una salida con objetivo y stop fijos da un retorno NETO > 0 a 4h?

Es la pregunta que el orden (P) no contesta. P cuenta **quién llega primero** entre el
máximo y el mínimo de 4h, pero no **cuánto vale** el que llega primero. Este preregistro
mide directamente lo que una regla cobraría.

### ⚠️ FUGA DECLARADA — lo que ya vi

Nada de esto es ciego, y se dice antes:

1. **P = 0,624** (W37–W47, 1.745 entradas), con todas las semanas entre 0,587 y 0,694.
2. **MFE mediano +3,31 %, MAE mediano −3,62 %, cierre a 4h −1,63 %** (`dt_vivo.py`).
3. **La deriva**: la media neta por horizonte es −0,07 / −0,49 / −1,14 / −1,73 % a
   15m / 1h / 4h / 24h. A 4h, 0 % de las semanas en positivo.

Consecuencia: los números de la regla (§2) salen de una **aritmética escrita**, no de
mirar qué combinación hubiera dado mejor. Y **no se rellena hacia atrás** (§5): el dato
viejo es el que ya vi.

### Prior: BAJO, pero no nulo

- Con una deriva de −1,14 % a 4h, cualquier regla que se quede hasta el final pierde. El
  teorema de parada opcional dice lo mismo en general: sobre un proceso con deriva
  negativa uniforme, **ninguna regla de parada da expectativa positiva**.
- La única forma de que esta regla gane es que la deriva **no sea uniforme**: que haya un
  empujón temprano a favor y la caída venga después. Eso es justo lo que P > 0,5 permite,
  pero no lo prueba (ver §2).

---

## 2. El problema primero: "el MFE primero" puede ser la entrada misma

Si el precio arranca a bajar apenas se entra, el máximo de las 4h **es la vela de
entrada** y el mínimo cae al final. Eso suma a P y no vale nada: no hay favorable que
cobrar. Es la explicación alternativa que `PREREGISTRO_SALIDA.md` §1 ya había declarado.

**La regla lo resuelve por construcción, sin un filtro aparte:**

- **Un objetivo a distancia fija** no se toca si el "máximo" es la entrada misma: un MFE de
  ~0 % no llega a +1 %. Ese caso cae en el stop o en el cierre a 4h, y cuenta como lo
  que es.
- **La vela de la entrada se excluye.** El camino arranca en la primera vela de 1m que
  **abre después** de `alerted_at`. Su high puede ser anterior al alerta, y contarlo
  regalaría justo el caso que hay que separar.

### La regla, fijada acá

```
entrada  = entry_price, en alerted_at
objetivo = entrada × (1 + 1,0 %)
stop     = entrada × (1 − 1,0 %)
camino   = velas de 1m que abren en (alerted_at, alerted_at + 4h]

para cada vela, en orden:
  si toca el stop                 -> sale en −1,0 %      (fin = "stop")
  si no, si toca el objetivo      -> sale en +1,0 %      (fin = "obj")
si no tocó ninguno                -> sale al close de la última vela  (fin = "4h")

r = retorno bruto de la salida − 0,20 %   (costo ida y vuelta)
```

**Por qué 1,0 % y simétrico, dicho antes:**

- **5× el costo.** Un objetivo más cerca cobra poco más que el costo, y queda dentro del
  ruido de 1 minuto. Uno más lejos se acerca a la mediana del MFE (+3,31 %): la mitad de
  los trades no lo toca, y la regla pasa a medir de nuevo el cierre a 4h, que ya se sabe
  negativo.
- **Simétrico** porque MFE y MAE medianos son casi iguales (asimetría 0,91). Un stop más
  ancho que el objetivo sería apostar a la dirección, y la dirección está cerrada en todo
  lo probado.
- **No es una grilla:** es un solo par de números. No se prueban 0,5 % ni 1,5 % (§5).

**Si en la misma vela tocan los dos, cuenta como stop.** En 1m no se sabe el orden
dentro de la vela, y suponer el stop es lo conservador.

**Supuestos optimistas, dichos ahora** (la corrida 14 midió un 3× peor de lo predicho por
no escribirlos): (a) el objetivo y el stop se llenan **exactos**, sin deslizamiento; (b)
el costo de 0,20 % cubre comisiones de ida y vuelta, **no** el deslizamiento de un stop en
una vela que salta. Si esto pasa, la ejecución real necesita su propia verificación antes
de poner plata.

---

## 3. El dato, hacia adelante

**Universo:** el mismo de `dt_salida.py`: `signal_type` en BREAKOUT / EXPLOSION /
PREBREAK, deduplicado con `dt_vivo.deduplicar` (una por par+señal cada 24h). No se cambia
el filtro.

**Instrumentación** (PR aparte, después de mergear este documento): `update_outcomes.py` ya
baja las velas de 1m de las primeras horas (líneas ~176-190). Se agregan dos columnas,
calculadas **solo cuando ya pasaron las 4h completas** (antes de eso el camino está
incompleto y el valor sería otro):

```sql
alter table daytrader_outcomes
  add column regla_ret_4h double precision,   -- retorno BRUTO de la regla, en %
  add column regla_fin_4h text;               -- 'obj' | 'stop' | '4h'
```

Van en `CAMPOS_OPCIONALES` (`update_outcomes.py:217`), así el PATCH degrada solo si la
columna todavía no existe. Pero el reloj de §4 **no arranca** hasta que el SQL corra y una
fila nueva tenga el valor.

**La primera semana no cuenta**, igual que en `PREREGISTRO_SALIDA.md`: el conteo arranca
el primer lunes completo después de que la columna empieza a llenarse.

---

## 4. LAS COMPUERTAS

La unidad independiente es la **semana ISO**: se calcula la media de `r` dentro de cada
semana y cada semana pesa uno. Nunca se usa el t sobre trades sueltos.

### (P) POTENCIA — antes de mirar el signo

```
MDE = 2,8 · sigma_semanal(media de r) / sqrt(n_semanas)
```

> **Si el MDE supera 0,20 % (el costo), se declara "no se pudo medir"** y se dice si
> falló por *n* (se reabre esperando) o por *σ* (no se reabre).

**La cuenta a priori:** el 4h crudo de `dt_vivo.py` dio un MDE de 0,59 en 11 semanas →
σ_semanal ≈ 0,59·√11 / 2,8 ≈ 0,70 %. La regla acota cada trade a ±1 % salvo los que llegan
al cierre, así que se espera **≈ 1/3 de eso, σ ≈ 0,25 %**. Entonces
`n = (2,8 · 0,25 / 0,20)² ≈ 12` semanas.

### La fecha

Si la columna arranca la semana del 2026-11-30, la primera semana completa es la
**2026-W50 (lunes 2026-12-07)**, y 12 semanas después cae el **2027-03-01**. Si la
instrumentación se mergea más tarde, la fecha corre lo mismo: **primer lunes completo +
12 semanas**, y se escribe acá con el día exacto al arrancar.

### (C) EL EFECTO — recién después de (P)

Las cuatro tienen que pasar:

| compuerta | umbral |
|---|---|
| 1. media semanal de `r` (neta) | **> 0**, con p de bloques < 0,025 (una cola, 5.000 reps) |
| 2. semanas con media > 0 | **≥ 60 %** |
| 3. sin la mejor semana | media **> 0** |
| 4. sin el mejor par | media **> 0** |

Se reporta, **descriptivo y sin compuerta**: la fracción de `obj` / `stop` / `4h`, y la
media de `r` en cada grupo.

---

## 5. Lo que este preregistro NO autoriza

- **Barrer el objetivo o el stop** (0,5 %, 1,5 %, asimétricos, en ATR…). Es un par de
  números. Si no da, la familia "salida con objetivo fijo" se cierra; no se busca otro par.
- **Barrer la ventana.** 4h, como en `PREREGISTRO_SALIDA.md`.
- **Rellenar hacia atrás.** Binance sirve las velas de 1m viejas, así que se podría. Pero
  ese dato es el mismo sobre el que ya vi P y las medianas (§1): un relleno convertiría un
  test ciego en uno contaminado.
- **Mirar `regla_ret_4h` antes de la fecha**, ni para "ver si anda". Para verificar que la
  instrumentación funciona alcanza con contar filas no nulas, sin mirar valores.
- **Reinterpretar un resultado negativo** como "entonces al revés" (short, o stop más
  ancho). Sería otra hipótesis.

---

## 6. La expectativa honesta

**Lo más probable es un cierre.** La deriva a 4h es fuerte (−1,14 %) y el teorema de parada
opcional juega en contra. P = 0,624 es la única razón para mirar, y puede salir entero del
caso "la entrada es el máximo", que esta regla cuenta como pérdida.

Si cierra, el daytrader queda medido de punta a punta: **agarra volatilidad, no dirección,
y ni la salida la rescata.** Es la respuesta que hace falta para decidir si sigue en
producción.

Si pasa, tampoco es un negocio todavía: los supuestos optimistas de §2 piden un paper
trading con fills reales antes de cualquier plata.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

*(vacío hasta la fecha de §4)*
