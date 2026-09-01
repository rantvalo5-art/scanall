# PREREGISTRO — el timing de la salida del daytrader

> Escrito el **2026-09-01**, **antes de que el dato exista**. Es la situación más limpia
> posible para un preregistro: las dos columnas que deciden esto **todavía no se están
> guardando**, así que no hay forma de haber espiado.
>
> Motivo: la medición de `dt_vivo.py` (2026-09-01). Código de la medición futura:
> `dt_salida.py`, que **no existe todavía y no se escribe hasta que se cumpla §5**.
> Resultados **debajo de la línea**.

---

## 1. Qué se pregunta

`dt_vivo.py` midió el daytrader en producción sobre **1.526 entradas reales en 11
semanas** (no las 14.736 filas de la tabla: FADING es una salida y RIDING/HOLD repiten
sobre la misma posición). El resultado:

| horizonte | media semanal, neta | semanas > 0 | MDE | |
|---|---|---|---|---|
| 15m | −0,07 % | 27 % | 0,32 | dentro del MDE |
| 1h | −0,49 % | 18 % | 0,49 | dentro del MDE |
| **4h** | **−1,14 %** | **0 %** | 0,59 | **negativo, ~2× el MDE** |
| 24h | −1,73 % | 10 % | 1,31 | negativo, supera el MDE |

Y el recorrido a 4h: **MFE mediano +3,31 %**, **MAE mediano −3,62 %**, cierre **−1,63 %**.

> **El movimiento existe** —hay 3,31 % disponible contra un costo de 0,20 %— **pero la
> asimetría es 0,91**: la caída disponible es casi igual de grande que la subida. La
> alerta agarra volatilidad, no dirección.

Queda **una** pregunta abierta que el dato actual no puede contestar, y es esta:

> ### ¿El MFE llega ANTES que el MAE?
>
> Si el máximo favorable ocurre sistemáticamente antes que el adverso, hay algo que una
> regla de salida podría cobrar. Si el orden es una moneda, **ninguna regla de salida
> puede extraer nada**, y la dirección se cierra entera.

Es la única palanca que queda, y es **parameter-free**: es un número, no una estrategia.

### Prior: BAJO

- Los cuatro horizontes son **≤ 0**. El mejor caso visible es *empatar* a 15m
  (−0,07 % ± 0,32), no ganar. Una salida más temprana reduce la pérdida; no está dicho
  que la dé vuelta.
- La asimetría 0,91 dice que subida y bajada disponibles son casi iguales. Para que el
  orden esté sesgado a favor teniendo magnitudes simétricas haría falta un mecanismo, y
  no tengo uno escrito.
- Es "forma, no expectativa", que `HANDOFF_CIERRE.md` ya documentó **cinco veces** en el
  banco: una señal que concentra movimiento sin mover la expectativa.

### ⚠️ FUGA DECLARADA — lo que ya vi

Esta hipótesis **no es ciega**, y hay que decirlo como lo hace `PREREGISTRO_TRANSVERSAL`
§7. Antes de escribir esto ya vi:

1. **La caída monótona por horizonte** (−0,07 → −0,49 → −1,14 → −1,73). De ahí sale la
   sospecha de que lo favorable está adelante: es exactamente lo que motivó la pregunta.
2. **MFE +3,31 / MAE −3,62 / asimetría 0,91.**
3. Que el score **no ordena** (ρ = −0,035 a 4h) y que los buckets están **invertidos**
   (BEST −1,40 %, WATCH −0,19 %).

Consecuencias asumidas:

- La pregunta de §1 es **confirmatoria, no exploratoria**. Nació de mirar el 1 y el 2.
- **Pero el estadístico que decide es distinto del que vi.** Yo vi *magnitudes* (MFE,
  MAE, retorno por horizonte); acá se mide un *orden temporal*, que no está en ninguna
  columna de la tabla ni se puede derivar de las que hay. Que la media decaiga con el
  horizonte **no implica** que el MFE venga primero: también es compatible con que la
  alerta entre cerca de un techo y el precio simplemente baje.
- Por eso el umbral de §4 se fija **ahora**, con la aritmética escrita, y no después.

---

## 2. El dato no existe. Qué hay que instrumentar

`update_outcomes.py:176-184` ya baja **velas de 1 minuto** y calcula
`max_high_4h`/`min_low_4h` con un `max()`/`min()`. Falta guardar **cuándo** ocurre cada
uno. Son dos columnas nuevas y ~6 líneas:

```python
if klines_4h:
    kh = max(klines_4h, key=lambda k: float(k[2]))
    kl = min(klines_4h, key=lambda k: float(k[3]))
    update["max_high_4h"] = float(kh[2])
    update["min_low_4h"]  = float(kl[3])
    update["mfe_min_4h"]  = int((int(kh[0]) - alerted_ms) / 60000)   # minutos
    update["mae_min_4h"]  = int((int(kl[0]) - alerted_ms) / 60000)
```

Resolución: **1 minuto**, de sobra. No hay request extra: las velas ya están en memoria.

**Ventana: 4h y solo 4h.** Es donde ya se registran MFE/MAE y donde el efecto medido es
más claro. No se prueban otras ventanas (§5).

> **n = 0 hoy.** Todo lo que hay en la tabla se descarta para este test: las 14.736 filas
> existentes **no tienen** estas columnas y no se pueden rellenar hacia atrás sin volver a
> bajar 1.526 × 240 velas de 1m. **Se mide solo hacia adelante.** Eso es lo que hace que
> el preregistro sea genuinamente ciego.

### Las columnas hay que crearlas ANTES, y el código lo dice solo

PostgREST rechaza el PATCH **entero** con un 400 si una columna no existe, y
`patch_outcome` se come la excepción en su `except`. O sea que sin este paso **todas las
filas dejarían de actualizarse mientras el workflow sigue verde en Actions** — un modo de
falla silencioso peor que el bug que esto viene a arreglar.

Por eso el código **degrada solo**: si el PATCH falla, reintenta sin las claves nuevas,
guarda el resto y avisa una vez. Pero el reloj de §4 **no arranca** hasta correr:

```sql
alter table daytrader_outcomes
  add column mfe_min_4h int,
  add column mae_min_4h int;
```

### La primera semana se descarta

La instrumentación agarra también filas alertadas **antes** de que esto se escribiera (las
que todavía no estaban completas). Son horas, no días, y el estadístico de §3 no estaba en
ninguna columna que yo haya mirado — pero para no discutirlo después: **la semana
calendaria en la que se instrumenta no cuenta.** El conteo de las 12 semanas empieza en el
primer lunes completo posterior.

**Y las columnas muertas, que conviene arreglar de paso** (no son parte de este test):
`price_7d`, `max_high_7d`, `min_low_7d`, `funding_rate` y `open_interest` están **100 %
vacías** en las 14.736 filas. O se llenan o se borran, pero no deberían quedar así.

---

## 3. La construcción, fijada acá

**Universo:** las mismas entradas que usó `dt_vivo.py` — `signal_type` en
`BREAKOUT`, `EXPLOSION`, `PREBREAK`, deduplicadas a una por (par, señal) cada 24h. No se
cambia el filtro.

**El estadístico, uno solo:**

```
P = fraccion de entradas con  mfe_min_4h  <  mae_min_4h
```

Empates (`mfe_min_4h == mae_min_4h`, mismo minuto) **se descartan** y se reporta cuántos
fueron. Es lo conservador: un empate no favorece a ninguna de las dos hipótesis.

**La unidad independiente es la SEMANA**, igual que en todo el repo. Se calcula `P` dentro
de cada semana y cada semana pesa uno. El p que decide es el de **bloques por semana**,
nunca el binomial sobre 1.526 alertas.

**La dirección se declara acá: se espera P > 0,50**, o sea el MFE primero. Es la única
dirección con la que una regla de salida serviría de algo; si sale P < 0,50 el cierre es
todavía más rotundo y no se reinterpreta como "entonces hay que shortear" — eso sería otra
hipótesis y necesita su propio preregistro.

---

## 4. LAS COMPUERTAS

### El umbral, con la aritmética escrita ANTES

Una regla de salida que apunte al MFE y corte en el MAE cobra, a primer orden:

```
EV = P · MFE − (1 − P) · |MAE| − costo
   = P · 3,31 − (1 − P) · 3,62 − 0,20
EV > 0   ⟺   P > 3,82 / 6,93 = 0,551
```

> **(C) EL EFECTO: se necesita P > 0,55.** Por debajo de eso, ninguna regla de salida
> construida sobre este orden paga el costo, y **la dirección se CIERRA.**

**Los supuestos de esa cuenta, dichos ahora**, porque la corrida 14 predijo 10,8 %/año y
midió 28,4 — un 3× — justamente por no escribirlos: (a) usa las **medianas** de MFE y MAE
como si fueran alcanzables en cada trade, y no lo son —poner el objetivo en la mediana
significa que solo la mitad lo toca—; (b) supone que tocar el extremo equivale a salir
ahí. Es una cota de **orden de magnitud**, no una promesa. Por eso el umbral se fija en
**0,55 redondo** y no en 0,551.

### (P) POTENCIA — antes de mirar P

```
MDE = 2,8 · sigma_semanal(P) / sqrt(n_semanas)
```

> **Si el MDE sobre `P` supera 0,05, se declara "no se pudo medir"** y no se concluye
> nada. Y hay que **decir cuál de las dos falló**: por *n* se reabre esperando, por *σ*
> no (regla de la corrida 9).

**La cuenta a priori, hecha antes de correr:** con ~139 entradas por semana y `P ≈ 0,5`,
la σ binomial dentro de una semana es `0,5/√139 ≈ 0,042`. Con la variación entre semanas,
`σ_semanal ≈ 0,06`. Entonces `n = (2,8 · 0,06 / 0,05)² ≈ 11` semanas.

→ **Esto se puede medir en ~3 meses.** Es lo que separa a esta pregunta del skew, que pide
años. Es la razón principal para instrumentarlo hoy.

### La fecha

Instrumentación el **2026-09-01** → 12 semanas → **se mira el 2026-11-24**, no antes.

> **Y lo que la borra, verificado hoy** (regla nueva de `HANDOFF_FUENTES_NUEVAS.md` §6):
> `daytrader_outcomes` tiene retención de **550 días** desde que se mergeó el PR #27
> (`17fa52f`, 2026-09-01). Los datos del 2026-09-01 sobreviven hasta 2028-03. **La fecha
> está cubierta con margen.** La base entera son ~45 MB de los 500 del free tier, así que
> tampoco hay presión de espacio.

---

## 5. Lo que este preregistro NO autoriza

- **Escribir `dt_salida.py` antes del 2026-11-24**, ni mirar `mfe_min_4h` mientras se
  acumula. Es la regla que ahorró las corridas 8, 9, 13 y 14.
- **Barrer objetivos y stops** (+1 %, +2 %, +3 %…) buscando una combinación que dé. La
  pregunta de §3 es un número, no una grilla. Si `P` pasa, la regla de salida se diseña en
  **otro** preregistro, con sus propias compuertas.
- **Barrer la ventana.** 4h está fijado en §2.
- **Aflojar el 0,55** si sale 0,53. Ese umbral salió de una aritmética escrita antes de
  ver el dato; moverlo después es fabricar el resultado.
- **Reinterpretar un `P < 0,50`** como una señal short. Sería otra hipótesis.
- **Rellenar hacia atrás** las columnas nuevas para llegar antes al n. Eso convierte un
  test ciego en uno contaminado por las 11 semanas que ya miré (§1, fuga declarada).

---

## 6. La expectativa honesta

**Se espera P ≈ 0,50 y un cierre.** Con asimetría de magnitudes 0,91 y los cuatro
horizontes en negativo, lo más probable es que el orden también sea una moneda.

Si eso pasa, el resultado es que **el daytrader no tiene una palanca de salida**, y
entonces lo que queda medido es lo que ya dijo `dt_vivo.py`: la señal agarra volatilidad y
no dirección, y el score está invertido. Eso **cierra la última pregunta abierta sobre el
sistema en producción** — que es exactamente lo que hace falta para decidir con datos si
se sigue o no.

Y si sale P > 0,55, no es un negocio todavía: es permiso para escribir el preregistro de
la regla de salida.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

*(vacío hasta el 2026-11-24)*
