# PREREGISTRO — ¿el libro mejora la elección del radar?

> Escrito el **2026-11-13**, **antes de calcular la combinación**. De la ventana principal
> ya se conoce cada parte por separado (`PREREGISTRO_LIBRO.md`, RESULTADOS): `prof_1_rel`
> +0,160 ATR en magnitud, +0,093 residualizado contra `n_surge`. **No se calculó la
> combinación, ni la correlación entre las dos, ni el spread de `n_surge` en este
> tablero.** La reserva 2026-08 → 10 **no se bajó**. Si algo de este archivo cambia después
> de ver un número, el experimento no vale.

---

## 1. La pregunta, y por qué no está contestada

`PREREGISTRO_LIBRO` §6.3 dijo que el libro trae magnitud que `n_surge` no tiene. Eso es una
afirmación sobre **un residuo lineal**, no sobre la decisión que toma el radar. Lo que
importa para los straddles es otra cosa:

> **Si el radar elige su top-8 con `n_surge` Y la profundidad del libro, ¿los 8 elegidos
> se mueven más que los 8 que elige hoy con `n_surge` solo, en la misma barra?**

No es consecuencia de §6.3, por dos razones:

1. **El residuo no es el incremento.** Que `prof_1_rel ~ sin n_surge` ordene magnitud no
   dice cuánto cambia el top-8: si las dos features coinciden en los extremos, el top-8
   combinado y el de `n_surge` son casi los mismos nombres y la diferencia se va a cero.
2. **El antecedente es negativo.** `banco/combo.py` combinó `oi_rel_168` + `n_surge` +
   `turnover` y ganó **+0,0077 contra un MDE de ±0,078**: ruido. Ese es el motivo por el
   que el radar usa UNA feature. Aquellas tres medían lo mismo (actividad); la apuesta de
   este preregistro es que el libro no, y eso es lo que se prueba.

Además contesta la duda que dejó abierta §6.3 (la residualización es lineal y `n_surge`
tiene cola pesada): este test **no usa ningún modelo lineal**. Compara dos selecciones.

---

## 2. Diseño

**Tablero:** el de `PREREGISTRO_LIBRO`, sin cambios. Los 46 de `deriv46`, `paso =
horizonte = 4h`, `k = 8`, objetivo `y_magnitud` de `ranking.py` (recorrido
`runup − caida` en unidades de `atr_base`), con la vista cruda como chequeo.

**Ventanas:**
- **Principal:** 2023-01-01 → 2026-07-31 (183 semanas). **No es independiente**: de ahí
  salió `prof_1_rel`. Es condición necesaria, no evidencia (ver §5).
- **Reserva:** 2026-08-01 → 2026-10-31 (13 semanas). **Es la que decide.** Se baja solo
  si la principal pasa y si tiene potencia (§3).

**Los dos rankings — y nada más:**

| | score por barra |
|---|---|
| **BASE** | `n_surge`, tal cual lo usa `radar.py` |
| **COMBO** | promedio del **percentil dentro de la barra** de `n_surge` y de `prof_1_rel`, pesos iguales |

Por qué así y no de otra forma, decidido ahora:

- **Percentil, no z-score** (a diferencia de `combo.py`). `n_surge` es un cociente con cola
  pesada: con z-score, un par con 8x de actividad domina la suma y COMBO sería BASE con
  ruido. El percentil le da a las dos el mismo rango.
- **Pesos iguales.** Cualquier otro peso es un parámetro, y elegirlo mirando la principal
  es ajustar sobre la misma muestra que eligió la feature.
- **`prof_1_rel` y no `prof_1_pct`.** Fue la de mayor residuo contra `n_surge` (+0,093
  contra +0,076). Probar las dos duplica los brazos y abre la puerta a quedarse con la
  que dé mejor.
- **Pares sin libro en esa barra** (1,8 % de las filas): COMBO usa el percentil de
  `n_surge` solo, así BASE y COMBO eligen sobre **el mismo universo** en cada barra.

**Un solo brazo.** No hay FDR porque no hay lote. Si COMBO no pasa, no se prueba otro
peso, otra feature del libro ni otro k.

**El estadístico:** Δ = media semanal de

    magnitud(top-8 COMBO) − magnitud(top-8 BASE)      en la MISMA barra

Pareado por barra: el universo, el régimen y la hora se cancelan. Si los dos top-8 son
idénticos, esa barra aporta 0, que es lo correcto.

---

## 3. Compuertas

### Principal (P) — todas, en este orden

1. **Solapamiento, antes que nada:** se reporta la fracción media de nombres compartidos
   entre los dos top-8. Es descriptivo, no compuerta, pero se escribe antes de mirar Δ.
2. **MDE pareado:** `MDE_P = 2,8 · sd(Δ semanal) / √semanas`.
3. **Δ > MDE_P** (no basta Δ > 0).
4. **Bootstrap de semanas**, unilateral, **p < 0,05**.
5. **≥ 60 % de semanas con Δ > 0.**
6. **Sin los 3 pares que más aportan, Δ > 0** (la de siempre).
7. **Por año calendario (2023, 2024, 2025, 2026) Δ > 0 en los cuatro.** Es la regla de
   `PREREGISTRO_LIBRO` §4: para un straddle, un incremento que se da vuelta en un año no
   es un incremento.
8. **Misma señal en la vista cruda** (sin dividir por `atr_base`), y `atr_ratio` de COMBO
   contra BASE entre 0,95 y 1,05. Si COMBO gana solo porque elige nombres de ATR más
   alto, es el artefacto de escala de la corrida 1.

### Potencia de la reserva — se calcula ANTES de bajarla

    MDE_R = 2,8 · sd(Δ semanal de la principal) / √13

- **Si `MDE_R` > Δ de la principal**, la reserva no puede confirmar ni un efecto del
  tamaño del que se midió. **No se baja.** Se declara "pasa en muestra, la reserva no
  alcanza", y la reserva queda intacta para un test con más semanas.
- Si alcanza, se baja.

### Reserva (R)

**Δ_R > 0 y bootstrap unilateral p < 0,05** sobre las 13 semanas. Nada más. Ninguna
compuerta de la principal se re-aplica para "rescatar" un resultado.

---

## 4. Regla de parada

1. **Falla P** → el libro no mejora la elección del radar, aunque ordene magnitud por su
   cuenta. **El radar no cambia** y la familia "libro para straddles" queda cerrada en
   esta forma. No se prueba otra combinación.
2. **Pasa P, la reserva no tiene potencia** → resultado **en muestra**. El radar no
   cambia. Lo que sigue, si se quiere, es juntar hacia adelante; no se baja la reserva para
   "ver qué da".
3. **Pasa P, falla R** → era la muestra. Se cierra como el caso 1.
4. **Pasa P y R** → COMBO es candidato a reemplazar `n_surge` en el radar. **Recién ahí**
   se abre el problema de implementación (§6), que no es parte de este test.

**En ningún caso esto toca el forward test en curso.** `PREREGISTRO_SKEW` mide la prima del
straddle sobre el top-8 **por `n_surge`**, con compuerta en n = 200 apareados (~enero de
2027). Cambiar la elección del radar antes de esa compuerta rompe ese test. Si COMBO
pasa todo, entra **después** de esa compuerta, o como columna paralela que no cambia el
top-8 que se mide.

---

## 5. Fuga declarada — lo que ya sé antes de correr

1. **La principal está contaminada a favor.** `prof_1_rel` salió de esa ventana, y ya sé
   que su residuo contra `n_surge` es +0,093. Por eso P es necesaria y no suficiente, y por
   eso se escribió la regla de potencia de la reserva antes de nada.
2. **El prior para P es moderadamente alto; para el tamaño, bajo.** Un residuo de +0,093
   en el top-8 *de la feature sola* no se traslada entero al top-8 combinado: la mitad del
   peso sigue siendo `n_surge`. Espero un Δ chico, y que la pregunta real sea si la
   reserva tiene potencia.
3. **Sé el signo.** Libro más profundo que su mediana → más movimiento. Por eso COMBO usa
   el percentil alto de `prof_1_rel` y no el bajo, y eso no se cambia según el resultado.
4. **No sé la correlación entre `n_surge` y `prof_1_rel` dentro de la barra.** Es lo que
   más decide el resultado (si es alta, los top-8 coinciden y Δ → 0) y no la miré.

---

## 6. Lo que hay que construir, y lo que NO se resuelve acá

- `banco/libro_radar.py`, del tamaño de `combo.py`: carga el tablero de
  `PREREGISTRO_LIBRO`, arma BASE y COMBO, imprime en el orden de §3 y se detiene en la
  primera compuerta que falle. La reserva se baja con `load_libro(..., reserva=True)` y
  **solo** si el script lo autoriza según §3. La autorización la imprime el script.
- **Implementación en vivo — fuera de este test, pero hay que declararla.** `prof_1_rel`
  necesita la profundidad del perpetuo de las últimas 168 horas. Binance Vision publica
  los diarios con un día de atraso, así que el radar tendría que **juntar su propia
  historia** (`/fapi/v1/depth` cada hora). Antes de cualquier deploy hay que verificar que
  `fapi.binance.com` responde desde los runners de GitHub: `api.binance.com` da 451 desde
  ahí, y `radar.py` ya llama a `fapi` para `oi_rel_168` sin que esté medido si devuelve
  algo. Si no responde, COMBO no se puede usar desde CI aunque pase todo.

---

## ENMIENDA del 2026-11-13 — cinco pesos en la principal, uno solo en la reserva

Decidida por el usuario después de leer el diseño, **antes de calcular nada**: el estado
de la cabecera sigue igual (ni la combinación, ni la correlación, ni la reserva). La
objeción es razonable: pesos iguales es una elección tan arbitraria como cualquier otra,
y si la mezcla útil es 30/70 el diseño original no la encuentra.

Lo que **no** se puede es probar varios pesos sobre la reserva: con 13 semanas, el mejor
de cinco gana por azar y la reserva deja de confirmar nada. Por eso se separan los papeles.

**Reemplaza** a "pesos iguales" y "un solo brazo" de §2, y a "no se prueba otro peso" de
§4.1. El resto del documento no cambia.

1. **La grilla, fijada ahora.** COMBO(w) = `(1 − w) · pct(n_surge) + w · pct(prof_1_rel)`,
   con **w ∈ {0,20 · 0,35 · 0,50 · 0,65 · 0,80}**. w = 0 es BASE. No se agregan pesos
   después de ver la grilla, ni se refina alrededor del ganador.
2. **En la principal, los cinco** pasan por las compuertas de §3, cada uno contra BASE.
   La compuerta 4 (bootstrap) se corrige por **Holm sobre los 5**: el que tiene el menor p
   necesita p < 0,01, el segundo p < 0,0125, y así. Los cinco pesos están muy
   correlacionados, así que Holm es conservador. Se acepta ese costo a cambio de que no
   haga falta ningún supuesto.
3. **Elección:** entre los pesos que pasan **todas** las compuertas de P, el de mayor Δ.
   Si ninguno pasa, es §4.1: se cierra.
4. **Potencia de la reserva, con un número que no esté inflado por la elección.**

       MDE_R = 2,8 · sd(Δ semanal del peso elegido) / √13

   se compara contra la **mediana de Δ de los pesos que pasaron**, no contra el Δ del
   elegido, que viene favorecido por ser el máximo. Si pasó uno solo, se usa la mitad de
   su Δ. Si `MDE_R` supera ese número, la reserva no se baja (§4.2).
5. **La reserva prueba un único brazo:** el peso elegido, con las compuertas R de §3 sin
   cambios.
6. **El script** (`libro_radar.py`) imprime la tabla de los cinco pesos completa, sin
   ordenar por Δ, y la elección la hace el código con esta regla, no a mano.

Lo que cuesta: con Holm, la principal necesita un efecto algo mayor para pasar que el
brazo único original. Lo que se gana: si la mezcla útil no es 50/50, se encuentra.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

*(vacío: n = 0 el 2026-11-13)*
