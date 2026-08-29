# PREREGISTRO — CORRIDA 5: el INSTRUMENTO (perpetuo en vez de spot)

> Escrito el **2026-08-27**, **antes de bajar una sola vela de futuros**. Es la dirección
> 4.2 de `HANDOFF_SIGUIENTE.md`, la única de las cinco que no necesita una fuente de datos
> nueva ni un instrumento que puede no existir.
>
> Los resultados van **debajo de la línea**, como en las corridas 1 a 4.

---

## 1. La pregunta, y por qué las cuatro corridas anteriores NO la contestan

Todo el repo —las 4.140 hipótesis direccionales incluidas— midió **un juego long-only
sobre klines de spot con costo de spot**. La corrida 3 usó *features* de futuros (OI,
posicionamiento) pero **la serie de precios seguía siendo spot y el costo también**.

Con el perpetuo cambian **tres cosas a la vez**, y hay que separarlas o el resultado no
dice nada:

| qué cambia | de | a | por qué podría importar |
|---|---|---|---|
| **costo** | 0,20% ida y vuelta (taker spot 0,10%/lado) | **0,10%** (taker fut 0,05%/lado) | la mitad del término de fee de toda la tabla de §2.3 del handoff |
| **carry** | no existe en spot | **funding cada 8h**, con signo | un largo paga cuando el funding es positivo; un corto **cobra**. A 24h son 3 pagos |
| **universo** | top-200 spot por volumen | **top-200 perp por volumen** | hay perps de monedas con spot flaco o sin spot, y `1000X` que en spot no se rankean |

Y una cuarta, silenciosa: **la serie de precios del perp no es la de spot.** La base se
mueve, y se mueve más justo cuando hay presión direccional.

**La honestidad de esta corrida está en el contraste, no en el número.** El handoff ya
lo dice y hay que repetirlo acá: *abaratar el trading baja la vara, no resucita nada*.
Si aparece un sobreviviente que en spot no estaba, hay que poder decir **cuál de las tres
cosas** lo produjo. Por eso son **tres paneles**, no uno.

---

## 2. Diseño — espejo exacto de la corrida 2, cambiando SOLO el instrumento

Todo lo que no sea el instrumento queda **idéntico a la corrida 2**: ventana
2025-08-01 → 2026-08-01, velas de 1h, `paso = horizonte = 24h` (sin solape), `k = 20`,
panel **ANCHO** (con las 6 features de flujo), las dos direcciones de cada score, los
tres objetivos, `MIN_SYMS = 30`, y las mismas seis compuertas de `ranking.py`.

**Tres paneles:**

| panel | símbolos | velas | costo | carry |
|---|---|---|---|---|
| **S** | `base200` (spot) | **spot** | 0,20% | — |
| **F∩S** | los de `base200` **que tienen perp** | **perp** | 0,10% | funding |
| **F200** | top-200 por volumen de **perp** (`fut200`) | **perp** | 0,10% | funding |

- **S** es la réplica de la corrida 2. **Se re-corre igual**, aunque ya esté medida: si el
  comparador sale de un CSV viejo, cualquier diferencia de código se cuela como si fuera
  del instrumento. El comparador tiene que salir del **mismo binario, el mismo día**.
- **S vs F∩S** aísla el **instrumento** (precio del perp + costo + funding) con el
  universo clavado.
- **F∩S vs F200** aísla el **universo**.

### El funding, y cómo entra

El funding no es un costo de transacción: es parte del **retorno del activo**. Así que
entra en el retorno, **no en el término de costo**, y le entra a **las dos patas** (top-k
y universo), no solo a la seleccionada:

```
carry(sym, t)  = suma de los fundings pagados en (t, t+H]      # signo directo de Binance
ret_largo      = ret − carry          # funding > 0 -> el largo PAGA
ret_corto      = −ret_largo           # ...y el corto COBRA
```

La antisimetría se mantiene (`y_corto = −y_largo`), así que las dos direcciones siguen
siendo el mismo test con el signo dado vuelta, como en las corridas 1-4.

**`magnitud` no lleva funding ni costo**: no es una posición (§3.7 de la corrida 1), y
`runup − caída` es camino de precio.

### Los símbolos `1000X`

`1000PEPEUSDT` en perp es `PEPEUSDT` en spot con el precio ×1000. **Los retornos son
idénticos y el ATR normalizado también**, así que el mapeo no distorsiona nada. Se usa
`funding._variantes` para resolverlo, que ya está escrito y probado.

---

## 3. Compuertas — las mismas, sin tocar

Las seis de `ranking.py`, cableadas: spread > 0, **signo crudo igual al normalizado**
(anti-artefacto de escala), fuera del MDE del azar, FDR q=0,10 **sobre el lote entero de
cada panel**, `sin_top3` > 0, `sin_top1` > 0, y ≥60% de semanas con spread > 0.
Veredicto por default: **CERRADA**.

La nula (8 rankings al azar × 3 objetivos) se corre **primero y por panel**, porque el
MDE de F200 no tiene por qué ser el de S.

---

## 4. Regla de parada — fijada ANTES, y es la del handoff

La del handoff §4.2, escrita para esta corrida:

> *Si el lote de futuros no da MÁS sobrevivientes que el de spot sobre la misma ventana
> y la misma nula, el cambio de instrumento no aporta.*

Aterrizada, en orden, y sin margen para discutirla después:

1. **Primaria.** Sobrevivientes **direccionales** (`largo` + `corto`) en **F∩S** > los de
   **S** en la misma corrida. S ya se midió en 0 y se espera 0 otra vez, así que en la
   práctica la barra es: **al menos uno**. Si **F∩S = 0** → *el instrumento no aporta*, y
   el eje "costo + funding + precio del perp" de 4.2 queda **cerrado**.

2. **Si F200 tiene sobrevivientes y F∩S no**, el hallazgo **no es del instrumento, es del
   universo** — y hay que decirlo con esas palabras. Además tiene que pasar dos cosas más,
   porque el universo extra es justamente la cola ilíquida que §2.3 ya cerró por costos:
   sobrevivir a **0,50%** (compuerta 7 de siempre), y que sus símbolos se contrasten
   contra la tabla de costos reales del libro (rank 201-400 = 0,441% a $1k, 0,994% a $10k).
   Si el sobreviviente vive de nombres de esa banda, **muere ahí**.

3. **Todo se corre a los dos costos**: el de futuros (0,10%) y 0,50%. Un sobreviviente que
   solo vive a 0,10% **no cuenta**. Esto es lo que impide que "abaratar el trading" se
   disfrace de hallazgo.

4. **Si un sobreviviente de `corto` desaparece al poner `carry = 0`, no es direccional:
   es CARRY.** Y entonces la pregunta correcta no es la de esta corrida sino: *¿shortear
   las monedas de funding más alto, sin ningún ranking direccional, da lo mismo?* Se mide
   como brazo aparte (`carry_acum`, ver §5), y si el brazo pelado empata al ranking, lo
   que hay es una prima de funding, no una señal. **Eso se escribe ahora para no
   discutirlo después.**

5. **Multiplicidad entre paneles.** Son 3 paneles × ~2 costos = 6 lotes. No se elige el
   mejor: se reportan **los seis**, y un sobreviviente aislado en uno solo mientras sus
   vecinos están en cero se trata con la regla 2 de la corrida 4 (**ruido de barrido**).

6. **Régimen.** Cualquier sobreviviente va al corte de §5.2 de la corrida 3. Pero acá hay
   un límite duro que conviene declarar de entrada: la ventana es **un año y un solo
   régimen bear**. Con eso **no se puede promover nada a capital**, gane lo que gane. Lo
   máximo que puede producir esta corrida es un **candidato** para la reserva OOS
   2024-08 → 2025-08, que sigue **sin mirarse**.

---

## 5. Brazos — los mismos, más uno

Los ~35 scores de la corrida 2 (precio + flujo), sus residualizados contra `roc_24`, sus
dos direcciones, y 3 controles al azar. **Más un brazo nuevo que solo existe en perp:**

- **`carry_acum`** — el funding acumulado de las últimas 24h de cada símbolo. Es la única
  variable que no existe en spot y que no se probó nunca como *ranking* (la corrida 3
  probó OI y posicionamiento; funding como score, no). Va con sus dos direcciones y su
  residualizado, como todos.

Nótese que este brazo es **el que la regla 4.4 vigila**: si funciona, hay que separar
carry de dirección.

---

## 6. Lo que esta corrida NO puede decir

1. **Nada sobre apalancamiento.** El perp permite 20×; nada de lo que se mide acá cambia
   con leverage (multiplica retorno y riesgo por igual, y agrega liquidación). Fuera de
   alcance, a propósito.
2. **Nada sobre la cola.** El universo son 200 perps líquidos. La cola sigue cerrada por
   §2.3.
3. **Nada fuera de un año bear.** Ver regla 6.
4. **No modela impacto de mercado** ni la operativa de rebalancear 20 posiciones por día.
   Es un ranking, no un backtest — la misma limitación de las corridas 1-4.

---

## 7. Fuga declarada — lo que ya vi antes de escribir esto

- Vi **0 de 4.140** direccionales en spot. Esta corrida es, por diseño, un intento de
  **contradecir** un negativo que ya conozco. El sesgo apunta a **encontrar algo**, no a
  no encontrarlo, y por eso las reglas 2, 3 y 4 son más duras que las de una corrida
  exploratoria.
- Vi que la magnitud sobrevive en spot en cuatro regímenes. Si en perp también sobrevive,
  **no es un hallazgo nuevo**: es una réplica, y se reporta como tal.
- No he mirado **ninguna** vela de perpetuo, ni un funding agregado, ni el universo
  `fut200` más allá de contar cuántos perps USDT hay vivos (**524**, consultado hoy para
  saber si el universo alcanzaba).

---

# RESULTADOS DE LA CORRIDA 5 (2026-08-27)

Cuatro paneles, no tres: la corrida obligó a agregar **SF** (spot sobre *exactamente* los
mismos símbolos que quedaron en FS). Por qué, en §R3.

| panel | qué es | pares | filas | barras | semanas | MDE (dirección) |
|---|---|---|---|---|---|---|
| **S** | spot, `base200` | 187 | 58.573 | 335 | 49 | ±0,122 |
| **SF** | spot, los mismos 172 nombres de FS | 170 | 53.232 | 335 | 49 | ±0,116 |
| **FS** | **perp** de esos mismos nombres | 172 | 54.043 | 335 | 49 | ±0,126 |
| **F200** | **perp**, top-200 por volumen de perp | 191 | 58.928 | 335 | 49 | ±0,125 |

Funding: **100% de cobertura**, carry mediano **+0,0161% cada 24h** — o sea que el largo
paga y el corto cobra **~5,9% anualizado**. No es despreciable, y es la primera vez que el
banco lo mete dentro del retorno en vez de ignorarlo.

## R1. La regla de parada 1, contestada

| panel | costo | brazos direccionales | spread > 0 | **SOBREVIVEN** | mejor spread |
|---|---|---|---|---|---|
| S | 0,20% | 140 | 1 | **0** | +0,0213 |
| S | 0,50% | 140 | 0 | **0** | −0,1974 |
| SF | 0,20% | 140 | 3 | **0** | +0,0449 |
| SF | 0,10% | 140 | 20 | **0** | +0,1149 |
| SF | 0,50% | 140 | 0 | **0** | −0,1649 |
| **FS** | **0,10%** | 148 | 44 | **2** | **+0,1890** |
| **FS** | **0,50%** | 148 | 0 | **0** | −0,1017 |
| **F200** | **0,10%** | 148 | 47 | **2** | **+0,2067** |
| **F200** | **0,50%** | 148 | 1 | **0** | +0,0179 |

**El panel S replica la corrida 2** (0 de 140), que era el requisito para que el
comparador valiera: sale del mismo binario y el mismo día.

Los cuatro candidatos —los cuatro `largo`, ninguno `corto`— son:

| panel | ranking | spread @0,10% | crudo | sin_top3 | sem>0 | p |
|---|---|---|---|---|---|---|
| FS | `dd_720` | +0,1890 | +0,0016 | +0,0776 | 65% | 0,0125 |
| FS | `roc_168 ~ sin roc_24` | +0,1651 | +0,0019 | +0,1230 | 61% | 0,0065 |
| F200 | `roc_168` | +0,2020 | +0,0045 | +0,1453 | 61% | 0,0165 |
| F200 | `pos_168` | +0,1834 | +0,0026 | +0,1233 | 61% | 0,0005 |

**Los cuatro mueren en la regla 3** (compuerta de 0,50%), que estaba escrita antes de
correr. Y mueren de una forma que vale la pena mirar de cerca (§R2).

## R2. No mueren por ser cero: mueren porque el efecto TIENE EL TAMAÑO DEL COSTO

Interpolando entre las dos pasadas de costo, el punto donde cada candidato cruza cero:

| candidato | costo de equilibrio (ida y vuelta) |
|---|---|
| `dd_720` (FS) | **31 bps** |
| `roc_168 ~ sin roc_24` (FS) | **35 bps** |
| `pos_168` (F200) | **37 bps** |
| `roc_168` (F200) | **46 bps** |

Contra eso, la tabla de costos **reales medidos** del §2.3 del handoff (`banco/libro.py`,
caminando el libro): rank 1-50 = **23–28 bps**, rank 51-200 = **34–60 bps**.

> **El costo de equilibrio de los cuatro cae DENTRO de la banda de costo real medido.**
> No hay margen: son brazos que viven o mueren según en qué parte de esa banda caiga la
> ejecución real.

**Y acá hay algo honesto que declarar: esa tabla se midió sobre el libro de SPOT.**
`libro.py` camina `api.binance.com`. El libro del **perpetuo** no está medido, y para los
nombres líquidos suele ser más ajustado y más profundo. O sea que el número que decide
—el costo total (fee + spread + slippage) de un perp— **es la única pieza que falta**, es
barata de medir, y es lo único que esta corrida deja abierto. Ver §R6.

## R3. Por qué hubo que agregar el panel SF, y qué apareció ahí

`dd_720` daba **−0,2209** en spot y **+0,1890** en perp: un salto de **+0,41 ATR**.
Abaratar el fee de 0,20% a 0,10% vale ~+0,05 ATR. O sea que el salto **no podía ser el
instrumento**, y sin un panel intermedio no había forma de saber qué era.

Resulta que **26 de los 200 símbolos de `base200` no tienen perpetuo**, y **16 de esos 26
no son cripto**:

- **7 stablecoins y FX**: `USD1`, `RLUSD`, `EURI`, `FDUSD`, `EUR`, `BFUSD`, `XUSD`
- **9 acciones tokenizadas** (sufijo `B`): `QQQB`, `SPCXB`, `SPYB`, `SNDKB`, `CRCLB`,
  `SKHYB`, `AAPLB`, `SNXXB`, `NVDAB` — que no cotizan fines de semana

Su `atr_base` mediano es de **0,013% a 0,018%**, contra ~2% de una alt normal: **cien
veces más quietas.** Y son el **9,1% del universo** pero se llevan:

| ranking | % de los cupos del top-20 que se llevan los 26 |
|---|---|
| `vol_24 [bajo]` | **44,3%** |
| `atr_24 [bajo]` | **43,1%** |
| `dd_168` | **37,8%** |
| `dd_720` | **36,9%** |
| `roc_168` | 7,3% *(no los selecciona)* |

**Cualquier ranking que elija "lo quieto" se llena de instrumentos que no son cripto.**
Por eso `dd_720` y `dd_168` estaban en −0,22 y −0,28 en el panel S: no los mataba el
mercado, los mataba la composición del universo.

### La descomposición, que es el resultado central de la corrida

Con los cuatro paneles se puede partir el salto en sus tres pedazos:

| brazo (`largo`) | S@0,20 | SF@0,20 | SF@0,10 | FS@0,10 | **universo** | **costo** | **perp** |
|---|---|---|---|---|---|---|---|
| `dd_720` | −0,2209 | +0,0098 | +0,1044 | +0,1890 | **+0,2307** | +0,0947 | +0,0845 |
| `dd_720 ~ sin roc_24` | −0,2558 | −0,0415 | +0,0535 | +0,1542 | **+0,2143** | +0,0950 | +0,1007 |
| `dd_168` | −0,2799 | −0,1145 | −0,0192 | +0,1202 | **+0,1655** | +0,0953 | +0,1394 |
| `roc_168 ~ sin roc_24` | +0,0213 | +0,0449 | +0,1149 | +0,1651 | +0,0237 | +0,0700 | +0,0502 |
| `roc_168` | −0,0279 | −0,0114 | +0,0591 | +0,1663 | +0,0164 | +0,0705 | +0,1072 |
| `pos_168` | −0,0874 | −0,0555 | +0,0223 | +0,0906 | +0,0319 | +0,0778 | +0,0683 |

Y **promediando los 140 brazos direccionales**, que es donde se ve sin cherry-picking:

| panel | media | mediana | máximo |
|---|---|---|---|
| S @0,20 | −0,2007 | −0,1674 | +0,0213 |
| SF @0,20 | −0,1541 | −0,1532 | +0,0449 |
| SF @0,10 | −0,0770 | −0,0760 | +0,1149 |
| FS @0,10 | −0,0730 | −0,0660 | +0,1890 |

> **Limpiar el universo vale +0,047. Abaratar el fee vale +0,077 — exactamente el término
> aritmético de costo, ni un punto más. Y cambiar la serie de spot al perpetuo vale
> +0,004: CERO.**

Eso contesta 4.2 con un número en vez de con una opinión. La frase del handoff
—*"abaratar el trading baja la vara, no resucita nada"*— queda **medida**: el
abaratamiento mueve todos los brazos por igual, que es lo que hace una constante, no una
señal.

## R4. El lado corto: cero, y eso golpea la premisa de 4.2

**Ninguno de los cuatro candidatos es `corto`. En ninguno de los cuatro paneles, a ningún
costo, sobrevivió un solo brazo corto.**

El caso de 4.2 era: *el repo encuentra señales del lado corto una y otra vez y no tiene
instrumento para operarlas* (lead-lag 149 cortas y 0 largas; funding sentimiento +3,86pp;
OI shock bajista +7pp). Con el instrumento puesto —perp, short real, fee a la mitad y
cobrando funding— **el lado corto no ordenó nada**. El mejor brazo corto de FS@0,10 es
`roc_720 ~ sin roc_24 [bajo]` con **+0,0048**, dentro del MDE del azar.

Y el carry jugaba **a favor** del corto: +0,0161% cada 24h, ~5,9% anualizado. Aun con ese
viento de cola, cero.

## R5. `carry_acum` — el brazo que solo existe en perp

El funding de las últimas 24h como **ranking transversal** (no como costo, no como filtro
de sentimiento pooled — como score, que es lo que el banco nunca había hecho):

| panel | costo | brazo | spread | veredicto |
|---|---|---|---|---|
| F200 | 0,10% | `carry_acum` | +0,1439 | muere en FDR |
| FS | 0,10% | `carry_acum ~ sin roc_24 [bajo]` | +0,1104 | muere en FDR |
| FS | 0,10% | `carry_acum [bajo]` | +0,0761 | muere en FDR |
| F200 | 0,50% | `carry_acum` | −0,0509 | spread ≤ 0 |

**No sobrevive en ningún panel a ningún costo.** La regla 4 (separar carry de dirección)
ni siquiera se activa: no hay sobreviviente corto que separar.

## R6. Veredicto

> **El cambio de instrumento no aporta. El precio del perpetuo ordena igual que el de
> spot (+0,004 ATR de diferencia media sobre 140 brazos), el lado corto —que era la mitad
> del caso de 4.2— sigue en cero con el instrumento puesto, y el funding como señal no
> pasa la corrección. Lo único que aporta el perp es el fee a la mitad, que mueve todos
> los brazos por igual y por eso no es una ventaja: es una vara más baja.**

**4.2 queda CERRADO** en sus tres ejes medibles con lo que hay: precio del perp, lado
corto, funding (como costo y como señal).

**Lo único que queda abierto, y es concreto y barato:** los cuatro candidatos cruzan cero
entre **31 y 46 bps** ida y vuelta, y el **costo real del libro del perpetuo no está
medido** — `libro.py` camina el libro de spot. Si el perp de los top-200 ejecuta por
debajo de ~30 bps ida y vuelta *incluido spread y slippage*, estos cuatro brazos vuelven
a estar vivos y hay que correrles el corte por régimen y la reserva OOS. Si ejecuta por
arriba, están cerrados del todo. **Es una medición de un rato, no una sesión.**

## R7. Lo que NO se hizo, a propósito

**No se re-corrió el panel con `USDCUSDT` afuera.** Es el único símbolo con ATR patológico
que sobrevive en FS/SF (`atr_base` 0,0129%, o sea 15,5 ATR de costo a 0,20%). Sacarlo
sería aflojar el universo **después** de ver los resultados, que es exactamente lo que la
§3 prohíbe. `SF@0,20` ya da 0 sobrevivientes con él adentro, así que la conclusión no
depende de sacarlo.

## R8. Una advertencia que excede a esta corrida

El defecto de §R3 **está en `base200`, que es el universo de las corridas 1, 2 y 4.** Ahí
nunca se vio porque nada sobrevivía igual, pero el sesgo tiene dirección conocida:
**empuja hacia abajo el spread de todo ranking que seleccione baja volatilidad o baja
caída**, o sea que pudo producir **falsos negativos** en esa familia.

La corrida 5 lo re-testea sin querer: **`SF@0,20` es la corrida 2 con el universo limpio,
y da 0 sobrevivientes direccionales igual** (mejor brazo +0,0449, contra un MDE de
±0,116). Así que la conclusión de las corridas 1, 2 y 4 **se sostiene** — pero de ahora en
adelante el universo hay que filtrarlo por clase de activo, no solo por volumen.

**`magnitud` no se ve afectada** y replica en los cuatro paneles (34, 34, 38 y 30
sobrevivientes): es la quinta vez que sobrevive, ahora también sobre perpetuos.

---

# CORRIDA 5b — el libro del perpetuo, medido (2026-08-28)

Esto cierra el único pendiente que §R6 dejó abierto, y lo cierra por el camino que §R6
dejó escrito **antes** de medir nada: *"si el perp de los top-200 ejecuta por debajo de
~30 bps ida y vuelta incluido spread y slippage, estos cuatro brazos vuelven a estar
vivos"*.

## El libro, medido en el MISMO instante que el de spot

`banco/libro_perp.py`. 200 perps por volumen, 146 con par de spot vivo, 3 snapshots, los
dos libros pedidos en la misma vuelta y apareados por símbolo — porque medir el perp hoy y
compararlo contra la tabla de spot de otro día mezcla mercado con momento, que es la regla
de método que dejó §R3.

| banda | orden | **spread perp** | spread spot | **costo perp** | costo spot | diferencia |
|---|---|---|---|---|---|---|
| 1-50 | $1k | **0,011%** | 0,027% | **12,1 bps** | 23,9 bps | −11,8 |
| 51-200 | $1k | **0,024%** | 0,070% | **18,1 bps** | 34,4 bps | −15,6 |
| 1-50 | $10k | **0,013%** | 0,027% | **14,7 bps** | 29,5 bps | −13,6 |
| 51-200 | $10k | **0,025%** | 0,067% | **31,9 bps** | 61,6 bps | −26,3 |

**El libro del perpetuo es la mitad del de spot, y el spread cotizado es 2,6× más
ajustado.** Ningún par falló en llenar la orden a ninguno de los dos tamaños.

Mediana del top-200 perp: **16,6 bps a $1k** (100% de los pares por debajo de 31) y
**29,5 bps a $10k** (55,5% por debajo). O sea que **la condición de §R6 se cumple**: el
perp ejecuta por debajo de la banda de equilibrio de los cuatro candidatos.

> **Limitación heredada de `libro.py` y declarada de nuevo:** el libro es el de HOY
> aplicado a una ventana histórica. Si en 2025-08 era peor, los candidatos mueren más, no
> menos — así que para un cierre el sesgo va a favor.

## Y aun así los cuatro mueren — pero ahora por otra cosa

Re-corridos los dos paneles a los costos **medidos** (0,17% y 0,30%) en vez del 0,10% de
solo-fee:

| panel | costo | brazos | spread > 0 | **SOBREVIVEN** | mejor |
|---|---|---|---|---|---|
| FS | 0,17% | 148 | 21 | **0** | +0,1253 |
| FS | 0,30% | 148 | 4 | **0** | +0,0320 |
| F200 | 0,17% | 148 | 38 | **0** | +0,1662 |
| F200 | 0,30% | 148 | 18 | **0** | +0,1076 |

| candidato | @0,10% | @0,17% | @0,30% | **muere en** |
|---|---|---|---|---|
| `dd_720` (FS) | +0,1890 ✓ | +0,1253 | +0,0072 | **FDR** |
| `roc_168 ~ sin roc_24` (FS) | +0,1651 ✓ | +0,1184 | +0,0317 | **FDR** |
| `roc_168` (F200) | +0,2020 ✓ | +0,1624 | +0,0889 | **FDR** |
| `pos_168` (F200) | +0,1834 ✓ | +0,1355 | +0,0466 | **consistencia semanal (55%)** |

> **Los cuatro necesitaban el 0,10% de solo-fee. Al costo real del instrumento que los
> favorecía mueren en compuertas que no tienen nada que ver con el costo: multiplicidad y
> consistencia semanal.**

## Por qué esto es un cierre MEJOR que el de §R1

El cierre original los mataba con una guarda de **0,50%** que era conservadora justamente
porque el costo del perp **no estaba medido**. Se le podía objetar —con razón— que 0,50%
era irreal para un perpetuo líquido.

Ahora esa objeción no existe: el costo está medido, es **16,6 bps**, la condición que §R6
puso para reabrirlos **se cumplió**, se los re-corrió a ese costo, y **igual no pasan**. Un
resultado que sobrevive a que le corrijan el número a favor es mucho más firme que uno que
nunca fue puesto a prueba.

**El pendiente de §R6 queda resuelto y 4.2 queda cerrado del todo.** No hace falta extender
el panel de perp a cinco años para el corte por régimen: no hay candidato que cortar.

## Lo que sí queda, y vale para cualquier trabajo futuro

**La tabla de costos del §2.3 del handoff es de SPOT y subestima al perpetuo por 2×.**
Cualquier cosa que se evalúe sobre perp de acá en adelante usa esta tabla, no aquella. Y
la conclusión de fondo de la corrida 5 no se mueve: el instrumento más barato **baja la
vara** (más brazos con spread positivo: de 44 a 0,10% a 38 a 0,17% en F200), pero no
fabrica sobrevivientes.
