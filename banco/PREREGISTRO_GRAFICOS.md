# PREREGISTRO — patrones de gráfico (la décima familia)

> **§1 (corrida 11, la compuerta de potencia)** corrida el **2026-08-29**.
> **§2-§6 (corrida 12, el efecto)** escritos el **2026-08-29**, con los parámetros del
> detector fijados **antes de estimar un solo efecto**.
> Dirección **§2.1 de `HANDOFF_TRES.md`** — la última que queda.
> Código: `banco/graficos.py` (detector), `banco/potencia_graficos.py` (compuerta),
> `banco/correr_graficos.py` (el lote).

---

# §1. LA COMPUERTA DE POTENCIA — corrida 11

## 1.1 Por qué va primero

Es la advertencia que las corridas 8 y 9 dejaron pagada, y que quedó escrita en el handoff:

> Antes de construir el detector, correr la compuerta de potencia para medir la σ por
> evento. Un patrón de gráfico es un **evento esparcido sobre símbolos volátiles**, o sea
> exactamente la forma que hizo fracasar a la corrida 9. Si la σ sale del orden de la de
> listados, se cierra ahí y no se escriben las 300 líneas.

**La idea que la hace barata:** el MDE de este estimador **no depende de qué patrón sea**.
Depende de **cada cuánto dispara**. Un patrón que dispara poco tiene pocas barras por
semana, la media semanal es más ruidosa y el MDE se abre. Así que la curva `MDE(tasa)` se
mide con **máscaras al azar** —la nula real— **sin escribir un solo detector**, y recién
después se pregunta si los patrones de gráfico disparan en la zona medible o afuera.

**El umbral:** MDE ≤ **0,10 ATR**. Con un ATR base típico de ~4%/día, un costo de 0,20%
son ~0,05 ATR; un efecto que sobreviva a dos costos tiene que estar bastante arriba de eso.
*(Referencia de la corrida 7, mismo estimador y universo: MDE ±0,0386 a 1d, ±0,0221 a 1h.)*

## 1.2 La curva MDE(tasa) — y la corrección que hubo que hacerle

**Primera versión, y estaba mal:** la curva se midió solo en el **horizonte más corto** de
cada resolución. Eso reporta **el mejor caso**, no la compuerta.

> **El MDE no depende solo de la tasa de disparo: crece con el HORIZONTE**, porque el
> retorno a 5 días tiene mucha más varianza que el de 1 día. A 1d, con la misma tasa del
> 0,5%, el MDE pasa de **0,061** (H=1) a **0,091** (H=3) a **0,128** (H=5).
>
> Se corrigió antes de estimar ningún efecto: la compuerta **barre los horizontes**, y el
> corredor de la corrida 12 usa **el MDE del horizonte que corresponde** en vez de uno
> solo para todos —que sería laxo en el corto y estricto en el largo—.

### La frontera real: la tasa mínima de disparo que sigue siendo medible

| resolución | horizonte | **tasa mínima medible** | MDE ahí |
|---|---|---|---|
| **1d** | H=1 | **0,200%** | 0,0886 |
| **1d** | H=3 | **0,500%** | 0,0914 |
| **1d** | H=5 | **2,000%** | 0,0736 |
| **1h** | H=4 | **0,050%** | 0,0776 |
| **1h** | H=24 | **0,500%** | 0,0654 |

**A 1h la frontera del horizonte corto es 4× más baja que a 1d** (0,050% contra 0,200%):
hay 25× más barras, así que el mismo patrón dispara 25× más veces.

> **Lo que se pierde al bajar la tasa no es tanto la σ como las SEMANAS.** A 1d, de 0,20%
> a 0,02% la sd semanal se multiplica por 1,7 pero **las semanas con al menos un disparo
> caen de 177 a 32**. Un patrón raro no es ruidoso: es que **no está la mayoría de las
> semanas**.

## 1.3 ¿Dónde disparan los patrones de gráfico?

Detector mínimo (`graficos.py`), 136 pares, 168.347 barras diarias:

| patrón | disparos | tasa a 1d | medible a 1d |
|---|---|---|---|
| `doble_techo` | 1.197 | **0,711%** | H=1 ✔ H=3 ✔ H=5 ✗ |
| `doble_piso` | 1.122 | **0,666%** | H=1 ✔ H=3 ✔ H=5 ✗ |
| `triangulo` | 449 | **0,267%** | H=1 ✔ H=3 ✗ H=5 ✗ |
| `hch` | 421 | **0,250%** | H=1 ✔ H=3 ✗ H=5 ✗ |
| `hch_inv` | 307 | **0,182%** | **✗ en todos** |

## 1.4 VEREDICTO DE LA COMPUERTA: **PASA, pero acotada.** Y es la primera que pasa en tres corridas.

**Lo que sí es medible:**

- **A 1d, horizonte 1:** cuatro de los cinco patrones. `hch_inv` (0,182%) queda **abajo de
  la frontera de 0,200%** y entra marcado como **"no se pudo medir"** de antemano.
- **A 1d, horizonte 3:** solo los dos dobles.
- **A 1d, horizonte 5:** **ninguno.** Hace falta 2,0% de tasa y el más frecuente tiene
  0,711%. Los brazos de H=5 se leen como **"no se pudo medir"**, no como ceros.
- **A 1h:** la frontera de H=4 es 0,050%, muy por debajo de cualquiera de las cinco tasas.
  **Por eso 1h es la resolución PRIMARIA.**

**Lo que esto significa, dicho sin adornos:** §2.1 **no muere por potencia**, que es lo que
esta compuerta existía para averiguar. A diferencia de la corrida 8 (murió por n) y la 9
(murió por σ), acá el instrumento alcanza —**en los horizontes cortos**—. Si esta familia da
cero ahí, va a ser **un cero medido**.

> **Y la compuerta ya pagó su costo:** dijo, antes de estimar nada, que **los brazos de
> H=5 a 1d no van a poder concluir**. Sin ella, ese resultado se habría leído como un cero
> más, y no lo es.

---
---

# §2. EL DETECTOR — parámetros fijados ANTES de estimar

**Cómo se eligieron, y por qué eso importa.** Los valores de abajo se escribieron antes de
calcular **cualquier** efecto. Lo único que se midió con ellos fue la **tasa de disparo**
de §1.3.

> **Y no se tocaron después de verla.** Si se hubieran ajustado para subir la tasa —y así
> mejorar el MDE— la corrida no valdría. Quedan como están, `hch_inv` incluido, aunque su
> tasa lo deje al borde.

| parámetro | valor | qué controla |
|---|---|---|
| `K` | **3** | semiventana del pivote: máximo/mínimo sobre [i−3, i+3] |
| `TOL` | **3%** | dos techos son "iguales" si difieren ≤ 3% |
| `PROF` | **3%** | el valle entre los dos techos tiene que estar ≥ 3% abajo |
| `HOMBRO` | **5%** | los dos hombros del HCH difieren ≤ 5% |
| `MIN_SEP` | **5 barras** | separación mínima entre los dos pivotes del par |
| `MAX_SEP` | **60 barras** | del primer pivote a la ruptura |
| `CUNA_N` | **3** | pivotes por lado para el triángulo |

## 2.1 Los cinco patrones y su DIRECCIÓN, declarada antes

| patrón | qué es | **dirección declarada** |
|---|---|---|
| `doble_techo` | dos máximos parecidos con valle en medio; rompe el valle | **corto** |
| `doble_piso` | el espejo | **largo** |
| `hch` | tres máximos, el del medio más alto; rompe el cuello | **corto** |
| `hch_inv` | el espejo | **largo** |
| `triangulo` | máximos que bajan y mínimos que suben; rompe cualquier lado | **ninguna** (se miden las dos) |

> **Se declara antes por lo que midió la corrida 7:** 3 de sus 5 mejores brazos estaban
> **invertidos** respecto de lo que el patrón afirma, y eligiendo el signo después los
> cinco contaban como aciertos.

## 2.2 Sin mirar el futuro — lo único delicado del detector

Un pivote centrado en la barra `i` **recién se confirma en `i+K`**: hasta entonces no se
sabe si el máximo local aguanta. **Todo patrón usa solo pivotes con índice ≤ j−K cuando
decide en la barra j**, y la máscara se marca **en la barra de ruptura**, que es donde se
podría entrar de verdad. La entrada es al cierre de esa barra.

---

# §3. EL ESTIMADOR

Se reusa **`correr_velas.py` tal cual** — es literalmente lo que el handoff dice que hay
que hacer: *"solo hay que reemplazar `velas.patrones(df)` por el detector nuevo"*.

```
exceso(t) = media(y | simbolos donde disparo en t) - media(y | universo de t)
semana(w) = media de exceso(t) sobre las barras de w
estadistico = media de semana(w), cada semana pesando UNO
```

**Control POR BARRA**, no por símbolo: los patrones bajistas disparan más en días bajistas
del mercado entero, y aparear por símbolo no toca ese término.

- **Universo:** `base200` menos las 21 que no son cripto (regla de clase de activo).
- **Ventana:** 2021-08-01 → 2026-08-01, la misma de la corrida 7.
- **Resoluciones:** **1h** (primaria) y **1d**.
- **Horizontes:** los de `PARAMS` — 1h: 4 y 24; 1d: 1, 3 y 5.
- **Dos costos:** 0,20% y 0,50%.
- **El p que decide es el de BLOQUES semanales.** El n efectivo son las semanas.
- **`sin_top3` y `sin_top1`** en cada brazo.
- **FDR q=0,10 sobre el LOTE ENTERO** de cada resolución.
- **"No se pudo medir"** si el brazo tiene < 200 disparos o < 20 semanas.

---

# §4. LOS CONTROLES — y el que el handoff exige

Un patrón de gráfico puede no ser más que "el precio rompió un mínimo reciente". Los
controles están para separar **la estructura** del **breakout pelado**.

1. **`CTRL azar`** (×3) — máscaras al azar a la tasa mediana de los cinco patrones. Da el
   MDE con la nula real.
2. **`CTRL ruptura simple`** (×2) — cierre por debajo del mínimo de las últimas `MAX_SEP`
   barras, y su espejo. **Sin ningún pivote, sin ninguna tolerancia, sin estructura.**
   > **Si un patrón no se separa de esto, lo que detectó es un breakout, no una figura.**
3. **`CTRL pivotes barajados`** (×5) — el control que pide el handoff: **los mismos
   parámetros y la misma lógica de ruptura, pero con los pivotes reemplazados por índices
   al azar** de la misma cantidad y la misma separación mínima.
   > **Si el patrón real no se separa de su versión con la estructura destruida, lo que se
   > detectó es ruido con forma.**

**Los controles entran al FDR igual que los patrones.**

---

# §5. LA REGLA DE PARADA

> Un brazo **sobrevive** solo si cumple **todas**: exceso > 0 a los **dos** costos; signo
> crudo igual al normalizado; **fuera del MDE del azar**; **p de bloques** que pasa FDR
> q=0,10 sobre el lote entero; **`sin_top3` mantiene el signo**; y **supera a `CTRL ruptura
> simple` y a `CTRL pivotes barajados` del mismo patrón**.

> **Si ningún brazo sobrevive, el veredicto es CERO MEDIDO**, no "no se pudo medir" — la
> compuerta de §1 ya estableció que la potencia alcanza. **Esa distinción se hace explícita
> en los resultados**, porque es justo lo que las corridas 8 y 9 dejaron escrito que hay
> que decir.

> `hch_inv` a **1d** entra con la etiqueta **"en el borde"** puesta de antemano (MDE 0,1002
> contra un umbral de 0,10). A 1h no tiene esa marca.

---

# §6. LO QUE ESTA CORRIDA NO HACE

- **No barre parámetros del detector.** Un solo juego, el de §2. Barrer tolerancias es
  precisamente la máquina de fabricar falsos positivos que el handoff señala.
- **No agrega patrones después.** Los cinco de §2.1 y nada más.
- **No mira perpetuos ni on-chain.** Spot, OHLC, y nada más.

---
---

# RESULTADOS DE LA CORRIDA 12

_(debajo de esta linea, despues de correr)_
