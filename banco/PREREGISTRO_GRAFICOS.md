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

> Corrido el **2026-08-29**. `banco/correr_graficos.py --tf 1d` (161 s) y `--tf 1h` (2.487 s).
> **180 brazos a 1d**, **120 a 1h**. 136 pares a 1d, 142 a 1h, 2021-08-01 → 2026-08-01.

## VEREDICTO: **CERO. Y es un cero MEDIDO.**

**Ningún brazo sobrevive, en ninguna de las dos resoluciones. Ninguno pasa FDR.** La
décima familia queda cerrada, y con la compuerta de §1 corrida antes, **esto no es un
"no se pudo medir"**: en los horizontes que la compuerta habilitó, la potencia estaba.

---

### R1. Lo que la estructura NO agrega: el control pelado le gana a todo

Ésta es la tabla que cierra la familia. Excesos **antes de costos**, despejados
exactamente de las dos corridas de costo (el término de costo es lineal, así que
`e(0) = e(0,20) + 0,20·(e(0,20) − e(0,50))/0,30`):

| resolución | el mejor de… | exceso pre-costo |
|---|---|---|
| **1h** | **`CTRL ruptura arriba` — sin pivotes, sin tolerancias, sin figura** | **+0,2532** |
| 1h | el mejor patrón de gráfico real | +0,1545 |
| 1h | el mismo detector con **pivotes barajados** | +0,0758 |
| 1h | máscara al azar | +0,0291 |

> **Una ruptura pelada del máximo de 60 barras le gana a los cinco patrones de gráfico,
> antes y después de costos.** Y el detector con la estructura destruida —mismos
> parámetros, pivotes al azar— llega a +0,0758 contra +0,0652 del `doble_piso` real al
> mismo horizonte.
>
> O sea: lo poco que los patrones detectan **es el breakout**. La geometría —los dos
> techos iguales, el cuello, los hombros simétricos— **no aporta nada medible.**

Después de costos la conclusión es la misma pero más aburrida: a 1d el mejor brazo real
es `doble_techo` corto a H=5 con **+0,0577** contra un MDE de **0,1561**; el control de
ruptura simple da **+0,0569** —prácticamente lo mismo— y una **máscara al azar** llega a
**+0,0823**, más que cualquier patrón.

---

### R2. El brazo que casi engaña, y las tres cosas que lo matan

El resultado más tentador de toda la corrida:

| | |
|---|---|
| brazo | **`hch_inv`, objetivo CORTO, 1d, H=5** |
| exceso pre-costo | **+0,2158** |
| `p` de bloques | **0,0245** a H=3, **0,0450** a H=5 |
| `sin_top3` | **+0,1242** — *sobrevive sacar 3 símbolos* |

Un p de 0,0245 que aguanta `sin_top3`. **Y está muerto por tres razones escritas antes de
mirarlo:**

1. **Es la dirección INVERTIDA.** `hch_inv` —hombro-cabeza-hombro invertido— es un patrón
   **alcista**: su dirección declarada en §2.1 es **largo**. Este brazo gana yendo
   **corto**. Elegir el signo después convierte el hallazgo en su opuesto.
2. **La compuerta de §1 ya lo había descalificado.** `hch_inv` dispara al **0,182%**, por
   debajo de la frontera de 0,200% a 1d. Su MDE **a su propia tasa y a H=5 es 0,2245**, y
   su exceso es **0,2158**: **está adentro de su propia banda de ruido.** Eso se sabía
   antes de correr el efecto.
3. **No pasa FDR** sobre el lote entero (180 brazos a 1d).

**Los tres filtros lo matan por separado.** Y el más limpio es el segundo, porque el
número que lo mata se calculó **antes** de que este brazo existiera.

---

### R3. Una máscara al azar con p = 0,0495

`CONTROL azar 2`, objetivo largo, 1d, H=5: exceso pre-costo **+0,1103**, `p` de bloques
**0,0495**, y **`sin_top3` +0,0475** — o sea que también aguanta sacar tres símbolos.

**Es ruido puro con p < 0,05 y resistente a la compuerta de concentración.** Con 180
brazos, que uno al azar cruce 0,05 es exactamente lo esperable. Por eso el FDR va **sobre
el lote entero** y no brazo por brazo, y por eso los controles entran al lote.

> Si esta corrida se hubiera hecho mirando un patrón por vez, habría "encontrado" dos
> cosas: un HCH invertido que funciona al revés y una máscara aleatoria.

---

### R4. La inversión costo/ruido entre resoluciones — lo que corrige a la compuerta

El término de costo, medido en ATR (es el mismo despeje lineal de R1):

| resolución | costo a 0,20% | costo a 0,50% | MDE (horizonte corto) | **qué manda** |
|---|---|---|---|---|
| **1d** | **0,0292 ATR** | 0,0730 | **0,0547** | **el ruido** |
| **1h** | **0,1548 ATR** | 0,3869 | **0,0289** | **el costo, 5,4×** |

**A 1h el costo de una vuelta es cinco veces el mínimo detectable.** La compuerta de §1
decía que 1h era la resolución primaria porque tiene 25× más barras y por lo tanto menos
ruido — y es cierto, pero **incompleto**: bajar el ruido no sirve si el piso de costo sube
más rápido. Un ATR horario es ~5× más chico que uno diario, así que el mismo 0,20% pesa
~5× más en esas unidades.

> **La resolución que conviene no es la que tiene más barras: es aquella donde el efecto
> buscado es grande comparado con `max(ruido, costo)`.** A 1h esta corrida solo podría
> haber encontrado un efecto **mayor a 0,155 ATR**, o sea 5× más grande que lo que su
> propio ruido permitía detectar. **Eso se puede calcular antes**, y la compuerta de §1 no
> lo hacía. Es lo que le falta a esa compuerta y hay que agregárselo a la próxima.

---

### R5. Las tasas de disparo a 1h, que la compuerta había pedido

| patrón | tasa 1h | disparos | frontera H=4 |
|---|---|---|---|
| `doble_techo` | 0,756% | 30.135 | 0,050% ✔ |
| `doble_piso` | 0,654% | 26.058 | ✔ |
| `hch` | 0,457% | 18.202 | ✔ |
| `hch_inv` | 0,427% | 17.000 | ✔ |
| `triangulo` | 0,192% | 7.663 | ✔ |

**Los cinco pasan con holgura a 1h**, y ningún brazo cayó en "no se pudo medir" (0 de 120)
— contra 24 de 180 a 1d. La compuerta acertó en eso.

---

### R6. Lo que queda dicho, y lo que no

**Dicho:** con este detector, estos siete parámetros, cinco figuras clásicas, dos
resoluciones y cinco horizontes, **no hay dirección**. Y el mecanismo del cero está
identificado: lo que los patrones capturan es el **breakout**, que ya se mide solo y mejor
sin ninguna geometría.

**NO dicho:** que ninguna implementación posible de "hombro-cabeza-hombro" funcione. Los
patrones de gráfico son subjetivos por construcción y otro juego de tolerancias detecta
otro conjunto. Lo que sí quedó medido es que **la estructura no aporta sobre la ruptura**
—control de pivotes barajados y control de ruptura simple, los dos—, y ése es un resultado
sobre la **forma funcional**, no sobre un juego de umbrales.

> **Y el camino para reabrirlo tiene un peaje escrito:** barrer tolerancias es la máquina
> de fabricar falsos positivos que el handoff señala. Quien lo intente tiene que
> preregistrar la grilla completa **antes** y meterla entera en el FDR — o el resultado no
> vale. Con 180 brazos ya apareció una máscara aleatoria con p = 0,0495.

