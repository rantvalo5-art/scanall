# PREREGISTRO — corrida 13: el último hueco declarado, horizontes > 1 semana

> Escrito el **2026-08-30**, **antes de correr una sola configuración**.
> Código: `banco/horizonte_largo.py`. Resultados **debajo de la línea**.

---

## 1. Por qué existe este hueco, y por qué es el único que queda

El veredicto de la corrida 4 está escrito así, textual:

> *"Dirección: 0 de 4.140 brazos… Por la regla 1 la familia queda cerrada — ranking
> transversal top-k sobre precio, flujo del kline y posicionamiento de futuros,
> **a horizontes de 4h a 7d**."*

**Nada se probó más allá de una semana.** El handoff lo declara ("horas a semanas"), así que
no está escondido — pero después de doce corridas es el único lugar del mapa donde la
respuesta es *"no se probó"* en vez de *"no está"*.

Y no es un rincón arbitrario: en la literatura, el momentum transversal se define a
**1-12 meses** de formación y tenencia. Este repo lo midió a horas y días.

---

## 2. Lo que se mueve a favor, y lo que se mueve en contra

**A favor — el costo por unidad de tiempo cae con el horizonte.** El término de costo del
harness es `(costo/100) / atr_base`, y `atr_base` es la mediana móvil de 30d del **ATR de
24h**: no depende del horizonte. O sea que **el costo por rebalanceo es constante**, pero
se paga 365 veces al año a 24h y **12 veces** a 30d.

Eso es exactamente lo contrario del problema que mató a 1h en la corrida 12, donde el costo
era 0,155 ATR contra un ruido de 0,029.

**En contra — el n se derrumba.** Con `paso = horizonte` (sin solape) sobre 5 años:

| horizonte | rebalanceos no solapados en 5 años |
|---|---|
| 168h (7d) | ~260 |
| **720h (30d)** | **~60** |
| **2160h (90d)** | **~20** |

Cuál de los dos gana **no lo sé, y no lo voy a estimar de memoria** — el error de la
corrida 12 fue justamente comparar ruido y costo en unidades distintas. Se mide.

> **Corrección de algo que dije antes de escribir esto:** en la conversación predije que
> esta compuerta iba a fallar "por n, como la corrida 8". Esa predicción la hice sin hacer
> la cuenta del costo por unidad de tiempo, que empuja para el otro lado. **La dejo
> registrada igual** —una predicción hecha antes no se borra porque después convenga— pero
> con menos confianza de la que le puse.

---

## 3. La unidad: retorno ANUALIZADO en %, no ATR por rebalanceo

Comparar un MDE en "ATR por tenencia" entre 24h y 90d no significa nada: la misma unidad
mide cosas distintas. Se convierte todo a **% anualizado**:

```
% por rebalanceo   = spread_en_ATR x atr_base_mediana(%)
% anualizado       = % por rebalanceo x (8760 / horizonte_en_horas)
```

Es la misma unidad en la que se decidió la corrida 8, y la única que se puede comparar
contra un piso de stablecoins.

---

## 4. La regla de parada

> **Se corre `--nula` (solo controles al azar) a horizonte 168h, 720h y 2160h**, con
> `paso = horizonte` para que las barras no se solapen, sobre **2021-08-01 → 2026-08-01**.
>
> **Si el MDE anualizado de un horizonte es > 10%/año, ese horizonte es "no se pudo
> medir"** y no se estima ningún brazo real ahí.
>
> **Si ningún horizonte largo (720h, 2160h) queda por debajo de 10%/año, el hueco se
> cierra como "no se pudo medir" y la dirección queda cerrada del todo.**

**Por qué 10%/año, y es el mismo umbral de la corrida 8:** es la escala en la que se decide
contra un piso de stablecoins de 4-5%/año y un costo de rebalanceo. Un efecto que no se
distingue de cero a ese nivel no cambia ninguna decisión aunque exista.

**168h es la CALIBRACIÓN, no una hipótesis.** Es el horizonte más largo donde la corrida 4
**sí concluyó** (0 de 4.140). Si el MDE anualizado a 168h también da > 10%/año, entonces el
umbral está mal calibrado para esta unidad y hay que decirlo, no fingir que los horizontes
largos son especiales. Es el mismo truco que en la corrida 8, donde BTC —con efecto conocido—
sirvió para mostrar que el problema era el estimador y no las alts.

---

## 5. Lo que esta corrida NO hace

- **No estima ningún brazo real.** Es solo la compuerta. Los rankings reales se corren
  después, y solo en los horizontes que la compuerta habilite.
- **No agrega features.** El mismo panel de la corrida 4.
- **No cambia el universo.** `base200`, con la regla de clase de activo.

---
---

# RESULTADOS

> Corrido el **2026-08-30**. `banco/horizonte_largo.py` (56 s).
> 187 pares, 2021-08-01 → 2026-08-01, `paso = horizonte` (sin solape), top-k=20.

## VEREDICTO: **NO SE PUDO MEDIR** — y alargar el horizonte resulta ser **un empate exacto**

| horizonte | barras | MDE ATR | **MDE %/año** | costo %/año | **BRUTO necesario** | |
|---|---|---|---|---|---|---|
| **168h (7d)** | 255 | 0,3062 | **22,0** | 10,43 | **32,4** | ← calibración |
| **720h (30d)** | 58 | 1,7957 | **30,2** | 2,43 | **32,6** | |
| **2160h (90d)** | 19 | 5,7671 | **29,8** | 0,81 | **30,6** | |

*(umbral preregistrado: 10%/año)*

---

### R1. La pregunta que motivaba la corrida tiene respuesta, y es "da igual"

La hipótesis era: **a horizonte largo el costo por unidad de tiempo cae**, así que quizás
compense la pérdida de n. Se midió, y la compensación es **exacta**:

- el costo cae **13×** — de 10,43%/año a 0,81%/año,
- la precisión cae **casi lo mismo** — el MDE sube de 22,0 a 29,8%/año,
- **el efecto BRUTO que haría falta para detectar algo se queda quieto en 30,6-32,6%/año.**
  Dispersión entre los tres horizontes: **6%.**

> **Alargar el horizonte no es una palanca. Es un empate.** Lo que se gana en costo se
> pierde en barras, y las dos cosas se mueven al mismo ritmo. Esa era la última perilla del
> mapa que no se había girado, y no gira nada.

---

### R2. El barrido de `k`, que es la única otra perilla

Antes de dar el veredicto se probó si el estimador estaba mal elegido para potencia — la
misma pregunta que la corrida 8 le hizo al pooling. Barriendo el tamaño de la cartera:

| k | BRUTO necesario a 168h | BRUTO necesario a 720h |
|---|---|---|
| 5 | 59,0% | 62,7% |
| 10 | 42,9% | 43,3% |
| **20** *(el preregistrado)* | **32,1%** | 32,6% |
| **40** | 32,1% | **26,4%** ← el mejor de todos |
| 60 | 34,4% | 27,8% |

**El `k=20` de las corridas anteriores estaba bien elegido a 7d y algo corto a 30d.** Pero
el mejor caso sobre las dos perillas juntas —`k=40` a 30 días— es **26,4%/año**, todavía
**2,6× el umbral**. La perilla existe, y no alcanza.

---

### R3. Lo que esto le corrige al repo entero, y es lo más importante de la corrida

**La calibración también falla, y ése era el punto de ponerla.** A **168h** —el horizonte
más largo donde la **corrida 4 SÍ concluyó**, con 0 de 4.140 brazos— el efecto bruto
detectable era **32,4%/año**.

> **Ese "0 de 4.140" descarta un efecto de ~32%/año bruto. NO descarta uno de 10%.**

No invalida nada: un cero es un cero, y ninguno de los 4.140 brazos se acercó. Lo que
cambia es **con qué precisión** hay que leerlo. Dicho como corresponde:

- ✔ **Lo que el repo sí estableció:** no hay un efecto transversal **grande** —del orden de
  30%/año bruto o más— en precio, flujo y posicionamiento, sobre las 200 más líquidas, a
  horizontes de 4h a 90d.
- ✘ **Lo que NO estableció, y hasta hoy sonaba como si lo hubiera hecho:** que no haya un
  efecto **modesto**. Un edge real de 8-15%/año habría sido invisible en las trece corridas.

**Y el motivo no es un defecto del método: es el largo de la muestra.** Con 5 años y
rebalanceo semanal hay 255 observaciones independientes; el error estándar de la media no
baja de ahí por más brazos que se prueben. La única cura es **más historia**, y eso llega a
razón de un año por año.

> **Esto NO es una invitación a seguir buscando.** Es lo contrario: dice que con los datos
> disponibles la pregunta "¿hay un edge modesto?" **no tiene respuesta alcanzable**, y que
> insistir con más features sobre la misma ventana no cambia el error estándar ni un poco.
> Lo que cambia el número es el tiempo.

---

### R4. Lo que quedó descartado en esta corrida

- **La predicción que había hecho antes de escribir el preregistro se cumplió, pero por la
  razón equivocada.** Dije "va a fallar por n, como la corrida 8". Falla, sí — pero no
  porque el n largo sea peor: **el bruto necesario es el mismo en los tres horizontes.**
  El n y el costo se cancelan. La predicción acertó el veredicto y erró el mecanismo, y eso
  se registra.
- **No se corrió ningún brazo real.** La compuerta no habilitó ningún horizonte, así que no
  hay ranking que estimar. Es exactamente para lo que se corre primero.

### R5. Qué lo reabriría

**Solo más historia, y hay una cuenta para saber cuánta.** El MDE va como `1/√n`. Para que
el bruto necesario a 30d baje de 26,4% a 10%/año haría falta `(26,4/10)² ≈ 7×` más
observaciones, o sea **~35 años** de mercado — o un universo mucho más ancho que
`base200`, que es la otra forma de sumar secciones cruzadas independientes… salvo que la
cola ilíquida ya se cerró por costo, y la corrida 8 midió que las cripto están correlacionadas
al **+0,92**, así que sumar nombres agrega mucho menos n del que parece.

