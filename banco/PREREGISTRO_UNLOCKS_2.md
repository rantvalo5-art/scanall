# PREREGISTRO 2 — unlocks, dosis-respuesta como TENDENCIA CONTINUA

> ## ESTE PREREGISTRO NO ES CIEGO. LEER ESTO PRIMERO.
>
> La corrida 1 (`PREREGISTRO_UNLOCKS.md`) ya se hizo y **ya vi sus numeros**. Se que el
> bucket 2-5% dio **+5,3pp** contra el pareado, o sea **en contra** de H original, y que
> las cuatro hipotesis con efecto visible dieron vuelta el signo al sacar el top-3.
> Mi prior esta contaminado y presentar esto como escrito a ciegas seria mentir.
>
> Consecuencias, asumidas explicitamente:
>
> 1. **El test es TWO-SIDED.** No se predice el signo. H original decia beta<0
>    (mas desbloqueo -> peor); lo poco medido apunta a beta>0. Se declara empate y
>    decide el dato.
> 2. **Un beta>0 significativo NO es un hallazgo promovible aca.** Seria una hipotesis
>    NUEVA sugerida por la corrida 1, y para promoverla hace falta la confirmacion por
>    epoca de la seccion 8, que es la unica parte de esto que sigue siendo ciega.
> 3. Este preregistro **no puede resucitar** la regla de parada de la corrida 1. Aquella
>    quedo inadjudicable y asi queda. Esto es una pregunta distinta sobre la misma muestra.

Escrito el **2026-08-25**, despues del conteo de potencia y **antes de calcular una sola
pendiente**. Los conteos de las secciones 2 y 6 se corrieron primero a proposito: es la
regla nueva que salio del error de la corrida 1.

---

## 1. Por que un test de tendencia y no otro corte

La corrida 1 murio por planificacion, no por resultado: la regla de parada se apoyaba en
el bucket `>=10%`, que post-join quedo con **n=143** contra una compuerta de n>=200.
Cuatro de cinco buckets subpotenciados.

La causa es estructural, no mala suerte: **cortar 1.040 eventos en cinco buckets deja
~200 por bucket**, y la decision se apoyaba en el mas chico. Un test de tendencia usa
**toda la muestra para estimar un solo parametro**. Esa es la unica salida que no pasa por
re-cortar los buckets, que esta prohibido.

**Lo que NO se hace:** re-cortar, mover el corte de 0,5%, cambiar horizonte o barreras.
La poblacion es exactamente la de la corrida 1.

---

## 2. La muestra, contada POST-JOIN (esto es lo que falto la vez pasada)

Corrido antes de escribir la regla de parada, sin mirar ningun resultado:

| paso | quedan |
|---|---|
| eventos `cliff` agregados por (simbolo, dia), pct calculable | 13.097 |
| `pct >= 0,5%` del circulante | 2.431 (115 pares) |
| pares con panel horario 2021-01-01 -> 2026-08-01 y >=1500 velas | 111 pares, 2.319 eventos |
| entradas de primer toque marcadas por un desbloqueo (ventana 12h) | **1.051** |
| **marcadas Y resueltas (la muestra del test)** | **1.036, sobre 78 simbolos** |

Contexto de la tabla de primer toque completa: 229.349 entradas, 96,4% resueltas.

**Dosis (`pct` = tokens del evento / circulante ese dia):**

| p0 | p10 | p25 | p50 | p75 | p90 | p100 |
|---|---|---|---|---|---|---|
| 0,50% | 0,83% | 1,41% | 2,80% | 5,82% | 12,95% | 332,80% |

**Concentracion, contada antes:** mediana de 10 eventos por simbolo, maximo 56. El top-3
(`CELOUSDT`, `GMTUSDT`, `SUIUSDT`) es el **15,1%** de la muestra.

**Pool pareado:** cada uno de los 78 simbolos tiene **>=155** entradas no-evento resueltas
(mediana 1.617). Con cualquier umbral de pool hasta 100 **no se cae ningun simbolo**. La
linea base por moneda esta bien estimada; no es un problema de este test.

**Dispersion temporal:** 23 trimestres (mediana 34 eventos, minimo 12); 12 semestres
(mediana 70, minimo 14). Por epoca: 2021-2023 = 313, 2024-2026 = 723.

**Tasa de resolucion por quintil de dosis** (es una tasa de resolucion, no un win rate):
0,5% / 4,3% / 1,0% / 1,0% / 0,5% sin resolver. No hay censura diferencial por dosis, asi
que descartar los timeouts no sesga la pendiente.

---

## 3. La especificacion. UNA sola.

Todo lo que no se nombra aca es lo mismo que la corrida 1 y no se toca.

- **Poblacion**: los 1.036 eventos de la seccion 2. Barreras +-8%, horizonte 14 dias,
  paso 12h, entrada en el cierre de la ultima vela horaria CERRADA (offset -1),
  costo 0,20%.
- **Resultado por evento**, ya pareado a nivel observacion:

  ```
  y_i = win_i - p_base(simbolo_i)
  ```

  `win_i` en {0,1} (primer toque resuelto). `p_base(s)` = win rate de las entradas de **s**
  que **no** son evento. Restarle a cada evento la linea base de su propia moneda ES el
  control pareado del preregistro 1, hecho por observacion en vez de por agregado. Sin eso
  el test mide "las alts con vesting bajan", que es un hecho de la muestra.
- **Regresor**: `x_i = log10(pct_i)`, **winsorizado en [p1, p99] = [0,51% , 100,00%]**.

  > La winsorizacion se declara ACA y por una razon mecanica, no de resultado: hay 15
  > eventos con `pct >= 100%` y el maximo es **332,80%** del circulante. En OLS una sola
  > observacion a 2,5 desvios de palanca puede fijar la pendiente sola. Afecta a 21 de
  > 1.036 eventos y le saca 0,003 al desvio de x (0,490 -> 0,487): es proteccion contra
  > apalancamiento, no un recorte de muestra. La version **sin** winsorizar se reporta
  > siempre como diagnostico.
- **Estadistico primario**: la pendiente OLS **beta** de `y` sobre `x`.
  Unidad: **puntos porcentuales de win rate por decada de dosis** (x10 en el desbloqueo).
- **Direccion**: **two-sided**, por la contaminacion del encabezado.

---

## 4. Inferencia. El p que decide es el remuestreado.

El p de OLS supone entradas independientes y aca es falso (se solapan, el regimen esta
autocorrelacionado, y hay hasta 56 eventos del mismo simbolo). Se reporta como referencia
y **no decide** — es exactamente la brecha p_indep/p_bloques que este banco ya identifico
como el autoengano.

Tres nulas, las tres corridas:

1. **Permutacion de la dosis DENTRO de cada simbolo** (3.000 reps) -> el **p primario**.
   Rompe el vinculo dosis->resultado conservando el nivel de cada moneda y su estructura
   temporal. Es la nula correcta para una pregunta de dosis.
2. **Bootstrap de simbolos enteros** (2.000 reps) -> IC del 95%. Es la respuesta directa a
   lo que mato la corrida 1: si el efecto vive en pocos nombres, este IC cruza cero.
3. **Bootstrap de bloques trimestrales** (23 bloques, 2.000 reps) -> IC del 95%.
   **Reemplaza a la compuerta semanal de `lote.py`**, que es estructuralmente inaplicable
   a eventos esparcidos: `SEM_N_MIN=20` con ~3,5 eventos/semana deja todas las semanas
   afuera y la compuerta sale `--`. Ese defecto ya se documento; aca se esquiva por
   diseno, no se afloja.

---

## 5. Regla de parada, exacta

**La familia se declara MUERTA salvo que se cumplan TODAS:**

1. `p` de la permutacion intra-simbolo (two-sided) **< 0,05**;
2. el IC 95% del **bootstrap de simbolos** no contiene 0;
3. el IC 95% del **bootstrap trimestral** no contiene 0;
4. el **signo de beta se conserva** sacando los 3 simbolos con mas eventos
   (`CELOUSDT`, `GMTUSDT`, `SUIUSDT`) **y** sacando solo el primero;
5. el **signo de beta coincide** en 2021-2023 (n=313) y en 2024-2026 (n=723);
6. el **placebo pasa**: repitiendo todo con los eventos corridos **-30 dias** (una fecha
   donde no hubo desbloqueo), `|beta_placebo|` queda **por debajo** de `|beta|`.

Cualquiera que falle cierra la familia. Un p lindo con el signo dado vuelta sin el top-3
**no cuenta** — es justo el modo de muerte de la corrida 1.

**Si beta no es distinguible de cero**, el veredicto es *no hay dosis-respuesta de este
tamano o mayor*, y eso **si es adjudicable** gracias a la seccion 6. No es "subpotenciado".

---

## 6. Potencia, calculada ANTES (lo que la corrida 1 no hizo)

Desvio de la pendiente bajo cada nula, medido con la muestra real (solo la **nula**; la
pendiente observada no se calculo para escribir esto):

| nula | sd(beta) | efecto minimo detectable (2 sigma) |
|---|---|---|
| permutacion intra-simbolo | 0,0237 | **4,7pp por decada** |
| bootstrap de simbolos | 0,0332 | **6,6pp por decada** |
| bootstrap trimestral | 0,0317 | **6,3pp por decada** |

El rango util de dosis es de **2,29 decadas** winsorizado, asi que el MDE de punta a punta
—del desbloqueo mas chico al mas grande— es de **~15pp de win rate**.

Y el estimador tiene de donde agarrarse: **el 51% de la varianza de `log10(pct)` es
INTRA-simbolo** (sd 0,350 de 0,489). La pendiente no se identifica comparando monedas
distintas sino comparando desbloqueos chicos contra grandes **de la misma moneda**, que es
justo lo que la hace inmune al shock comun por simbolo — el mecanismo que mato la corrida 1.

**Este es el numero que decide si el test valia la pena, y esta escrito antes.** Con
n=143 la corrida 1 no lo tenia; con 1.036 y un solo parametro, si.

---

## 7. Secundarios: declarados ahora, se reportan siempre, NO deciden

Se listan para que despues no se pueda elegir el que salio lindo:

- **Poblacion ampliada `pct >= 0,1%`**: n=2.303 sobre 83 simbolos, sd(x)=0,699 (mas
  palanca). **Pero el top-3 pasa a ser el 28,3%** de la muestra, casi el doble de
  concentracion, por eso es secundario y no primario.
- **Spearman** entre `y` y `pct` (libre de forma funcional), con la misma permutacion.
- **Pendiente por categoria** (`insiders` n=273, `noncirculating` n=238, `privateSale`
  n=237, `farming` n=93, `ecosystem` n=70). Ninguna decide sola: **si la poblacion
  completa no cruza, una categoria que cruza es una celda de un lote de cinco.**
- **beta sin winsorizar**, para ver cuanto movian los 21 extremos.

---

## 8. Si algo cruza (la unica parte que sigue siendo ciega)

Confirmacion **por epoca**, como declaro la corrida 1: descubrimiento en **2021-2024**,
confirmacion en **2025-2026**. Ya estaba escrito de antes y no se cambia.

Y una compuerta aparte, que **no** gatea el hallazgo cientifico pero **si** gatea cualquier
promocion a operar: el decil superior de dosis tiene que **cruzar el win rate necesario**
(`(8 + 0,20) / 16 = 51,25%`). Una pendiente real puede ser toda "los chicos andan mejor"
sin que ningun extremo sea operable. Son dos preguntas distintas y se contestan por
separado.

---

## 9. Los sesgos, sin cambios respecto de la corrida 1

1. **No es point-in-time**: el calendario es el snapshot de hoy.
2. **Supervivencia, y va en contra de H original**: solo estan los protocolos que
   DefiLlama trackea hoy; el que se murio tras un desbloqueo grande no esta. Censura los
   peores resultados. **Ojo: ese mismo sesgo empuja beta hacia arriba**, o sea *a favor*
   de la direccion que la corrida 1 insinuo. Con el prior contaminado apuntando al mismo
   lado, un beta>0 es el resultado que mas escepticismo merece, no el que menos.

---

## 10. Lo que NO se permite, escrito antes

- re-cortar buckets, mover el corte de 0,5%, cambiar horizonte, barreras o ventana de 12h;
- cambiar la winsorizacion despues de ver la pendiente;
- promover un secundario si el primario no cruza;
- reportar una categoria sola;
- convertir un p entre 0,05 y 0,10 en "sugestivo": la regla dice 0,05.

---

# RESULTADOS (2026-08-25)

> Nada de lo de arriba se toco. Corrida: `py -3.13 -u test_unlocks.py --tendencia`
> (log en `unlocks_tendencia.log`, eventos en `unlocks_tendencia.csv`).

## Veredicto: FAMILIA CERRADA

**4 de 6 compuertas pasan. Las dos que fallan son las que deciden significancia.**

| # | compuerta | resultado | |
|---|---|---|---|
| 1 | p permutacion two-sided < 0,05 | **p = 0,1093** | FALLA |
| 2 | IC95 bootstrap de simbolos no contiene 0 | **[-12,76 , +0,83]pp** | FALLA |
| 3 | IC95 bootstrap trimestral no contiene 0 | [-10,23 , -0,65]pp | ok |
| 4 | el signo aguanta sin top-3 y sin top-1 | -3,93pp / -5,74pp | ok |
| 5 | el signo coincide en las dos epocas | -1,25pp / -7,53pp | ok |
| 6 | el placebo -30d queda por debajo del real | -0,77pp vs -5,91pp | ok |

**Pendiente: beta = -5,91pp de win rate por decada de dosis** (n=1.036, 78 simbolos,
23 trimestres), o **-13,5pp de punta a punta** sobre las 2,29 decadas utiles.

Y esta vez el cierre **no es "subpotenciado"**. La seccion 6 fijo el MDE en 6,6pp por
decada antes de mirar; el efecto observado (5,91) quedo **justo debajo** de ese umbral.
La respuesta es: *no hay dosis-respuesta de 6,6pp por decada o mayor*. Es una respuesta,
no un "no se pudo medir".

## Lo mas importante del resultado: el prior contaminado apuntaba al reves

El encabezado declaro que mi prior estaba sucio porque la corrida 1 mostraba el bucket
2-5% en **+5,3pp**, o sea *contra* H original. **El test de tendencia dio negativo**: mas
desbloqueo, peor rendimiento — exactamente lo que H original predecia.

No es contradiccion, es lo que pasa cuando se cambia de estimador. La corrida 1 comparaba
**buckets entre monedas**; la tendencia se identifica **51% dentro de cada moneda**
(seccion 6). Un bucket suelto de 341 eventos comparado contra un agregado no es lo mismo
que la pendiente de toda la muestra.

**La leccion practica: el bucket que "apuntaba en contra" y contamino el prior era ruido
de composicion.** Escribir el test two-sided fue lo que salvo la corrida — con un
preregistro one-sided en la direccion sugerida por la contaminacion, este resultado se
habria leido como refutacion cuando en realidad apunta al lado original.

## Por que igual se cierra, y no es una decision cobarde

Las tres firmas que uno querria ver estan: aguanta concentracion (era lo que mato la
corrida 1), coincide en las dos epocas, y el placebo a -30 dias da practicamente cero
(-0,77pp, p=0,76 — el control negativo funciona, lo cual valida el cableado).

Lo que no esta es **potencia contra la variacion entre monedas**. El IC de simbolos
[-12,76 , +0,83] toca cero; el trimestral [-10,23 , -0,65] no. La diferencia entre los dos
es la respuesta: el efecto es consistente **en el tiempo** pero no lo suficiente **entre
nombres**. Con 78 simbolos y un efecto de ~6pp por decada, hace falta mas muestra — y
**la muestra no existe**: 1.040 eventos usables es el techo de la data (478 pre-listado,
913 futuros; ver corrida 1).

La escalera por decil dice lo mismo, y no es monotona:

| decil | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| dosis mediana | 0,68% | 0,92% | 1,42% | 1,88% | 2,45% | 3,17% | 4,29% | 5,83% | 9,40% | 25,81% |
| excedente (pp) | +2,8 | +6,9 | +6,3 | -1,5 | +2,8 | +5,5 | +7,7 | -4,7 | -8,9 | -2,0 |

Los siete primeros deciles son mayormente positivos y los tres ultimos negativos, pero
salta. La pendiente es real como resumen; la escalera no es limpia.

## Secundarios (se reportan porque estaban declarados, no porque salgan bien)

- **Poblacion ampliada >=0,1%** (n=2.303): beta = **-0,59pp**, p=0,55, los dos IC cruzan
  cero. Los eventos chicos no aportan senal: agregar 1.267 eventos de dosis baja **diluye**
  la pendiente en vez de precisarla, que es consistente con que el efecto (si existe) viva
  solo en los desbloqueos grandes.
- **Spearman** = -0,0743. Mismo signo, magnitud despreciable.
- **beta sin winsorizar** = -5,96pp contra -5,91pp winsorizado. Los 21 extremos no movian
  nada; la winsorizacion fue seguro barato, no un recorte con efecto.
- **Por categoria**: `insiders` **+13,15pp** contra `privateSale` **-15,42pp**, con
  `noncirculating` -5,98, `ecosystem` -14,99, `farming` -7,76. **Signos opuestos entre
  celdas de tamano parecido es la firma del ruido**, no de un mecanismo. Se dijo antes que
  ninguna decide sola y no decide.

## La compuerta de operable, y lo que NO se hace con ella

El decil superior de dosis (n=104, >=13,0% del circulante) da **46,15% en largo** contra
51,25% necesario: no cruza. Su complemento en corto da **53,85%**, que nominalmente si
cruza.

**Eso no reabre nada, y queda escrito por que:**

1. el primario fallo, y la seccion 10 prohibe promover cuando el primario no cruza;
2. son **0,53 sigma** por encima del umbral (se binomial 4,90pp con n=104) — o sea, ruido;
3. el sesgo de supervivencia declarado en la seccion 9 censura justo los peores resultados,
   asi que la cola corta de la muestra esta sistematicamente subestimada... **en la
   direccion que haria ver mejor al corto**. Un corto que apenas cruza sobre una muestra
   que censura las quiebras no es un hallazgo.

## Que hizo falta para que esto fuera adjudicable

La corrida 1 fallo por contar el n **antes** del join. Esta corrida conto primero
(seccion 2), calculo el MDE con la nula real **antes** de estimar nada (seccion 6), y
cambio el estimador para usar toda la muestra en un parametro. Resultado: un veredicto
en vez de un "no se pudo".

**La familia unlocks queda cerrada.** No por falta de efecto aparente —el signo es el
correcto y aguanta concentracion y epoca— sino porque el efecto es **mas chico que lo que
1.040 eventos pueden distinguir de cero**, y esos 1.040 son todos los que hay.
