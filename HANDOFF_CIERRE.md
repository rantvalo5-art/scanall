# HANDOFF — Cierre de la búsqueda (2026-08-17)

> **Empezá por acá, no por `HANDOFF_SENALES.md`.** Ese sigue siendo válido y tiene el
> detalle ítem por ítem, pero su plan ya se ejecutó entero. Esto es el estado final.

---

## 1. Dónde quedó todo

El presupuesto de 4 sesiones de `HANDOFF_SENALES.md` se agotó. **Ocho líneas medidas, siete
cerradas, una viva y no concluyente.**

| ítem | veredicto | el número que lo cerró |
|---|---|---|
| 4.1 fadear el ranking | **CERRADO** — era composición | el score da −0,576pp/punto a 24h, pero **−0,178pp (t=−0,81)** sacando FADING, que está apagado en producción |
| 4.2 funding como sentimiento | **CERRADO** | +3,86pp con dosis-respuesta monótona, muere en bloques (p 0,155) y concentración (+1,8pp sin top-3) |
| 4.3 detectores de régimen | **CERRADO** por evidencia previa | batería de 7 ya medida, 0 pasan; el "ganador" no replicó sobre 22 trimestres (+0,09 → −0,08) |
| 4.4 vender volatilidad | **CERRADO** — se compitió | +20,96%/año en 5,3 años, pero el premio cayó de 16,6% a 9,9% de la prima → **+7,33% reciente, dentro del piso** |
| 4.5 funding entre exchanges | **sin correr** | prior bajo, duplica la superficie operativa, la diferencia también está competida |
| **4.7 fadear extensión** | **VIVO a 4h** | +0,550%, IC95 [+0,14%, +2,04%], p = 0,008 — **a 24h el IC cruza cero** |

**4.4 es el cierre más interesante del repo:** no era falso, era **tarde**. El premio de
varianza existía y era grande cuando el mercado de opciones cripto era inmaduro. Se arbitró.

---

## 2. Lo único pendiente — y es esperar, no trabajar

**4.7: fadear las señales de extensión (EXPLOSION y BREAKOUT) a 4h.**

Pasó sus cuatro compuertas escritas a tres niveles de costo, y después sobrevivió el
remuestreo por semanas, el corte de la ventana al medio, el filtro de "¿hay perpetuo?"
(77% de las alertas) y el fill realista a 15 minutos (cuesta 5-9% del edge).

**Pero son 51 días de un solo régimen bear**, y las dos mitades de la ventana son el mismo
bear. Eso no es out-of-sample de régimen, y la historia entera de este repo es que lo que
brilla en ventana corta bajista se muere después.

### El calendario, con fechas

El cuello **no son las alertas** (entran ~130 por semana) sino **las semanas**, porque es la
unidad independiente. Bajar más datos no acelera nada.

| cuándo | qué | para qué |
|---|---|---|
| **2026-10-19** (9 semanas) | correr `fade/evaluar.py` | **matar temprano.** Si la media a 4h se dio vuelta, se cierra sin esperar el resto |
| **2026-12-21** (18 semanas) | correr de nuevo | confirmación *si el efecto es tan grande como el medido* |
| **2027-12-13** (69 semanas) | correr de nuevo | confirmación *si el efecto real es la mitad* — que es lo normal |

```
cd fade && py -3.13 evaluar.py
```

> **Nada de capital hasta que aguante un tramo alcista.** Y la fila realista es la tercera:
> la primera medición de cualquier cosa casi siempre exagera, porque se encontró mirando, y
> lo que se encuentra mirando es la parte alta del ruido.

Refutar es mucho más rápido que confirmar. Por eso el chequeo de octubre vale la pena y los
otros dos son opcionales.

---

## 3. Lo que se construyó (y por qué importa más que los cierres)

### `banco/lote.py` — probar familias enteras por corrida

30 hipótesis por corrida, 450 con `--cruces`, en ~20 minutos. Seis compuertas cableadas en
el código, con veredicto por default **cerrada**: muestra ≥200 · cruza el umbral ·
Benjamini-Hochberg sobre el lote · gana contra la línea base **del mismo símbolo** ·
sobrevive sacar el top-3 y el mejor par solo · ≥60% de semanas.

**El resultado de la corrida grande es el número que justifica todo el aparato:**

| | |
|---|---|
| hipótesis con p < 0,05 **suponiendo independencia** | **68** |
| hipótesis con p < 0,05 remuestreando por semanas | **0** |

A una hipótesis por sesión, esas 68 habrían sido 68 "hallazgos" y más de un año de trabajo
para llegar al mismo lugar. Peor: **las 12 mejores son todas cruces de `mkt_vol_168 bajo`
con otra cosa** — no son 12 hallazgos, es **uno disfrazado doce veces**. Ese modo de falla
es invisible probando de a una.

### `opciones/iv_rv.py` — la cuenta de 4.4, re-corrible

Queda para re-chequear en un año. **Lo único que reabre el ítem** es que IV/RV vuelva
sostenido arriba de ~1,30 con el premio arriba del 15% de la prima.

### `fade/evaluar.py` — el forward test de 4.7 en un comando

---

## 4. Los dos errores de método de esta sesión — leer esto antes que nada

Son más valiosos que cualquier resultado, porque los dos son reincidentes.

**1. El bootstrap que pooleaba las alertas.** Remuestreaba bloques de semanas pero
concatenaba las alertas de cada bloque, así que las semanas con más alertas pesaban más
(iban de 66 a 199) y, con solo 8 semanas, hay 7 bloques distintos posibles — remuestrear de
ahí subestima la variabilidad. **Dio vuelta un veredicto:** IC [+0,17%, +2,32%] contra el
correcto **[−0,52%, +3,30%]**. Se reportó "p = 0,0000, sobrevive" y estaba mal.

> **La unidad independiente es la semana. Cada semana pesa uno.** Corregido en
> `banco/lote.py` y `fade/evaluar.py`.

**2. Medir la dirección equivocada de una hipótesis contraria.** En 4.2 se corrió primero el
lado **largo**: 0 de 26, listo para cerrar. Pero "funding positivo = longs amontonados =
reversión" dice **shortear**. Con barreras simétricas alcanza con dar vuelta el signo de
`res`. Cerrarlo ahí habría sido un **falso negativo**.

---

## 5. El hallazgo que explica todo el proyecto

**"Siempre hay una moneda que sube 30% por día" es cierto — y es una propiedad de la
varianza, no de la información.**

Sobre 168 monedas × 365 días: el 39,2% de los días hay al menos una moneda +30%, y la mejor
del día sube 23,7% en mediana. **Barajando los retornos** —lo que destruye todo patrón— el
fenómeno aparece **idéntico** (40,3% contra 39,2%). Es el **máximo de 168 sorteos**.

- Elegir una: P(+30%) = **0,308%** → 1 cada 324 intentos, o sea una vez cada ~0,9 años.
- Perseguir al ganador de ayer: **−0,554%** contra −0,197% de una al azar.
- El top de hoy repite mañana el **7,1%** de las veces contra 0,6% al azar — 12× más. *Se
  siente* predecible. Pero multiplica por **2,67×** la chance de volver a subir 10% y por
  **4,19×** la de caer 10%. **Lo que persiste es la volatilidad, y está inclinada abajo.**
- Diversificar sube la *probabilidad* de tocar una ganadora (3%/día con 10 monedas) y deja
  la *esperanza* exactamente igual (−0,202%). **Ganar todos los días requiere esperanza.**

Esto explica todos los cierres de una: el bot detectaba bien —el 83% de lo que elegiría un
oráculo— y perdía plata igual. **Detectar volatilidad no es detectar dirección.** Por eso
4.4 era el ítem con sentido: es el único negocio que paga por lo que sí se sabe medir.

---

## 6. Qué NO hacer

- **No reabrir 4.1, 4.2, 4.3 ni 4.4** sin una hipótesis nueva. Variantes de las mismas ya
  están medidas, y el archivo histórico tiene ~35 dead ends más
  (`~/.claude/projects/C--Users-asd-scancrypto-scanall/memory/`, 51 memorias — **chequearlo
  antes de proponer nada**; ya pasó que este handoff diera por no-probado algo enterrado).
- **No aflojar una compuerta de `lote.py` después de ver los números.** Es exactamente cómo
  se fabrica un falso positivo.
- **No creerle a un p-valor que supone independencia** en ningún dato de este repo.
- **No poner capital en 4.7** antes del forward test en un mercado que sube.
- **No confundir "SOBREVIVE" con "funciona".** Significa "no la pude matar en esta ventana".

---

## 7. Lo honesto

El piso de stablecoins (5-10% anual, sin drawdown, sin cola) sigue siendo el rival y sigue
ganando. Durante los meses de espera de 4.7 está rindiendo: esperar no es tiempo muerto, es
la opción que ya va adelante mientras la otra se prueba.

Esta sesión la disciplina cortó para los dos lados: **mató cuatro hipótesis con mejor prior
—una con p aparente de 0,0000— y dejó pasar una que no era favorita de nadie y que salió de
rebote midiendo otra cosa.** Eso es exactamente lo que tiene que hacer un criterio escrito
antes de mirar.

**Commits de la sesión:** `aee96b2` · `03f9871` · `4725c3d` · `831370d` · `5c779b4` ·
`e577e59` (rama `banco/primer-toque`).
