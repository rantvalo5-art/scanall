# PREREGISTRO — CORRIDA 7: patrones de velas japonesas

> Escrito el **2026-08-28**, **antes de calcular un solo patrón**. Los resultados van
> debajo de la línea, como en las corridas 1 a 6.

---

## 1. Por qué esto NO está cubierto por las 4.140 anteriores

El banco midió **forma de vela como features continuas** (`lote_ancho.py`): `cuerpo =
(c−o)/rango`, `mecha_sup`, `mecha_inf`, eficiencia de 24h, volumen por unidad de
movimiento — cada una cortada por **quintiles**. Resultado: **0 de 86**.

Un patrón clásico es **otra forma funcional**, y la diferencia no es cosmética:

| | features continuas (ya medido) | patrón clásico (nunca medido) |
|---|---|---|
| forma | corte por cuantil de UNA variable | **conjunción booleana** de 4-6 desigualdades |
| memoria | una vela | **2 a 3 velas consecutivas** |
| contexto | ninguno | **requiere una tendencia previa** ("envolvente alcista *después de una baja*") |

Ejemplo, envolvente alcista:
`c>o AND c₋₁<o₋₁ AND c>o₋₁ AND o<c₋₁`, tras N barras de caída.
Eso es un punto aislado del espacio de conjunciones. Un corte por quintil de `cuerpo` no
lo contiene, ni al revés.

**Y hay un desajuste de resolución que agranda el agujero:** el banco mide todo en velas
de **1h**, y los patrones se definieron y se usan sobre velas **diarias**. El repo no
tiene una sola caché de 1d.

## 2. El prior, dicho de entrada

**Es bajo, y hay que escribirlo antes y no después.** Razones:

1. Un patrón es una función determinística del OHLC de 2-3 barras, y el repo ya recorrió
   mucho de ese espacio.
2. La literatura sobre patrones de velas en acciones es mayormente negativa (Marshall,
   Young & Rose 2006 sobre el DJIA; Horton 2009).
3. Es la familia con más difusión pública que existe: si funcionara de forma simple,
   estaría arbitrada.

**Contra eso, tres razones por las que igual se mide:** la forma funcional es genuinamente
distinta (§1), la resolución correcta nunca se probó (1d), y el costo es bajo — la
maquinaria está entera y los datos de 1d son 2 requests por par.

## 3. Diseño

### 3.1 El estimador: control POR BARRA, no por símbolo

Un patrón es un **evento**, no un ranking, así que el top-k de `ranking.py` no aplica
directamente. Pero el estimador sí, y es el que hay que usar:

```
exceso(t) = media(y | símbolos donde el patrón disparó en t) − media(y | universo de t)
semana(w) = media de exceso(t) sobre las barras de w
estadístico = media de semana(w), cada semana pesando UNO
```

Esto neutraliza **el término de mercado** — que en un test de patrones es el sesgo
principal, porque los patrones alcistas disparan más en días alcistas del mercado entero.
`lote.py` aparea por **símbolo**, no por barra, y por eso nunca lo neutralizó.

`y` en unidades del ATR base del propio símbolo (mediana móvil de 30d), igual que las
corridas 2-6: sin normalizar, ~87% de cualquier efecto es escala.

### 3.2 Universo, ventana, resoluciones

- Universo: `base200` **menos las 16 que no son cripto** (7 stablecoins/FX + 9 acciones
  tokenizadas). Se aplica la regla de método de la corrida 5, **antes** de correr.
- Ventana: **2021-08-01 → 2026-08-01** (5 años, cuatro regímenes).
- **Dos resoluciones**: `1d` (donde los patrones se definieron) y `1h` (donde el banco
  mide todo). Se corren las dos y se reportan las dos.
- Horizontes: para `1d`, **1, 3 y 5 días**. Para `1h`, **4h y 24h**.

### 3.3 Los patrones — declarados ANTES, con su definición exacta

**14 patrones**, los clásicos, cada uno con su espejo bajista donde existe. Las
definiciones se fijan acá para que no se puedan ajustar después:

| patrón | definición |
|---|---|
| `martillo` | mecha inferior ≥ 2× cuerpo, mecha superior ≤ cuerpo, cuerpo ≤ 30% del rango |
| `estrella_fugaz` | espejo del martillo (mecha superior ≥ 2× cuerpo) |
| `doji` | cuerpo ≤ 5% del rango |
| `marubozu_alc` / `marubozu_baj` | cuerpo ≥ 90% del rango, del signo correspondiente |
| `envolvente_alc` / `envolvente_baj` | el cuerpo de hoy contiene entero al de ayer, signo opuesto |
| `harami_alc` / `harami_baj` | el cuerpo de ayer contiene entero al de hoy, signo opuesto |
| `perforante` / `nube_oscura` | cierra más allá del punto medio del cuerpo anterior, signo opuesto |
| `estrella_maniana` / `estrella_noche` | 3 velas: cuerpo grande, cuerpo chico con gap, cuerpo grande opuesto |
| `tres_soldados` / `tres_cuervos` | 3 cuerpos consecutivos del mismo signo, cada cierre superando al anterior |

### 3.4 El contexto — el brazo que hace honesta la prueba

Los patrones de reversión **se definen con contexto**: un envolvente alcista solo cuenta
*después de una baja*. Medir el patrón sin contexto es medir otra cosa, y medirlo solo con
contexto es meterle un filtro de momentum que ya sabemos que no aporta.

**Se corren los dos brazos de cada patrón**, y se declara ahora qué significa cada
resultado:

- **sin contexto**: el patrón crudo.
- **con contexto**: patrón de reversión alcista precedido de `roc_3 < 0` (3 barras
  abajo), y el espejo para los bajistas.

> Si un patrón vive **solo con contexto**, hay que mostrar que le gana al contexto
> **solo** (`roc_3 < 0` sin ningún patrón). Si no le gana, lo que se midió es el momentum
> de tres barras, no la vela. **Este brazo de control va en el lote.**

### 3.5 Brazos de control

1. **`roc_3<0` / `roc_3>0` pelados** — el contexto sin patrón (§3.4).
2. **3 máscaras al azar** con la misma tasa de disparo que la mediana de los patrones,
   para el MDE.
3. **`cuerpo` en quintil alto** — la feature continua ya medida, para verificar que este
   diseño reproduce su cero y no está inventando señal por construcción.

## 4. Compuertas — las mismas seis

Spread > 0, signo crudo igual al normalizado, fuera del MDE del azar, **FDR q=0,10 sobre
el lote entero** (todos los patrones × contextos × direcciones × horizontes × resoluciones
juntos, no una familia por vez), `sin_top3` > 0, `sin_top1` > 0, y ≥60% de semanas con
exceso > 0. Veredicto por default: **CERRADA**.

**Costos**: 0,20% y 0,50%. Un sobreviviente que solo vive al costo barato no cuenta
(lección de la corrida 5b).

**n mínimo por patrón**: un patrón que dispara en menos de **200 barras-símbolo** o menos
de **20 semanas distintas** se reporta como *"no se pudo medir"*, no como *"no está"*. El n
se cuenta **antes** de mirar el resultado, y se publica la tabla de disparos completa.

## 5. Regla de parada — fijada ANTES

1. **Si ningún patrón sobrevive en ninguna resolución**, la familia queda cerrada. No se
   prueba un patrón más, ni una definición alternativa del mismo patrón, ni un umbral
   distinto para "cuerpo chico". **Las definiciones de §3.3 son las que se corren.**

2. **Un patrón que vive solo CON contexto y no le gana al contexto pelado, muere** (§3.4).

3. **Un patrón que vive en una sola resolución y no en la otra** no se descarta
   automáticamente —1d y 1h son mercados distintos para esto— pero tiene que pasar el
   corte por régimen de la §5.2 de la corrida 3 antes de contar como candidato.

4. **Un patrón que vive en un solo horizonte mientras sus vecinos están en cero es ruido
   de barrido** (regla 2 de la corrida 4).

5. **Nada se promueve a capital desde acá.** Lo máximo que puede salir es un candidato
   para forward test, con su fecha escrita antes de mirar el resultado.

## 6. Lo que esta corrida NO puede decir

1. **Nada sobre patrones de gráfico** (hombro-cabeza-hombro, triángulos, banderas). Son
   otra familia, requieren detección de estructura y no entran acá.
2. **Nada sobre patrones con volumen** — la confirmación por volumen es parte del canon y
   queda fuera para no multiplicar el espacio de búsqueda. Si algo sobrevive, ahí sí.
3. **Nada sobre la cola.** El universo es el top-200 por volumen.
4. **No modela impacto de mercado.**

## 7. Fuga declarada

- Vi **0 direccionales** en las seis corridas anteriores. El sesgo apunta a querer
  encontrar algo.
- Vi que `cuerpo`/`mecha` continuas dieron 0 de 86 a 1h. **No vi ningún patrón calculado**,
  ni una tasa de disparo, ni un solo dato de velas diarias — la caché de 1d no existe
  todavía.
- El prior bajo de §2 está escrito antes de mirar, precisamente para no poder decir
  después "ya lo sabía".

---

# RESULTADOS DE LA CORRIDA 7 — velas japonesas (2026-08-28)

**180 pares** (`base200` menos las 21 que no son cripto), **2021-08-01 → 2026-08-01**.

| resolución | filas | barras | pares | **semanas** | MDE |
|---|---|---|---|---|---|
| **1d** | 160.051 | 1.766 | 136 | **253** | ±0,0386 |
| **1h** | 3,98 M | 43.094 | 142 | **257** | ±0,0221 |

**720 brazos** = 15 patrones × 2 contextos × 2 direcciones × 5 horizontes × 2 costos, más
los 6 controles. FDR q=0,10 sobre el lote entero de cada resolución.

## R1. El veredicto

| resolución | horizonte | costo | brazos | exceso > 0 | **SOBREVIVEN** | mejor |
|---|---|---|---|---|---|---|
| 1d | 1d | 0,20% | 66 | 5 | **0** | +0,0222 |
| 1d | 3d | 0,20% | 66 | 13 | **0** | +0,0942 |
| 1d | 5d | 0,20% | 66 | 21 | **0** | +0,1030 |
| 1d | (los tres) | 0,50% | 198 | 4 | **0** | +0,0600 |
| **1h** | 4h | 0,20% | 66 | **0** | **0** | −0,0868 |
| **1h** | 24h | 0,20% | 66 | **0** | **0** | −0,0886 |
| **1h** | (los dos) | 0,50% | 132 | **0** | **0** | −0,3141 |

**0 de 396 en diarias. 0 de 264 en horarias, y ahí ni un solo brazo con exceso positivo.**

## R2. Ningún brazo quedó sin medir — la distinción que importa

**0 de 432 brazos cayeron en "NO SE PUDO MEDIR"** (el umbral era n < 200 barras-símbolo o
< 20 semanas, fijado en §4 antes de correr). Los 15 patrones dispararon con n suficiente:
el más raro, `marubozu_baj`, 1.294 veces en 5 años sobre 136 pares.

Eso convierte el resultado en **"no está"**, no en **"no se pudo medir"** — que es la
distinción que mató a unlocks y a la cola ilíquida por el lado equivocado.

## R3. El diseño pasa su propio control de validez

`CTRL cuerpo q80` es la feature continua que `lote_ancho.py` ya había medido en 0 de 86.
Acá da **−0,016 (largo) / −0,037 (corto)**: reproduce su cero. **El estimador no fabrica
señal.** Y los tres controles al azar quedan entre −0,010 y −0,044, o sea planos.

## R4. Lo más fuerte de la corrida: la dirección canónica es una moneda

Cada patrón tiene una dirección que el canon dice que predice, y esa dirección quedó
**declarada en `velas.py: DIRECCION` antes de correr** — precisamente para que no se
pudiera elegir el signo después de ver el resultado. Comparando cada patrón contra **su
propia dirección contraria**, al mismo costo y en la misma barra:

| resolución | la dirección CANÓNICA le gana a la contraria | exceso medio canónica | exceso medio contraria |
|---|---|---|---|
| **1d** | **42 de 84 — 50,0%** | −0,0287 | −0,0255 |
| **1h** | **16 de 56 — 28,6%** | −0,1671 | −0,1340 |

> **En diarias, la dirección que el patrón predice acierta exactamente la mitad de las
> veces contra su propio espejo. Es una moneda, con dos decimales.**

Y en las dos resoluciones el exceso medio es **peor** en la dirección canónica que en la
contraria. Un patrón sin ventaja pero con alguna estructura daría 60 de 84 y moriría en
las compuertas; esto no llega ni a tener estructura.

**Ojo con no sobrevender el 28,6% de 1h**: ahí las **dos** direcciones dan negativo
(−0,134 y −0,167) porque el costo se aplica solo a la pata que dispara y a 4h eso son
~0,10 ATR fijos contra un universo que no paga nada. **No es una señal inversa
aprovechable, es aritmética de costo.** Lo que sí vale es la comparación relativa, que se
hace al mismo costo: la etiqueta canónica no aporta información.

## R5. Los pocos positivos apuntan al revés del canon

Los mejores brazos de 1d, con su dirección canónica al lado:

| patrón | canon dice | da positivo en | exceso | muere en |
|---|---|---|---|---|
| `marubozu_baj` | corto | corto ✓ | +0,1030 | FDR |
| `marubozu_alc +ctx` | largo | largo ✓ | +0,0942 | `sin_top3` < 0 y 44% de semanas |
| **`estrella_maniana`** | **largo** | **corto** ✗ | +0,0803 | FDR |
| **`envolvente_alc`** | **largo** | **corto** ✗ | +0,0324 | MDE |
| **`nube_oscura +ctx`** | **corto** | **largo** ✗ | +0,0303 | MDE |

Tres de los cinco mejores están **invertidos respecto de lo que el patrón afirma**. Si la
dirección se hubiera elegido después de mirar, los cinco contarían como aciertos. Es el
ejemplo más limpio de por qué la regla se escribe antes.

## R6. El brazo de contexto no hizo falta activarlo

La regla 5.2 decía: *un patrón que vive solo con contexto y no le gana al contexto pelado,
muere*. **No llegó a aplicarse** — ningún patrón sobrevivió, con contexto ni sin él. Para
el registro, el contexto pelado (`roc_3`, sin ninguna vela) da −0,054 / +0,002 / −0,016 /
−0,037 según dirección y signo: también plano.

## R7. Veredicto

> **La familia de patrones de velas queda CERRADA. 660 brazos, 15 patrones clásicos con y
> sin su condición de contexto, las dos direcciones, cinco horizontes, dos resoluciones
> —incluida la diaria, que es donde los patrones se definieron y que el banco nunca había
> mirado—, 253 semanas y cuatro regímenes. Cero sobrevivientes, ningún brazo sin medir, y
> la dirección canónica acertando exactamente la mitad de las veces contra su espejo.**

Por la regla 5.1, escrita antes: **no se prueba un patrón más, ni una definición
alternativa del mismo patrón, ni otro umbral para "cuerpo chico".**

Lo que esta corrida **no** toca sigue siendo lo declarado en §6: patrones de gráfico
(hombro-cabeza-hombro, triángulos, banderas), que son otra familia y necesitan detección
de estructura, y la confirmación por volumen, que se dejó afuera para no multiplicar el
espacio de búsqueda.

## R8. Lo que agrega al mapa general

Es la primera vez que el banco mide algo en **velas diarias**. Vale anotarlo porque
descarta una hipótesis de rescate que quedaba flotando: *"no encuentra nada porque mira a
1h"*. A 1d, con 253 semanas y un MDE de ±0,039, tampoco hay nada — y el MDE de 1d es casi
el doble de fino que el de 1h en unidades comparables.
