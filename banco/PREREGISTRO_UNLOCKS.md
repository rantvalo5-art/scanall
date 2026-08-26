# PREREGISTRO — desbloqueos de tokens como shock de oferta

> **Escrito ANTES de tocar un solo precio.** 2026-08-25. El censo de eventos ya se hizo
> (`banco/unlocks.py`), pero **ningun retorno fue calculado todavia**. Si algo de este
> archivo cambia despues de ver un numero, el experimento no vale.

## Por que esta idea y no otra

~17 lineas cerradas en este repo, todas con la misma firma: son transformaciones de la
serie de precios, y el techo condicional y el techo oraculo dicen que ahi no queda
informacion. Un desbloqueo **no sale del precio**: es oferta que entra al mercado, con
fecha publicada meses antes y mecanismo causal explicito. Es la unica familia disponible
que no comparte causa de muerte con lo ya muerto.

## Los datos, ya censados

`defillama-datasets.llama.fi/emissions/{slug}`, 370 protocolos, 121 con par USDT en
Binance. Eventos `cliff` pasados desde 2021 con tamano calculable: **16.837**.

| tamano (% del circulante) | eventos |
|---|---|
| < 0,1% | 11.525 |
| 0,1 – 0,5% | 2.605 |
| 0,5 – 1% | 978 |
| 1 – 2% | 713 |
| 2 – 5% | 514 |
| 5 – 10% | 175 |
| >= 10% | 327 |

Poblacion de trabajo: **>= 0,5% del circulante = 2.707 eventos, 105 pares**, por ano
2021:457 2022:228 2023:342 2024:503 2025:681 2026:496.

## Hipotesis

Un desbloqueo grande mete oferta vendedora. **H: el retorno posterior al desbloqueo es
peor que el de la misma moneda en un momento cualquiera**, y el efecto **crece con el
tamano relativo** del desbloqueo.

La prediccion fuerte no es el signo — es la **dosis-respuesta monotona**. Un solo bucket
que da bien es facil de fabricar; una escalera monotona sobre cinco buckets, no.

## Especificacion — UNA sola, sin variantes

- **Evento**: unlocks `cliff` agregados por `(simbolo, dia)`. Si tres categorias
  desbloquean el mismo dia, es UN evento con la suma de tokens — contarlos por separado
  triplicaria un shock unico.
- **Tamano** `pct` = tokens del evento / circulante ese dia (curva `documentedData`).
- **Entrada**: cierre de la ultima vela horaria CERRADA en el timestamp del evento
  (offset −1, la convencion del repo).
- **Barreras**: ±8%, horizonte **14 dias**. Un shock de oferta actua en dias, no en meses;
  14d es la eleccion, y no se prueban otros horizontes.
- **Costo**: `COSTO_PCT` del banco (0,20%).
- **Direcciones**: se corren **las dos**. El repo casi cierra funding midiendo el lado
  equivocado; el lado corto es el que predice la hipotesis, pero ambos se reportan.
- **Buckets de dosis**: `[0,5–1%)`, `[1–2%)`, `[2–5%)`, `[5–10%)`, `>=10%`.
- **Corte secundario por categoria**: `insiders`, `privateSale`, `noncirculating`,
  `ecosystem`, `farming`, `airdrop`. Declarado ahora para que no sea pesca despues.

## La compuerta que decide, y por que

**`vs_pareado` es la primaria, no el win rate.** Las monedas con calendario de vesting son
sistematicamente distintas — alts nuevas, FDV alto, float bajo, que sangraron entre 2021 y
2026. Sin el control mismo-simbolo, este test mide "las alts con vesting bajan", que es un
hecho de la muestra y no del desbloqueo. El control pareado compara cada evento contra
entradas de **la misma moneda** en momentos al azar.

Se aplican ademas las seis compuertas cableadas de `banco/lote.py`: n>=200, cruza el
umbral, Benjamini-Hochberg sobre el lote, gana al pareado, sobrevive sacar el top-3
simbolos, y >=60% de semanas.

## Regla de parada, exacta

**La familia se declara muerta si NO se cumplen las dos:**

1. el bucket `>=10%` (el shock mas grande, n=327) le gana a su pareado en la direccion
   predicha, **y**
2. la escalera de dosis es **monotona** en al menos 4 de los 5 buckets.

Un bucket suelto que cruza, con la escalera desordenada, **no cuenta** — eso es una celda
que salio bien de un lote de diez, y este repo ya lo confundio con un hallazgo antes.

## Lo que NO se permite, escrito antes

- cambiar el horizonte de 14d ni las barreras de ±8% despues de ver el resultado;
- mover el corte de 0,5% que define "grande";
- reportar una categoria sola si la poblacion completa no cruza;
- promover un resultado que muera en el p por bloques (el binomial esta inflado en este
  banco por un factor enorme — ver [[banco-lote-harness]]).

## Los dos sesgos, declarados y sin resolver

1. **No es point-in-time.** El calendario es el snapshot de hoy; si un proyecto reprogramo
   su vesting, la historia quedo reescrita. Para `cliff` de vesting documentado el riesgo
   es bajo (son contractuales) pero no es cero y no se puede verificar desde aca.
2. **Supervivencia, y va EN CONTRA de la hipotesis.** Solo estan los protocolos que hoy
   existen y DefiLlama trackea. El que se murio despues de un desbloqueo grande no esta.
   Eso censura los peores resultados, o sea sesga en contra de encontrar el efecto
   negativo que H predice. Si el efecto aparece igual, aparece **a pesar** del sesgo.

## OOS

`PREREGISTRO_ANCHO.md` dejo intacta la ventana 2024-08 → 2025-08 y no se uso (no
sobrevivio nada que promover). Si algo cruza aca, la replicacion se hace **partiendo por
ano**: descubrimiento en 2021-2024, confirmacion en 2025-2026. Se declara ahora.

---

# RESULTADOS (2026-08-25)

> Lo de arriba no se toco.

## El test NO es adjudicable, y la culpa es del preregistro

La regla de parada se apoya en el bucket `>=10%`. Ese bucket quedo con **n=143**, debajo
de la compuerta n>=200. **No se puede evaluar la regla.** Cuatro de los cinco buckets
quedaron subpotenciados:

| bucket | n | corto vs pareado | largo vs pareado |
|---|---|---|---|
| 0,5–1% | 197 | subpotenciado | subpotenciado |
| 1–2% | 193 | subpotenciado | subpotenciado |
| **2–5%** | **341** | **−5,27pp** | **+5,27pp** |
| 5–10% | 162 | subpotenciado | subpotenciado |
| **>=10%** | **143** | subpotenciado | subpotenciado |

**El error es de planificacion y es mio.** El censo decia 2.707 eventos grandes, pero eso
era ANTES de agregar por (sym,dia), de descartar los que ocurrieron cuando el token todavia
no cotizaba, y de exigir 14 dias de futuro. Desglose real de los 2.431 eventos agregados:

- 478 anteriores a la primera vela del par -> no hay precio, irrecuperables;
- 913 **futuros** (el calendario llega a 2032+) -> todavia no ocurrieron;
- **1.040 usables**, que es lo que el test efectivamente uso (1.051 entradas marcadas).

O sea: la muestra se agoto, no se desperdicio. El limite de potencia es de datos, no de
especificacion. Era calculable antes de correr y no lo calcule.

## Lo que SI se pudo medir, y apunta al reves

Ninguna de las 11 hipotesis sobrevive, en ninguna de las dos direcciones. Y el poco
efecto que hay va **contra** H: los desbloqueos son seguidos de leve SOBRE-rendimiento
contra la linea base de la misma moneda, no de caida.

| hipotesis | n | largo vs pareado | sin top-3 | p bloques |
|---|---|---|---|---|
| 2–5% | 341 | **+5,3pp** | −0,2 | 1,0000 |
| `noncirculating` | 238 | +5,1pp | −2,0 | 1,0000 |
| `insiders` | 273 | +4,1pp | −1,7 | 1,0000 |
| todos >=0,5% | 1.036 | +1,5pp | −3,3 | 1,0000 |
| `privateSale` | 237 | −1,4pp (unico en direccion de H) | −7,6 | 1,0000 |

Todas mueren igual: **`sin_top3` da vuelta el signo** en las cuatro, y el p por bloques es
1,0000 en todas. Es concentracion en pocos simbolos, no un efecto.

## Un defecto del harness que este test expuso

La compuerta de semanas (`sem`) salio **`--` en todas**: `SEM_N_MIN = 20` exige 20 senales
por semana para que la semana cuente, y ~1.040 eventos repartidos en 5,6 anos son ~3,5 por
semana. **La compuerta de semanas es estructuralmente inaplicable a estudios de evento
esparcido.** No es que haya pasado: no se pudo evaluar. Cualquier lote de eventos raros en
este banco tiene el mismo agujero y hay que reemplazarla por bloques temporales.

## Veredicto

**No se declara muerta la familia** — la regla de parada exigia medir el bucket `>=10%` y
no se pudo. Tampoco se re-cortan los buckets para ganar n: eso seria aflojar la
especificacion despues de ver los numeros, que es justo lo prohibido.

Lo que corresponde es un preregistro NUEVO con un primario que **si** tenga potencia con
1.040 eventos: la dosis-respuesta como **tendencia continua** sobre todos los eventos
(`vs_pareado` contra `log(pct)`), en vez de la comparacion del bucket superior. Un test de
tendencia usa toda la muestra en vez de 143 observaciones.

**Contaminacion declarada:** al escribir eso ya vi que el bucket 2–5% apunta en contra de
H. Cualquier preregistro nuevo tiene que decirlo, porque mi prior ya no esta limpio.
