# Herramientas del agente de IA sobre la capa gold

> Nota: en el código, las descripciones de cada herramienta están en inglés porque las lee el modelo; aquí se presentan traducidas.

Implementado como servidor MCP en [`agent/`](../agent) (`server.py`, `tools.py`, `athena.py`, tests offline en
`test_tools.py`). Notas de implementación que difieren del primer diseño:

- Los valores de `LIMIT` (`top_n`, `limit`) son enteros validados que se insertan en el SQL en lugar de
  placeholders `?`; todos los demás valores pasan por `ExecutionParameters`.
- `data_freshness` cubre cinco tablas (`payment_failure_reasons` queda afuera: comparte fechas con
  `payment_provider_daily`) y castea `NULL` de forma explícita.
- El agente se ejecuta como un usuario IAM dedicado que solo puede llamar a Athena en el workgroup `cdc-lakehouse`;
  accede a Glue y S3 exclusivamente a través de Athena (`aws:CalledVia`), y tiene denegado `gold/_staging/`,
  `silver/` y `bronze/`.

El agente nunca escribe SQL. Llama a un conjunto fijo de herramientas de solo lectura; cada herramienta corresponde a
una pregunta de negocio y ejecuta una consulta parametrizada de Athena contra `shopflow_gold` (Athena engine v3, SQL
de Trino, workgroup `cdc-lakehouse`, límite de escaneo de 1 GB por consulta).

## Convenciones comunes a todas las herramientas

- **Ejecución.** `StartQueryExecution` con un `QueryString` que contiene placeholders `?` y
  `ExecutionParameters` (strings, en el orden de los placeholders). El wrapper envía los valores string como
  literales SQL (`'AR'`, `'2026-10-01'`) y los enteros como números sin comillas. Los placeholders `?` nunca se
  concatenan. El único texto que se sustituye en el SQL son fragmentos fijos elegidos de una allow-list en el
  wrapper (los fragmentos `{dims}` / `{group_expr}` que se ven más abajo), nunca entrada del usuario.
- **Validación (antes de cualquier consulta).** Las fechas deben estar en formato ISO `YYYY-MM-DD`,
  `date_from <= date_to`, con un rango de 92 días como máximo. Los valores enumerados se verifican contra listas cerradas:
  - `country`: AR, MX, CL, CO, UY, PE
  - `category`: home, electronics, sports, books, beauty
  - `provider`: mercadopago, stripe, paypal
  - `carrier`: Andreani, OCA, Correo Argentino, Estafeta, Chilexpress, Servientrega
  - `limit` / `top_n`: entero 1..50.
  Cualquier otro valor devuelve un error de validación que lista los valores permitidos; no se ejecuta ninguna consulta.
- **Filtros opcionales.** Un filtro omitido se pasa como el string `ALL`, y el SQL usa
  `(? = 'ALL' OR col = ?)`, de modo que la cantidad de placeholders es fija. Por eso cada uno de estos filtros consume
  dos parámetros (el mismo valor dos veces).
- **Valores por defecto de fechas.** Cuando se omiten `date_from` / `date_to`, el wrapper los resuelve como la
  fecha UTC actual (`today`), así el SQL siempre recibe fechas explícitas.
- **Salida.** Filas en JSON (`columns`, `rows`) más `meta`: `table`, `rows_returned`, `data_as_of`
  (ver Guardrails), `currency` cuando corresponde, y `notes` (strings que el LLM debe transmitir).
- **Advertencia sobre la escala de tiempo.** El generador comprime el tiempo (1 hora simulada = 1 minuto real). Todo
  valor `hours_*` está en horas reales y es pequeño (de minutos a unas pocas horas); no debe interpretarse como
  tiempos de entrega realistas de producción.

---

## 1. `get_sales_summary`

**Descripción para el LLM.** Ingresos, unidades y cantidad de pedidos para un rango de fechas, con desglose opcional por
día, país y/o categoría. Úsala para "¿cuánto vendimos hoy / esta semana?". Cuenta solo los
pedidos que actualmente están en estado paid, shipped o delivered (no pending ni cancelled), según la fecha de creación
del pedido. No responde preguntas sobre ganancia, margen, reembolsos ni por cliente.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `date_from` | string (fecha ISO) | no | rango <= 92 días | hoy (UTC) |
| `date_to` | string (fecha ISO) | no | >= `date_from` | hoy (UTC) |
| `country` | string | no | AR, MX, CL, CO, UY, PE | todos |
| `category` | string | no | home, electronics, sports, books, beauty | todas |
| `group_by` | enum | no | `none`, `day`, `country`, `category`, `country_category`, `day_country` | `country` |

`{dims}` y `{group_expr}` provienen de un mapa fijo indexado por `group_by`
(p. ej., `country` -> dims `country`, `ORDER BY revenue DESC`; `none` -> literal `'all'`, sin dimensión).

```sql
SELECT {dims}
       sum(orders)                         AS orders,
       sum(units)                          AS units,
       CAST(sum(revenue) AS DECIMAL(14,2)) AS revenue_usd,
       CAST(sum(revenue) / nullif(sum(orders), 0) AS DECIMAL(12,2)) AS avg_order_value_usd
FROM shopflow_gold.daily_sales
WHERE order_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
  AND (? = 'ALL' OR country  = ?)                              -- country, country
  AND (? = 'ALL' OR category = ?)                              -- category, category
GROUP BY {group_expr}
ORDER BY {order_expr}
LIMIT 50
```

Nota: `sum(orders)` sobre un desglose por categoría cuenta dos veces un pedido que contiene varias categorías.
La herramienta solo informa el total de pedidos cuando `group_by` no incluye `category`; en caso contrario devuelve
`orders_with_category` y las notas aclaran que los pedidos no son aditivos entre categorías.

**Columnas de salida.** `[order_date] [country] [category] orders, units, revenue_usd, avg_order_value_usd`.
**Ejemplo.** "¿Cuánto vendimos esta semana en México, por categoría?"
-> `{date_from: monday, date_to: today, country: "MX", group_by: "category"}`.

---

## 2. `get_payment_failure_rate`

**Descripción para el LLM.** Proporción de intentos de pago que fallaron, por proveedor (opcionalmente por día).
Úsala para "¿qué porcentaje de los pagos falla?". Cuenta intentos, por lo que un pago reintentado cuenta como varios
intentos; no es la proporción de pedidos que no se pudieron pagar. Para conocer el motivo de los fallos usa
`get_payment_failure_reasons`.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `date_from` | string (fecha ISO) | no | rango <= 92 días | hoy - 6 días |
| `date_to` | string (fecha ISO) | no | >= `date_from` | hoy (UTC) |
| `provider` | string | no | mercadopago, stripe, paypal | todos |
| `group_by` | enum | no | `provider`, `day_provider` | `provider` |

```sql
SELECT {dims}
       sum(attempts)                                                   AS attempts,
       sum(failed)                                                     AS failed,
       sum(captured)                                                   AS captured,
       CAST(sum(failed) * 1.0 / nullif(sum(attempts), 0) AS DECIMAL(5,4)) AS failure_rate,
       CAST(sum(captured_amount) AS DECIMAL(14,2))                     AS captured_amount_usd
FROM shopflow_gold.payment_provider_daily
WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
  AND (? = 'ALL' OR provider = ?)                                -- provider, provider
GROUP BY {group_expr}
ORDER BY {order_expr}
LIMIT 50
```

La tasa siempre se recalcula como `sum(failed) / sum(attempts)`; promediar la columna diaria `failure_rate`
sería incorrecto.

**Columnas de salida.** `[attempt_date] provider, attempts, failed, captured, failure_rate, captured_amount_usd`.
**Ejemplo.** "¿Qué proveedor de pagos falla más esta semana?"

---

## 3. `get_payment_failure_reasons`

**Descripción para el LLM.** Por qué fallan los pagos: cantidad de fallos por `failure_code` y por proveedor, con la
proporción sobre el total de fallos y sobre el total de intentos. Úsala después de `get_payment_failure_rate`, o para
"¿cuál es el principal motivo de fallo de los pagos en Stripe?". No da detalle por cliente ni por pedido.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `date_from` | string (fecha ISO) | no | rango <= 92 días | hoy - 6 días |
| `date_to` | string (fecha ISO) | no | >= `date_from` | hoy (UTC) |
| `provider` | string | no | mercadopago, stripe, paypal | todos |
| `top_n` | integer | no | 1..50 | 10 |

`failure_code` es texto libre del origen (p. ej., `insufficient_funds`, `card_declined`), por lo que se
devuelve como dato y no se valida contra una lista.

```sql
WITH reasons AS (
    SELECT provider, failure_code, sum(failures) AS failures
    FROM shopflow_gold.payment_failure_reasons
    WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
      AND (? = 'ALL' OR provider = ?)                                -- provider, provider
    GROUP BY provider, failure_code
),
attempts AS (
    SELECT provider, sum(attempts) AS attempts
    FROM shopflow_gold.payment_provider_daily
    WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
      AND (? = 'ALL' OR provider = ?)                                -- provider, provider
    GROUP BY provider
)
SELECT r.provider,
       coalesce(r.failure_code, 'unknown')                                   AS failure_code,
       r.failures,
       CAST(r.failures * 1.0 / sum(r.failures) OVER (PARTITION BY r.provider) AS DECIMAL(5,4)) AS share_of_provider_failures,
       CAST(r.failures * 1.0 / nullif(a.attempts, 0) AS DECIMAL(5,4))        AS share_of_provider_attempts
FROM reasons r
JOIN attempts a ON a.provider = r.provider
ORDER BY r.failures DESC
LIMIT ?                                                                      -- top_n
```

**Columnas de salida.** `provider, failure_code, failures, share_of_provider_failures, share_of_provider_attempts`.
**Ejemplo.** "¿Cuáles son los principales motivos por los que fallan los pagos con MercadoPago?"

---

## 4. `get_late_shipments`

**Descripción para el LLM.** Envíos atrasados respecto de su fecha prometida, según el último build de gold.
`open_overdue` = todavía no entregado y con `promised_at` vencido; `delivered_late` = entregado después de
`promised_at`. Úsala para "¿qué pedidos están demorados ahora?". No devuelve los envíos a tiempo, por lo que
no puede dar un "% de atrasos"; los envíos devueltos no se incluyen. No se devuelven datos de clientes.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `late_type` | enum | no | `open_overdue`, `delivered_late`, `all` | `open_overdue` |
| `carrier` | string | no | Andreani, OCA, Correo Argentino, Estafeta, Chilexpress, Servientrega | todos |
| `country` | string | no | AR, MX, CL, CO, UY, PE | todos |
| `limit` | integer | no | 1..50 | 20 |

```sql
SELECT shipment_id,
       order_id,
       carrier,
       country,
       status,
       late_type,
       promised_at,
       shipped_at,
       delivered_at,
       hours_late,
       snapshot_at,
       count(*) OVER () AS total_matching
FROM shopflow_gold.late_shipments
WHERE (? = 'all' OR late_type = ?)        -- late_type, late_type
  AND (? = 'ALL' OR carrier = ?)          -- carrier, carrier
  AND (? = 'ALL' OR country = ?)          -- country, country
ORDER BY hours_late DESC
LIMIT ?                                   -- limit
```

Notas que agrega la herramienta: `hours_late` se mide en `snapshot_at`, no en el momento actual, por lo que los envíos
abiertos siguen sumando atraso hasta el próximo build; las unidades son horas reales bajo el reloj comprimido.
`total_matching` permite que el agente diga "mostrando 20 de 134".

**Columnas de salida.** `shipment_id, order_id, carrier, country, status, late_type, promised_at, shipped_at, delivered_at, hours_late, snapshot_at, total_matching`.
**Ejemplo.** "¿Qué pedidos están atrasados ahora mismo y con qué transportista?"

---

## 5. `get_late_shipments_by_carrier`

**Descripción para el LLM.** Cantidad y demora promedio de los envíos atrasados agrupados por transportista y país,
para una vista rápida de "quién está fallando". Usa `get_late_shipments` para ver los pedidos individuales. Cuenta solo
envíos atrasados (sin denominador), por lo que ordena a los transportistas por volumen de atrasos, no por tasa de atrasos.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `late_type` | enum | no | `open_overdue`, `delivered_late`, `all` | `open_overdue` |
| `country` | string | no | AR, MX, CL, CO, UY, PE | todos |

```sql
SELECT carrier,
       country,
       count(*)                                    AS late_shipments,
       CAST(avg(hours_late) AS DECIMAL(10,2))      AS avg_hours_late,
       max(hours_late)                             AS max_hours_late,
       max(snapshot_at)                            AS snapshot_at
FROM shopflow_gold.late_shipments
WHERE (? = 'all' OR late_type = ?)   -- late_type, late_type
  AND (? = 'ALL' OR country = ?)     -- country, country
GROUP BY carrier, country
ORDER BY late_shipments DESC
LIMIT 50
```

**Columnas de salida.** `carrier, country, late_shipments, avg_hours_late, max_hours_late, snapshot_at`.
**Ejemplo.** "¿Qué transportista tiene más paquetes demorados en Argentina?"

---

## 6. `get_order_lead_times`

**Descripción para el LLM.** Cuánto tardan los pedidos desde `pending` hasta `delivered`, y por etapa
(de pending a paid, de paid a shipped, de shipped a delivered): promedio, mediana y p90 en horas reales, para
pedidos creados en un rango de fechas. Solo se incluyen los pedidos que alcanzaron la etapa (`orders_measured`).
Las horas son horas reales bajo un reloj de simulación comprimido (1 hora simulada = 1 minuto real), así que
hay que multiplicarlas por 60 para leerlas como horas simuladas. No es adecuada para el seguimiento de pedidos individuales.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `date_from` | string (fecha ISO) | no | rango <= 92 días; se aplica a la fecha de creación del pedido | hoy - 6 días |
| `date_to` | string (fecha ISO) | no | >= `date_from` | hoy (UTC) |
| `country` | string | no | AR, MX, CL, CO, UY, PE | todos (una fila por país si `group_by=country`) |
| `group_by` | enum | no | `none`, `country` | `none` |

```sql
SELECT {dims}
       count(*)                                         AS orders_created,
       count(hours_end_to_end)                          AS orders_delivered,
       CAST(avg(hours_to_pay)       AS DECIMAL(10,2))   AS avg_hours_to_pay,
       CAST(avg(hours_to_ship)      AS DECIMAL(10,2))   AS avg_hours_to_ship,
       CAST(avg(hours_in_transit)   AS DECIMAL(10,2))   AS avg_hours_in_transit,
       CAST(avg(hours_end_to_end)   AS DECIMAL(10,2))   AS avg_hours_end_to_end,
       CAST(approx_percentile(hours_end_to_end, 0.5) AS DECIMAL(10,2)) AS p50_hours_end_to_end,
       CAST(approx_percentile(hours_end_to_end, 0.9) AS DECIMAL(10,2)) AS p90_hours_end_to_end
FROM shopflow_gold.order_lead_times
WHERE CAST(created_at AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
  AND (? = 'ALL' OR country = ?)                                             -- country, country
GROUP BY {group_expr}
ORDER BY {order_expr}
LIMIT 50
```

`approx_percentile` devuelve `double`; el wrapper castea como se muestra. Los promedios ignoran los NULL, por lo que cada
etapa tiene su propio tamaño de muestra implícito; la herramienta también devuelve `orders_delivered` y las notas advierten
que se excluyen los pedidos recientes que todavía no se entregaron (la censura por la derecha sesga los rangos cortos hacia
tiempos más rápidos).

**Columnas de salida.** `[country] orders_created, orders_delivered, avg_hours_to_pay, avg_hours_to_ship, avg_hours_in_transit, avg_hours_end_to_end, p50_hours_end_to_end, p90_hours_end_to_end`.
**Ejemplo.** "¿Cuánto tarda un pedido desde pending hasta delivered?"

---

## 7. `get_top_customers_ltv`

**Descripción para el LLM.** Principales clientes por ingresos de toda su vida (suma de las líneas de pedido de pedidos
paid, shipped o delivered), identificados solo por `customer_id`. Úsala para "¿quiénes son nuestros mejores clientes?".
Nunca devuelve nombres, emails ni dominios de email; para saber quién es un cliente, una persona debe buscar el id en
el sistema de origen. Los ingresos son históricos totales, no limitados a un período.

| Parámetro | Tipo | Obligatorio | Permitido / límites | Por defecto |
|---|---|---|---|---|
| `top_n` | integer | no | 1..50 | 10 |
| `country` | string | no | AR, MX, CL, CO, UY, PE | todos |
| `min_orders` | integer | no | 1..1000 | 1 |

```sql
SELECT customer_id,
       country,
       orders,
       revenue                AS lifetime_revenue_usd,
       avg_order_value        AS avg_order_value_usd,
       first_order_at,
       last_order_at,
       rank() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM shopflow_gold.customer_ltv
WHERE (? = 'ALL' OR country = ?)   -- country, country
  AND orders >= ?                  -- min_orders
ORDER BY revenue DESC, customer_id
LIMIT ?                            -- top_n
```

La lista explícita de columnas es el guardrail de PII: `email_domain` existe en gold pero deliberadamente no se
selecciona (cuasi-identificador). `revenue_rank` se calcula después de los filtros, por lo que es el ranking dentro del
conjunto filtrado.

**Columnas de salida.** `customer_id, country, orders, lifetime_revenue_usd, avg_order_value_usd, first_order_at, last_order_at, revenue_rank`.
**Ejemplo.** "¿Quiénes son nuestros 10 mejores clientes en Chile?"

---

## 8. `data_freshness` (opcional, metadatos)

**Descripción para el LLM.** Informa la fecha de datos más reciente de cada tabla gold. Llámala primero cuando el usuario
pregunte por "hoy" o "ahora", o cada vez que un resultado parezca vacío o desactualizado. No recibe parámetros.

```sql
SELECT 'daily_sales' AS table_name, CAST(max(order_date) AS TIMESTAMP) AS latest_data_at, NULL AS built_at
FROM shopflow_gold.daily_sales
UNION ALL
SELECT 'payment_provider_daily', CAST(max(attempt_date) AS TIMESTAMP), NULL
FROM shopflow_gold.payment_provider_daily
UNION ALL
SELECT 'payment_failure_reasons', CAST(max(attempt_date) AS TIMESTAMP), NULL
FROM shopflow_gold.payment_failure_reasons
UNION ALL
SELECT 'late_shipments', max(coalesce(delivered_at, shipped_at, promised_at)), max(snapshot_at)
FROM shopflow_gold.late_shipments
UNION ALL
SELECT 'order_lead_times', max(coalesce(delivered_at, shipped_at, paid_at, created_at)), NULL
FROM shopflow_gold.order_lead_times
UNION ALL
SELECT 'customer_ltv', max(last_order_at), NULL
FROM shopflow_gold.customer_ltv
```

Los tipos difieren entre ramas, de ahí los casts explícitos; Athena castea el `NULL` de la primera rama al
tipo de las ramas siguientes (`TIMESTAMP`). Si eso falla en la práctica, escribe `CAST(NULL AS TIMESTAMP)`.

**Columnas de salida.** `table_name, latest_data_at, built_at`.
**Ejemplo.** "¿Los datos están actualizados?" / "¿Hasta qué fecha tenemos ventas?"

---

## Guardrails

- **Límites estrictos.** Rol de solo lectura (Athena `athena:StartQueryExecution` y lectura de Glue solo sobre
  `shopflow_gold`, lectura de S3 sobre `gold/`), tope de escaneo de 1 GB en el workgroup, como máximo 50 filas por llamada,
  rangos de fechas de hasta 92 días, 1 consulta por llamada a herramienta, timeout de la herramienta (p. ej., 30 s) con un
  mensaje de error claro. Las llamadas idénticas se cachean con un TTL corto (60 s) para acotar el costo.
- **Cero filas.** Nunca inventar números. Devolver `rows: []` más una nota: "No hay filas para estos filtros
  entre X e Y; hay datos disponibles hasta Z (ver data_freshness)." El agente debe decir eso, sugerir un rango
  más amplio o quitar un filtro, y no interpretar 0 filas como 0 ventas. `late_shipments` y
  `payment_failure_reasons` pueden estar legítimamente vacías (la auditoría lo permite); en esos casos, vacío significa
  "nada atrasado / sin fallos según el último build".
- **Unidades y moneda.** Todos los montos están en USD: `orders.currency` tiene `'USD'` como valor por defecto en el esquema
  de origen pero no se propaga a gold, así que las herramientas etiquetan cada monto con `_usd` y explicitan el supuesto; no
  se hace conversión de moneda. `failure_rate` es una fracción (0.0842 = 8.42%); el agente debe mostrarla como
  porcentaje. Los `hours_*` son horas reales bajo el reloj comprimido (1 hora simulada = 1 minuto real).
- **Frescura.** Gold se reconstruye por completo mediante un job programado, por lo que es un snapshot, no tiempo real. Toda
  respuesta incluye `data_as_of`: para `late_shipments` es `snapshot_at`; para las demás tablas es la
  fecha más reciente disponible (`max(order_date)`, `max(attempt_date)`, `max(last_order_at)`, ...). Si el
  `date_to` solicitado es posterior a `data_as_of`, o si `data_as_of` tiene más antigüedad que el intervalo de build
  esperado, el agente debe decir "datos al ..." en lugar de presentarlos como en vivo. "Hoy" significa la
  fecha UTC actual, que puede no coincidir con la fecha local de un usuario en LATAM.
- **PII.** Gold no tiene nombres ni emails; además, las herramientas nunca seleccionan `email_domain`. Los ids de cliente
  son el único identificador que se devuelve; los ids de envío y de pedido están permitidos.
- **SQL fijo.** Ninguna herramienta acepta texto libre que llegue al SQL. Las enumeraciones y los rangos se validan en
  el wrapper antes de la consulta, los valores pasan por `ExecutionParameters`, y los pocos fragmentos
  estructurales (`group_by`) se eligen de una allow-list.
- **Consistencia entre tablas.** La publicación es atómica por tabla, no entre tablas, por lo que justo después de un build
  dos herramientas pueden discrepar brevemente. Los ingresos de `daily_sales` y `customer_ltv` se concilian en la auditoría
  y deberían coincidir al sumarse sobre todo el histórico.

## Limitaciones de datos encontradas (brechas frente a las 5 preguntas)

1. **Tasa de atrasos imposible.** `late_shipments` guarda solo las filas atrasadas; no hay conteo de envíos a tiempo por
   transportista/país, así que la P3 puede listar y ordenar por volumen pero no dar un "% de atrasos". Solución: agregar una
   tabla gold `shipments_daily` (date, carrier, country, shipments, delivered, late).
2. **El estado de atraso queda congelado en el build.** `hours_late` y `open_overdue` usan `current_timestamp()` al momento
   del build; un envío que pasó a estar vencido después del último build no aparece hasta el siguiente.
   Los envíos devueltos (`status = 'returned'`) quedan excluidos por completo.
3. **No hay timestamp de build salvo `snapshot_at`** (solo en `late_shipments`, que puede estar vacía). Se sugiere una
   tabla `gold_build_info` (table_name, built_at, versiones de silver del manifiesto) para `data_freshness`.
4. **Los motivos de fallo no tienen denominador por sí solos.** Los motivos por proveedor requieren un join con
   `payment_provider_daily` (lo hace la herramienta 3). La granularidad es el intento de pago, no el pedido, por lo que los
   reintentos inflan los intentos; no existe una métrica a nivel pedido de "el pago finalmente falló". `failure_code` es texto
   libre y puede ser NULL (se muestra como `unknown`).
5. **Las ventas se basan en la fecha de creación del pedido y su estado actual.** Un pedido que se pagó y luego se canceló
   sale de las cifras de forma retroactiva; no hay una vista por fecha de pago ni reembolsos/cancelaciones en
   `daily_sales`. Los pedidos no son aditivos entre categorías (un pedido con varias categorías se cuenta en cada una).
6. **No hay columna de moneda en gold**; USD es un supuesto tomado del valor por defecto del DDL de origen.
7. **Lead times: sin categoría, sin transportista, solo país**; las etapas son la primera ocurrencia de cada estado
   (`min(changed_at)`); los pedidos cancelados tienen `cancelled_at` pero no duración. Los promedios sobre ventanas cortas y
   recientes están sesgados hacia abajo porque se excluyen los pedidos no entregados. Bajo el reloj comprimido todas las
   duraciones son mínimas y no son realistas.
8. **`customer_ltv` tiene `email_domain`**, un cuasi-identificador, que se excluye de la herramienta; el LTV es
   histórico total y no se puede filtrar por período. No existe segmento ni nombre de cliente en gold (por diseño).
