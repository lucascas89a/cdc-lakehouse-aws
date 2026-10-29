# Detalle técnico

🌎 Español | [English](en/technical-details.md)

Complemento del [README](../README.md): cómo funciona cada capa, las decisiones de diseño y la operación del stack.

## Contenido

- [Flujo de datos y dónde corre cada pieza](#flujo-de-datos)
- [Decisiones de diseño](#decisiones-de-diseño)
- [Seguridad](#seguridad)
- [Puesta en marcha completa](#inicio-rápido)
- [Capa bronze](#capa-bronze-spark-structured-streaming)
- [Capa silver](#capa-silver)
- [Capa gold](#capa-gold-write-audit-publish)
- [Orquestación con Airflow](#orquestación-airflow)
- [Herramientas del agente de IA](#herramientas-del-agente-de-ia-mcp)
- [Infraestructura AWS](#infraestructura-aws-terraform)

## Flujo de datos

1. **Captura.** Debezium lee el write-ahead log de PostgreSQL a través de un slot de replicación
   lógica y publica un evento por cada cambio de fila en Kafka
   (`shopflow.shop.<table>`). Cada evento incluye la fila antes y después del cambio.
2. **Bronze.** Un job de Spark streaming agrega cada evento tal cual a tablas Delta en S3,
   particionadas por tabla y fecha. Bronze es el log de auditoría que se puede reprocesar.
3. **Silver.** Una segunda consulta de streaming aplica `MERGE` de cada micro-batch sobre tablas limpias:
   estado actual para la mayoría de las entidades, historial SCD Type 2 para `products`, una
   tabla `order_status_history` construida a partir de los cambios de estado, y PII enmascarada.
   Los eventos tardíos o duplicados se resuelven usando el LSN de origen, por lo que el reprocesamiento es
   idempotente.
4. **Gold.** Airflow reconstruye periódicamente tablas de negocio a partir de silver (ventas por
   país y categoría, tasas de fallo de pagos, envíos demorados, tiempos de ciclo de pedidos,
   valor de vida del cliente) y ejecuta controles de calidad de datos antes de publicarlas.
5. **Consumo.** Las tablas gold se registran en el Glue Data Catalog y se consultan con
   Athena. El agente de IA recibe un conjunto reducido de herramientas de solo lectura sobre esas tablas.

### Dónde corre cada pieza

| Componente | Corre en | Por qué |
|---|---|---|
| PostgreSQL, Kafka, Debezium, Spark, Airflow | Docker Compose en una sola VM, con límites de memoria y perfiles por servicio | Mantiene en cero el costo fijo; las mismas imágenes corren en ECS/EKS |
| Tablas Delta (bronze, silver, gold) | Amazon S3 | Almacenamiento durable y económico que cualquier motor puede leer |
| Catálogo y SQL | AWS Glue Data Catalog + Amazon Athena | Serverless, pago por consulta, sin clúster que mantener encendido |
| Herramientas del agente de IA | Contenedor en la VM, que llama a Athena | Acceso acotado y de solo lectura a gold |
| Infraestructura AWS | Terraform | Reproducible, revisable y destruible con un solo comando |

## Decisiones de diseño

| Decisión | Alternativa considerada | Razonamiento |
|---|---|---|
| Debezium + Kafka para CDC | AWS DMS | Debezium es open source, conserva las imágenes before/after y el orden por clave, y corre en cualquier lugar. DMS es una buena opción cuando el equipo quiere un servicio administrado y ya está completamente en AWS. |
| Kafka autoadministrado en Docker | Amazon MSK | MSK arranca en cientos de USD por mes. Para producción con SLAs reales, MSK (o MSK Serverless) es lo que recomendaría. |
| Airflow en Docker | Amazon MWAA | El mismo razonamiento: MWAA tiene un costo mensual fijo. Los DAGs se pueden llevar a MWAA sin cambios. |
| Delta Lake | Apache Iceberg | Delta tiene soporte maduro de `MERGE` en Spark open source y Athena lo lee de forma nativa. Iceberg sería la opción si el cliente está estandarizando sobre él. |
| Athena solo sobre gold | Consultar silver directamente | Las tablas gold son pequeñas, están documentadas y no contienen PII, lo que mantiene bajos tanto el costo de las consultas como el riesgo del agente. |
| Herramientas del agente en lugar de SQL libre | Dejar que el LLM escriba SQL | Costo y comportamiento predecibles; cada herramienta corresponde a una pregunta que el negocio realmente hace. |
| `REPLICA IDENTITY FULL` en las tablas de origen | Valor por defecto (solo clave primaria) | Las imágenes "before" completas hacen trivial derivar los cambios de estado y el historial de precios, a costa de más WAL. |

## Seguridad

- **Sin claves de AWS de larga duración para personas** (sesiones `aws login` con MFA). La identidad del pipeline solo puede
  acceder a los prefijos del lake, y únicamente desde la IP pública de la VM (`aws:SourceIp`, definida en un
  `terraform.tfvars` ignorado por git): una clave filtrada no sirve en ningún otro lugar.
- **S3:** cifrado en reposo, acceso público bloqueado, TLS obligatorio, escaneos de Athena limitados a 1 GB por consulta.
- **Contenedores:** todos los puertos ligados a `127.0.0.1`; Spark, el generador y Airflow corren como usuario no root sobre un
  sistema de archivos de solo lectura y sin capabilities; `no-new-privileges` en todos. Airflow ejecuta Spark
  en el mismo proceso en lugar de recibir el socket de Docker (que equivaldría a root en el host).
- **Bases de datos:** un rol por propósito (Debezium: replicación + lectura; generador: solo DML; Airflow: su propia
  base de datos), `CONNECT` revocado a `PUBLIC`.
- **Los secretos** viven en archivos no versionados (`docker/.env`, `docker/secrets/`), nunca en el repositorio ni en el
  state de Terraform. La contraseña de la UI de Airflow proviene de un archivo montado, por lo que nunca se imprime en los logs.
  El proveedor de configuración por variables de entorno de Kafka Connect está restringido a la única variable que necesita.
- **Los datos personales (PII)** (`email`, `full_name`) se seudonimizan en silver; gold no contiene PII.
- Revisado con una auditoría de seguridad independiente antes de publicar; los hallazgos y las correcciones están en el
  historial de commits.

## Inicio rápido

Requisitos: Docker con Compose v2, unos 3 GB de RAM libre.

```bash
cp docker/.env.example docker/.env      # then fill in two random passwords
docker compose -f docker/compose.yaml up -d
./scripts/register-connector.sh         # registers the Debezium connector
```

Haz un cambio en la base de datos de origen y observa cómo llega a Kafka:

```bash
docker compose -f docker/compose.yaml exec postgres \
  psql -U shopflow_admin -d shopflow -c "UPDATE shop.orders SET status = 'paid' WHERE order_id = 1"

docker compose -f docker/compose.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic shopflow.shop.orders --from-beginning
```

Inicia el generador de datos en vivo (clientes que hacen pedidos, pagos con fallos y reintentos, envíos,
cambios de precio, ediciones del carrito que producen deletes):

```bash
docker compose -f docker/compose.yaml --profile generator up -d generator
docker compose -f docker/compose.yaml logs -f generator     # progress every 60 s
```

El tiempo está comprimido (una hora simulada = 60 s por defecto). Se ajusta con `GENERATOR_RATE` (acciones por
segundo, 1 por defecto) y `GENERATOR_TIME_SCALE` en `docker/.env`. El generador se conecta con su propio
rol, que solo puede leer y escribir filas en el esquema `shop`.

UI web opcional para tópicos y conectores: `docker compose -f docker/compose.yaml --profile ui up -d`,
y luego abrir <http://localhost:8090>.

Cada evento incluye la fila `before` y `after` del cambio, la operación (`c`, `u`, `d`, o `r` para
lecturas de snapshot) y el LSN de origen, que la capa silver usa para aplicar los cambios exactly-once.

| Servicio | Imagen | Límite de memoria |
|---|---|---|
| PostgreSQL (origen) | `postgres:16-bookworm` | 512 MB |
| Kafka (KRaft, un solo nodo) | `apache/kafka:4.3.1` | 1 GB |
| Kafka Connect + Debezium | `quay.io/debezium/connect:3.6.2.Final` | 1 GB |
| Generador de datos (opcional) | construida desde `generator/` | 128 MB |
| Job bronze de Spark (opcional) | construida desde `spark/` | 2 GB |
| Job silver de Spark (opcional) | la misma imagen | 2 GB |
| Airflow (opcional) | construida desde `airflow/` sobre la imagen de Spark | 3 GB |
| Servidor MCP del agente (a demanda) | construida desde `agent/` | 256 MB |
| kafka-ui (opcional) | `kafbat/kafka-ui:v1.5.0` | 512 MB |

Debezium se conecta con un rol dedicado que solo tiene `REPLICATION` y acceso de lectura al
esquema `shop`, y su contraseña llega al conector a través del `EnvVarConfigProvider` de Kafka,
por lo que nunca aparece en la configuración del conector ni en la API REST.

## Capa bronze (Spark Structured Streaming)

`spark/jobs/bronze_cdc.py` se suscribe a todos los tópicos `shopflow.shop.*` y agrega cada evento de Debezium,
sin modificar, a una única tabla Delta en `s3://<lake-bucket>/bronze/cdc_events`, particionada por
`event_date` y `source_table`.

```bash
# needs docker/aws-pipeline.env with the pipeline IAM key and LAKE_BUCKET
docker compose -f docker/compose.yaml --profile spark up -d spark-bronze
```

| Columna | Significado |
|---|---|
| `source_table`, `op`, `source_lsn`, `source_ts_ms` | Extraídas del payload para filtrar y ordenar a bajo costo |
| `payload` | JSON crudo de Debezium (`before`, `after`, `source`, `op`), que solo se parsea en silver |
| `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` | Linaje hacia Kafka |
| `ingested_at`, `event_date` | Hora de carga y fecha de partición |

Notas de diseño:

- **Payload crudo como string.** Un cambio de esquema en el origen nunca puede romper la ingesta; bronze sigue siendo un
  log fiel y reprocesable, y los errores de parseo se manejan en silver.
- **Exactly-once.** El checkpoint en S3 vincula los offsets de Kafka con los commits de Delta. Verificado tras un reinicio:
  15,340 eventos, cero offsets duplicados, cero huecos en los seis tópicos.
- **Un solo escritor por tabla.** El log store por defecto de Delta en S3 es seguro con un escritor; varios escritores
  concurrentes sobre la misma tabla requerirían un log store coordinado.
- **Trigger pensado en el costo.** Cada micro-batch genera solicitudes a S3, por lo que el trigger por defecto es de un minuto y
  el job está pensado para correr a demanda en la demo. Los archivos pequeños se compactan luego con `OPTIMIZE`.
- **Imagen construida para ser reproducible.** Los jars de Spark 4.2.0, Delta 4.4.1 y hadoop-aws 3.5.0 se resuelven en
  tiempo de build (sin acceso a Maven en ejecución), omitiendo las librerías que Spark ya incluye y fijando Jackson a
  la versión de Spark para evitar conflictos de classpath. Corre como usuario no root sobre un sistema de archivos de solo lectura.

## Capa silver

`spark/jobs/silver_cdc.py` lee en streaming la tabla Delta de bronze y, en cada micro-batch, parsea los eventos de
cada tabla de origen y los aplica con `MERGE` de Delta:

| Tabla | Forma | Cómo se mantiene |
|---|---|---|
| `customers`, `orders`, `order_items`, `payments`, `shipments` | Estado actual, una fila por clave | Último evento por clave en el batch; se aplica solo si su LSN es más nuevo que el almacenado. Los deletes son lógicos (`_is_deleted`) |
| `products_history` | SCD Type 2 (`valid_from`, `valid_to`, `is_current`) | Cierra la versión actual e inserta las nuevas en un solo MERGE |
| `order_status_history` | Una fila por cambio de estado | Derivada de las imágenes `before`/`after` de Debezium; MERGE de solo inserción sobre `(order_id, lsn)` |
| `_rejects` | Eventos que no se pueden parsear o sin clave | Se conservan para inspección en lugar de hacer fallar el stream |

```bash
docker compose -f docker/compose.yaml --profile spark up -d spark-bronze spark-silver
```

Notas de diseño:

- **Idempotente por construcción.** Cada escritura está protegida por el LSN de origen o usa el LSN como clave
  (los rechazos, sobre `(topic, partition, offset)`), y los duplicados de la entrega at-least-once de Kafka Connect
  se descartan antes de los MERGE de historial. Volver a ejecutar un micro-batch, o todo el historial, no cambia nada.
- **Historial independiente del orden.** Las versiones SCD2 se insertan por `(product_id, lsn)` y `valid_to` /
  `is_current` se recalculan para cada producto afectado en el mismo MERGE, de modo que los reprocesamientos que leen archivos de bronze
  en otro orden (por ejemplo, después de `OPTIMIZE`) producen el mismo historial. Las columnas de tiempo usan el
  timestamp del commit, coherente con el orden del LSN.
- **Los mensajes envenenados (poison pills) van a `_rejects`, no río abajo.** Los valores que no se pueden convertir (`try_cast`) o
  los payloads que no son JSON se conservan con un `reason`. Los conflictos de concurrencia de Delta (por ejemplo, con `OPTIMIZE`)
  se reintentan, lo cual es seguro porque cada escritura es idempotente.
- **Deletes lógicos.** Conservar una fila marcada como eliminada (tombstone) con su LSN impide que un evento tardío o reprocesado
  reviva una fila eliminada.
- **La PII nunca llega a silver en texto plano.** `email` se convierte en un hash SHA-256 con salt más su dominio, y
  `full_name` se descarta. Esto es seudonimización, no anonimización: el salt es un secreto fuera del
  repositorio, y quien lo tenga podría probar emails candidatos.
- **El mínimo privilegio moldeó el código.** `DeltaTable.isDeltaTable()` sube hasta la raíz del bucket buscando
  un `_delta_log`, que la identidad del pipeline no puede leer; el job verifica `<table>/_delta_log` directamente.

**Conciliación con el origen.** Tras unas 3 horas de tráfico en vivo (~200,000 eventos de cambio, ~9,900
pedidos), 15 métricas (cantidades e importes de pedidos por estado, líneas de pedido, pagos por estado, envíos por
estado, clientes, productos activos y sus precios) resultaron **idénticas** en PostgreSQL y en silver, con
exactamente una versión actual por producto, cadenas `valid_from`/`valid_to` contiguas y sin cambios de estado
duplicados. Luego se borró el checkpoint de silver para forzar un reprocesamiento completo: cada MERGE reportó 0 filas
insertadas, actualizadas o eliminadas. Dos eventos malformados inyectados a propósito en Kafka terminaron en `_rejects`
con su motivo, y el stream siguió corriendo.

## Capa gold (write-audit-publish)

`spark/jobs/gold_build.py` reconstruye seis tablas de negocio pequeñas a partir de silver, una por cada pregunta que hace
el negocio. Nada llega a los consumidores sin pasar la auditoría:

```bash
python jobs/gold_build.py build     # write every table to gold/_staging/
python jobs/gold_build.py audit     # 25 checks; exits non-zero on any failure
python jobs/gold_build.py publish   # replace each published table in one Delta commit
```

| Tabla | Responde |
|---|---|
| `daily_sales` | Pedidos, unidades e ingresos por día, país y categoría |
| `payment_provider_daily`, `payment_failure_reasons` | Tasa de fallo de pagos por proveedor, y por qué fallan los pagos |
| `late_shipments` | Paquetes entregados tarde o todavía abiertos después de la fecha prometida |
| `order_lead_times` | Horas hasta el pago, hasta el despacho, en tránsito y de punta a punta, por pedido |
| `customer_ltv` | Pedidos, ingresos y ticket promedio por cliente (sin PII) |

La auditoría verifica tablas no vacías, claves no nulas y únicas, reglas de negocio (tasas de fallo en [0, 1],
sin duraciones negativas, paquetes demorados realmente demorados) y **concilia los ingresos de tres formas** (líneas de
pedido de silver, `daily_sales`, `customer_ltv`). En su primera ejecución bloqueó la publicación dos veces por motivos reales: un
pedido de origen cuyo total de cabecera no coincidía con sus líneas (los ingresos ahora se definen siempre a partir de las líneas, y
esa diferencia se reporta), y paquetes demorados cuyo retraso redondeado se mostraba como cero.

**Consultas con Athena.** Terraform crea la base de datos de Glue `shopflow_gold` y un workgroup de Athena
`cdc-lakehouse` (motor v3, resultados cifrados y expirados a los 7 días, límite de escaneo de 1 GB por consulta).
`scripts/register-gold-tables.sh` registra las tablas como tablas Delta nativas, de modo que Athena toma el esquema
del log de transacciones en lugar de una lista de columnas mantenida a mano.

```sql
SELECT provider, sum(failed) * 1.0 / sum(attempts) AS failure_rate
FROM shopflow_gold.payment_provider_daily
GROUP BY provider ORDER BY failure_rate DESC;
```

## Orquestación (Airflow)

Airflow 3.3.2 corre como un solo contenedor construido **sobre la imagen de Spark**, por lo que las tareas ejecutan los jobs de Spark
en el mismo proceso y Airflow nunca recibe el socket de Docker. `airflow standalone` (scheduler, API server, DAG
processor, triggerer) con el `LocalExecutor` y sus metadatos en su propia base de datos PostgreSQL: una
configuración de un solo nodo dimensionada para la VM de la demo, con `parallelism=1` para que dos JVM de Spark nunca compitan por memoria.

| DAG | Programación | Tareas |
|---|---|---|
| `gold_pipeline` | cada 15 min | `build` -> `audit` -> `publish` (la auditoría nunca se reintenta: un problema de datos no se arregla solo) |
| `lake_maintenance` | diario 06:30 UTC | `OPTIMIZE` + `VACUUM` (168 h) sobre bronze, silver y gold |

```bash
mkdir -m 700 docker/secrets
printf '{"lucas": "%s"}\n' "$(openssl rand -base64 18)" > docker/secrets/airflow_passwords.json
chmod 644 docker/secrets/airflow_passwords.json   # readable by the container user; the dir stays 700
docker compose -f docker/compose.yaml --profile spark build spark-bronze   # Airflow's base image
docker compose -f docker/compose.yaml --profile airflow up -d airflow       # UI on http://localhost:8085
```

`build` fija las versiones de las tablas silver en un manifiesto y `audit` usa time travel hacia esas mismas versiones, de modo que
los controles comparan datos equivalentes aunque silver siga recibiendo streaming. Verificado dejando que silver hiciera commit de nuevas
versiones entre `build` y `audit`: la auditoría usó las versiones fijadas y pasó 25/25.

## Herramientas del agente de IA (MCP)

`agent/` es un servidor [MCP](https://modelcontextprotocol.io) que permite a cualquier cliente MCP (Claude Code, Claude
Desktop, un framework de agentes) responder preguntas de negocio a partir de la capa gold. El agente nunca escribe SQL:
recibe ocho herramientas fijas y de solo lectura, una por pregunta de negocio, diseñadas en
[docs/agent-tools.md](agent-tools.md).

| Herramienta | Pregunta |
|---|---|
| `get_sales_summary` | ¿Cuánto vendimos, por día, país y categoría? |
| `get_payment_failure_rate`, `get_payment_failure_reasons` | ¿Qué proporción de pagos falla, con qué proveedor y por qué? |
| `get_late_shipments`, `get_late_shipments_by_carrier` | ¿Qué paquetes están vencidos y qué transportistas están atrasados? |
| `get_order_lead_times` | ¿Cuánto tarda un pedido en pagarse, despacharse y entregarse (promedio, p50, p90)? |
| `get_top_customers_ltv` | ¿Quiénes son los clientes más valiosos (solo por id)? |
| `data_freshness` | ¿Qué tan actualizados están los datos? |

```bash
docker compose -f docker/compose.yaml --profile agent build agent
claude   # from the repo root: .mcp.json registers the server, then just ask a question
```

Guardrails (límites), todos aplicados en código y probados sin conexión (`python -m unittest test_tools`):

- **Sin SQL libre.** Los argumentos se validan contra listas cerradas y rangos (países, proveedores,
  transportistas, fechas de hasta 92 días, como máximo 50 filas) antes de ejecutar cualquier consulta, y los valores llegan a Athena como
  `ExecutionParameters`, nunca por concatenación de strings. Los argumentos inválidos son rechazados por el esquema MCP.
- **Mínimo privilegio, aplicado por IAM y no por el prompt.** La identidad `cdc-lakehouse-agent` solo puede ejecutar
  consultas de Athena en el workgroup `cdc-lakehouse` (límite de escaneo de 1 GB) desde la IP de la VM. El acceso a Glue y S3 se
  otorga solo cuando Athena hace la llamada en su nombre (`aws:CalledVia`), por lo que la clave no puede descargar archivos
  del lake, y staging, silver y bronze están denegados explícitamente. Verificado: las consultas sobre gold funcionan; el acceso directo
  a S3, el acceso directo a Glue, otros workgroups y las llamadas a IAM son denegados.
- **Sin PII.** Los clientes se devuelven solo por `customer_id`; la lista de columnas de la herramienta de clientes nunca incluye
  emails ni dominios de email.
- **Respuestas honestas.** Cada resultado incluye notas sobre unidades (USD, fracciones, reloj de simulación comprimido),
  limitaciones conocidas (los paquetes demorados no tienen un denominador de entregas a tiempo) y frescura de los datos, y los resultados vacíos se marcan como
  "sin datos", no como cero.

Ejemplo, consultado a Claude Code con solo estas herramientas habilitadas: *"¿Qué transportistas tienen más paquetes
vencidos actualmente en Argentina, y qué tan actualizados están los datos?"*. El agente llamó a las herramientas, ubicó primero a OCA y
Andreani, señaló que el ranking es por volumen y no por tasa, convirtió el reloj comprimido a
horas simuladas e indicó que los datos correspondían a la última construcción de gold. También notó que transportistas de
otros países estaban entregando en Argentina, una peculiaridad del generador de datos que ahora figura en el roadmap.

## Infraestructura AWS (Terraform)

Dos stacks dentro de `terraform/`:

| Stack | Crea | State |
|---|---|---|
| `bootstrap/` | Bucket S3 para el state de Terraform (versionado, cifrado, solo TLS, `prevent_destroy`) | Local (copiado al bucket de state como respaldo) |
| `lake/` | Bucket del lake (`bronze/`, `silver/`, `gold/`, `_checkpoints/`), reglas de ciclo de vida, identidad IAM del pipeline | Backend S3 con lockfile nativo (sin DynamoDB) |

```bash
cd terraform/bootstrap && terraform init && terraform apply
cd ../lake
cp backend.hcl.example backend.hcl        # set the state bucket from the bootstrap output
terraform init -backend-config=backend.hcl && terraform apply
aws iam create-access-key --user-name cdc-lakehouse-pipeline   # store it outside git
```

Decisiones que vale la pena destacar:

- **Sin access keys en Terraform.** El usuario del pipeline se gestiona en código, pero su clave se crea con la
  CLI para que el secreto nunca quede en el state de Terraform.
- **Mínimo privilegio, verificado.** La identidad del pipeline puede leer, escribir y borrar solo dentro de los prefijos
  del lake. Escribir en la raíz del bucket, leer el state de Terraform, listar otros buckets, las llamadas a IAM y
  las solicitudes por HTTP sin cifrar están todas denegadas.
- **SSE-S3 en lugar de una clave KMS administrada por el cliente**, que agregaría un costo mensual fijo por clave.
- **Sin versionado de S3 en el lake.** Delta Lake mantiene su propio historial de tablas; el versionado retendría cada
  archivo que `VACUUM` elimina.
- **El id de la cuenta nunca está hardcodeado.** Los nombres de los buckets se derivan en tiempo de plan y el bucket del backend proviene de un
  `backend.hcl` ignorado por git.
