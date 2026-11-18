# Actividad_1.3.3 — Pipeline de eventos del caso end-to-end

Sistema de monitoreo de transacciones bancarias en tiempo real construido con Spring Boot, Kafka Streams y Docker.

> **Nombres:** el nombre visible del proyecto es `Actividad_1.3.3`. Los identificadores técnicos usan `actividad_1_3_3` (artefacto Maven, proyecto Compose y paquetes Java) porque los puntos no son válidos en paquetes Java ni en nombres de proyecto de Compose.

## Flujo implementado

1. Entrada: `banco-transacciones` (eventos `Transaccion` en JSON).
2. Filtro: se ignoran transacciones con `monto <= 10`.
3. Detección: transacciones con `monto > 10000` se transforman en `AlertaFraude` (severidad `ALTA`).
4. Salida: `banco-alertas`.

> La introducción de la guía menciona también enriquecimiento a USD y agregación por usuario, y llama `alertas-fraude` al topic final. La implementación sigue el código paso a paso del documento: filtro, detección y topic `banco-alertas`.

## Requisitos

- JDK 25
- Docker Desktop en ejecución
- Maven (o el wrapper `mvnw.cmd` incluido)

Si `mvnw.cmd` indica `JAVA_HOME not found`, configúralo antes en la misma terminal:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-25.0.3'
```

## 1. Levantar Kafka y Kafka UI

```powershell
docker compose up -d
```

Servicios:

| Servicio | URL |
| --- | --- |
| Kafka (host) | `localhost:9092` |
| Kafka UI | http://localhost:8081 |

> Kafka UI usa el puerto 8081 porque el puerto 8080 está reservado para la aplicación Spring Boot.
>
> El servicio `kafka-setup` crea `banco-transacciones` y `banco-alertas` antes de que arranque la aplicación. Kafka Streams necesita que el topic de entrada exista al iniciar; si no, se detiene con `MissingSourceTopicException`.

## 2. Compilar y probar

```powershell
.\mvnw.cmd clean test
```

Las pruebas unitarias (`TopologyTestDriver`) verifican los tres casos: monto 5 (filtrada), monto 500 (sin alerta) y monto 15000 (alerta `ALTA`).

## 3. Ejecutar la aplicación

```powershell
.\mvnw.cmd spring-boot:run
```

## 4. Enviar transacciones de prueba

Desde PowerShell:

```powershell
$headers = @{ 'Content-Type' = 'application/json' }

# Normal: se procesa, pero NO genera alerta
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/banco/transaccion' -Headers $headers -Body '{"usuario":"juan","monto":500,"origen":"app"}'

# Fraudulenta: genera alerta
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/banco/transaccion' -Headers $headers -Body '{"usuario":"juan","monto":15000,"origen":"cajero"}'

# Ignorada: no aparece en consola (5 < 10)
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/banco/transaccion' -Headers $headers -Body '{"usuario":"juan","monto":5,"origen":"app"}'
```

Equivalente con `curl` en bash/CMD:

```bash
curl -X POST http://localhost:8080/api/banco/transaccion -H "Content-Type: application/json" -d '{"usuario":"juan","monto":15000,"origen":"cajero"}'
```

Salida esperada en la consola de la aplicación:

```text
✅ Transacción procesada: 500.0
✅ Transacción procesada: 15000.0
🚨 ALERTA GENERADA: Transacción inusualmente alta en cajero
```

## 5. Verificar en Kafka UI

1. Abrir http://localhost:8081
2. Seleccionar el cluster `local` y el topic `banco-alertas`.
3. Pestaña **Messages**: debe aparecer el evento transformado:

```json
{
  "usuario": "juan",
  "monto": 15000.0,
  "motivo": "Transacción inusualmente alta en cajero",
  "severidad": "ALTA"
}
```

## 6. Detener los servicios

```powershell
docker compose down
```
