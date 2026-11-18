package com.actividad_1_3_3.config;

import com.actividad_1_3_3.model.AlertaFraude;
import com.actividad_1_3_3.model.Transaccion;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaccion> transacciones;
    private TestOutputTopic<String, AlertaFraude> alertas;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "actividad_1_3_3-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder builder = new StreamsBuilder();
        new PipelineTopology().kStreamPipeline(builder);
        driver = new TopologyTestDriver(builder.build(), props);

        JsonSerializer<Transaccion> serializadorTx = new JsonSerializer<>();
        JsonDeserializer<AlertaFraude> deserializadorAlerta =
            new JsonDeserializer<>(AlertaFraude.class).trustedPackages("*");

        transacciones = driver.createInputTopic(
            "banco-transacciones", new StringSerializer(), serializadorTx);
        alertas = driver.createOutputTopic(
            "banco-alertas", new StringDeserializer(), deserializadorAlerta);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void transaccionMenorA10EsFiltrada() {
        transacciones.pipeInput("juan", new Transaccion("juan", 5.0, "app", 1L));

        assertThat(alertas.isEmpty()).isTrue();
    }

    @Test
    void transaccionNormalNoGeneraAlerta() {
        transacciones.pipeInput("juan", new Transaccion("juan", 500.0, "app", 2L));

        assertThat(alertas.isEmpty()).isTrue();
    }

    @Test
    void transaccionMayorA10000GeneraAlerta() {
        transacciones.pipeInput("juan", new Transaccion("juan", 15000.0, "cajero", 3L));

        AlertaFraude alerta = alertas.readValue();

        assertThat(alerta.getUsuario()).isEqualTo("juan");
        assertThat(alerta.getMonto()).isEqualTo(15000.0);
        assertThat(alerta.getMotivo()).isEqualTo("Transacción inusualmente alta en cajero");
        assertThat(alerta.getSeveridad()).isEqualTo("ALTA");
        assertThat(alertas.isEmpty()).isTrue();
    }
}
