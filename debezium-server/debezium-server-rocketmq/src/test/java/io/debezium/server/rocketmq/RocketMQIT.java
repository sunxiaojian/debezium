package io.debezium.server.rocketmq;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.event.Observes;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.server.TestConfigSource;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RocketMQTestResourceLifecycleManager.class)
public class RocketMQIT {

    private static final int MESSAGE_COUNT = 4;

    private static final String TOPIC_NAME = "testc.inventory.customers";

    private DefaultLitePullConsumer consumer = null;

    {
        Testing.Files.delete(TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RocketMQTestConfigSource.OFFSET_STORE_PATH);
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException {
        Testing.Print.enable();

        consumer = new DefaultLitePullConsumer(RocketMQTestResourceLifecycleManager.getGroup());
        consumer.setNamesrvAddr(RocketMQTestResourceLifecycleManager.getNamesrvAddr());
        String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(true);
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw new RuntimeException(event.getError().get());
        }
    }

    @Test
    public void testRocketMQ() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(RocketMQTestConfigSource.waitForSeconds())).until(() -> {
            return consumer != null;
        });
        // start consumer
        this.consumer.subscribe(TOPIC_NAME, "*");
        consumer.start();

        // consume record
        final List<MessageExt> records = new ArrayList<>();
        Awaitility.await().atMost(Duration.ofSeconds(RocketMQTestConfigSource.waitForSeconds())).until(() -> {
            records.addAll(this.consumer.poll(5000));
            return records.size() >= MESSAGE_COUNT;
        });
        assertThat(records.size()).isGreaterThanOrEqualTo(MESSAGE_COUNT);
    }
}
