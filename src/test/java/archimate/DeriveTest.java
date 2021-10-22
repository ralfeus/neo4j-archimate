package archimate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.types.Relationship;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DeriveTest {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withFunction(Derive.class)
                .build();
    }

    @Test
    void deriveRelationships() {
        // This is in a try-block, to make sure we close the driver after the test
        try(Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
            Session session = driver.session()) {

            // When
			TransactionConfig trnCfg = TransactionConfig.empty();
			session.run("CREATE(:NODE) -[:RC_REALIZATION]-> (:TECH_SVC) -[:RC_SERVING] -> (:APP_COMP)");
			long id1 = session.run("MATCH (n:NODE) RETURN n").single().get("n").asNode().id();
			long id2 = session.run("MATCH (n:APP_COMP) RETURN n").single().get("n").asNode().id();
            Relationship result = session.run("MATCH (n1:NODE), (n2:TECH_SVC), (n3:APP_COMP) RETURN archimate.derive([n1, n2, n3]) AS result").single().get("result").asRelationship();

            // Then
			assertThat(result.startNodeId()).isEqualTo(id1);
			assertThat(result.endNodeId()).isEqualTo(id2);
            assertThat(result.type()).isEqualTo("RC_SERVING");
        }
    }
}