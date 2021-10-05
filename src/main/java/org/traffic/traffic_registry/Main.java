package org.traffic.traffic_registry;

import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.rdf4j.StardogRepository;
import lombok.val;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.StringWriter;

import static org.traffic.traffic_registry.Vocabulary.GEO;
import static org.traffic.traffic_registry.Vocabulary.IOT_STREAM;

public class Main {

  public static void main(String[] args) {
    val databaseName = "traffic";
    val databaseUrl = "http://localhost:5820/traffic";
    val server = "http://localhost:5820";
    val username = "admin";
    val password = "admin";

    try (val adminConnection =
        AdminConnectionConfiguration.toServer(server).credentials(username, password).connect()) {
      if (!adminConnection.list().contains(databaseName)) {
        adminConnection.newDatabase(databaseName).create();
      }
    }

    val repository =
        new StardogRepository(
            ConnectionConfiguration.from(databaseUrl).credentials(username, password));

    if (repository.isInitialized()) repository.initialize();

    val namespace = "http://localhost:9987/api/v1";
    val prefix = "traffic";

    try (val connection = repository.getConnection()) {
      val queryString =
          String.format(
              "PREFIX iot-stream: <%s> \n"
                  + "PREFIX geo: <%s> \n"
                  + "PREFIX xmlschema: <%s>\n"
                  + "CONSTRUCT {\n"
                  + "    ?sensor rdf:type iot-stream:IotStream;\n"
                  + "            iot-stream:windowStart ?streamStart;\n"
                  + "            iot-stream:generatedBy ?generatedBy;\n"
                  + "            geo:location ?point.\n"
                  + "    ?point rdf:type geo:Point;\n"
                  + "           geo:lat ?lat;\n"
                  + "           geo:long ?long.\n"
                  + "} WHERE {\n"
                  + "        ?sensor rdf:type iot-stream:IotStream;\n"
                  + "            iot-stream:windowStart ?streamStart;\n"
                  + "            iot-stream:generatedBy ?generatedBy;\n"
                  + "            geo:location ?point.\n"
                  + "        ?point rdf:type geo:Point;\n"
                  + "            geo:lat ?lat;\n"
                  + "            geo:long ?long.\n"
                  + "}",
              IOT_STREAM.getName(), GEO.getName(), XSD.DATETIME.getLocalName());

      val tupleQuery = connection.prepareGraphQuery(queryString);
      //      System.out.println(queryString);
      try (val result = tupleQuery.evaluate()) {
        val model = QueryResults.asModel(result);
        val ww = new StringWriter();
        model.setNamespace(IOT_STREAM);
        model.setNamespace(GEO);
        model.setNamespace(Values.namespace(XSD.PREFIX, XSD.NAMESPACE));
        Rio.write(model, ww, RDFFormat.TURTLE);
        System.out.println(ww);
      }
    }
  }
}
