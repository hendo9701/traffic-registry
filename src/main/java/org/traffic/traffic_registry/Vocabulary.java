package org.traffic.traffic_registry;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.util.Values;

public final class Vocabulary {

  public static final Namespace SOSA = Values.namespace("sosa", "http://www.w3.org/ns/sosa/");

  public static final IRI SENSOR = Values.iri(SOSA, "sensor");

  public static final Namespace QU = Values.namespace("qu", "http://purl.oclc.org/NET/ssnx/qu/qu#");

  public static final Namespace IOT_LITE =
      Values.namespace("iot-lite", "http://purl.oclc.org/NET/UNIS/fiware/iot-lite#");

  public static final IRI HAS_UNIT = Values.iri(IOT_LITE, "hasUnit");

  public static final IRI HAS_QUANTITY_KIND = Values.iri(IOT_LITE, "hasQuantityKind");

  private Vocabulary() {}
}
