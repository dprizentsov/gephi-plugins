package com.rocketsoft.test;


import org.gephi.io.importer.spi.DatabaseImporter;
import org.gephi.io.importer.spi.DatabaseImporterBuilder;
import org.openide.util.lookup.ServiceProvider;
/**
 *
 * @author dprizentsov
 */
@ServiceProvider(service = DatabaseImporterBuilder.class)
public class GremlinDatabaseImporterBuilder implements DatabaseImporterBuilder {
 
  public DatabaseImporter buildImporter() {
    return new GremlinDatabaseImporter();
  }

  public String getName() {
    return "Gremlin Database Importer";
  }
  
}
