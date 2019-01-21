package com.rocketsoft.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.gephi.io.importer.api.ContainerLoader;
import org.gephi.io.importer.api.Database;
import org.gephi.io.importer.api.EdgeDraft;
import org.gephi.io.importer.api.ElementDraft;
import org.gephi.io.importer.api.NodeDraft;
import org.gephi.io.importer.api.Report;
import org.gephi.io.importer.impl.NodeDraftImpl;
import org.gephi.io.importer.spi.DatabaseImporter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 *
 * @author dprizentsov
 */
public class GremlinDatabaseImporter implements DatabaseImporter {
  GremlinDatabase database;
  ContainerLoader containerLoader;
  
  public void setDatabase(Database dtbs) {
    this.database = (GremlinDatabase)dtbs;
  }

  public Database getDatabase() {
    return database;
  }

  public boolean execute(ContainerLoader cl) {
    this.containerLoader = cl;
    containerLoader.setAllowParallelEdge(true);
    containerLoader.setAllowSelfLoop(true);
    final ElementDraft.Factory factory = cl.factory();
    /*NodeDraft a1 = factory.newNodeDraft("A1");
    NodeDraft a2 = factory.newNodeDraft("A2");
    NodeDraft a3 = factory.newNodeDraft("A3");
    cl.addNode(a1);
    cl.addNode(a2);
    cl.addNode(a3);
    EdgeDraft newEdgeDraft = factory.newEdgeDraft("A1-A2");
    newEdgeDraft.setSource(a1);newEdgeDraft.setTarget(a2);
    cl.addEdge(newEdgeDraft);*/
    
    
    Cluster cluster = Cluster.build("127.0.0.1").port(8182).create();
    Client client = cluster.connect();
    
    final Map<Long, NodeDraft> nodes = new HashMap<Long, NodeDraft>();
    try {
      client.submit("g.V()").all().get().forEach(new Consumer<Result>() {
        @Override
        public void accept(Result r) {
          Vertex v = (Vertex)r.getObject();
          long id = (Long)v.id();
          String label = v.label();
          String keyval = "";
          Set<String> keys = v.keys();
          for (String k : keys) {
            Iterator<VertexProperty<Object>> properties = v.properties(k);
            String val = "[";
            while (properties.hasNext()) {
              Object value = properties.next().value();
              val += value + ",";
            }
            val += "]";
            Object value = val;
            keyval += k + "=" + value + ";";
          }
          //System.out.println("V id=" + id + " label=" + label + " " + keyval);
          NodeDraft a1 = factory.newNodeDraft(label + ": " + keyval);
          nodes.put(id, a1);
          GremlinDatabaseImporter.this.containerLoader.addNode(a1);
        }
      });
      
      client.submit("g.E()").all().get().forEach(new Consumer<Result>() {
        @Override
        public void accept(Result t) {
          Edge e = (Edge)t.getObject();
          long id = (Long)e.id();
          String label = e.label();
          String keyval = "";
          Set<String> keys = e.keys();
          for (String k : keys) {
            Object value = e.value(k);
            keyval += k + "=" + value;
          }
          Vertex outVertex = e.outVertex();long outId = outVertex == null ? -1 : (Long)outVertex.id();
          Vertex inVertex = e.inVertex();long inId = inVertex == null ? -1 : (Long)inVertex.id();
          //System.out.println("E id=" + id + " from=" + outId + " to=" + inId + " label=" + label + " " + keyval);
          
          NodeDraft n0 = nodes.get(outId);
          NodeDraft n1 = nodes.get(inId);
          if (n0 != null && n1 != null) {
            EdgeDraft edge = factory.newEdgeDraft(label + ":" + outId + "-" + outId);
            edge.setSource(n0);edge.setTarget(n1);
            edge.setType(label);
            GremlinDatabaseImporter.this.containerLoader.addEdge(edge);
          }
        }
      });
    } catch(Exception ex) {
      ex.printStackTrace();
    }
    return true;
  }

  public ContainerLoader getContainer() {
    return containerLoader;
  }

  public Report getReport() {
    return new Report();
  }
  
}
