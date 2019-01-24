package com.rocketsoft.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.gephi.io.importer.api.ContainerLoader;
import org.gephi.io.importer.api.Database;
import org.gephi.io.importer.api.EdgeDraft;
import org.gephi.io.importer.api.ElementDraft;
import org.gephi.io.importer.api.NodeDraft;
import org.gephi.io.importer.api.Report;
import org.gephi.io.importer.spi.DatabaseImporter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *
 * @author dprizentsov
 */
public class GremlinDatabaseImporter implements DatabaseImporter {
  GremlinDatabase database;
  ContainerLoader containerLoader;
  ElementDraft.Factory factory;
  public String address;
  public String port;
  public String username;
  public String password;
  public String query;
  
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
    factory = cl.factory();
    
    Cluster cluster = Cluster.build(address).port(Integer.parseInt(port)).create();
    Client client = cluster.connect();
    
    Consumer<Result> resultHandler = new Consumer<Result>() {
      @Override
      public void accept(Result r) {
        Object resultObject = r.getObject();
        if (resultObject instanceof Element) {
          handleElement((Element)resultObject);
        } else if (resultObject instanceof Path) {
          handlePath((Path)resultObject);
        } else {
          System.out.println("### unknown gremlin result type " + resultObject.getClass().getName());
        }
      }
    };
    client.submit("g.E()").forEach(resultHandler);//get entire griph without node attributes and uncinnected nodes
    if (query != null && !query.isEmpty()) {//if user specified a request
      client.submit(query).forEach(resultHandler);
    }
    
    return true;
  }
  
  private ElementDraft handleElement(Element gremlinElement) {
    ElementDraft gephiElement;
    Object gremlinId = gremlinElement.id();
    String gremlinIdStr = gremlinId.toString();
    String gremlinLabel = gremlinElement.label();
    Map<String, Object> gremlinAttributes = null;
    Set<String> attributeKeys = gremlinElement.keys();
    if (attributeKeys != null && !attributeKeys.isEmpty()) {
      gremlinAttributes = new HashMap<String, Object>();
      for (String attributeKey : attributeKeys) {
        Iterator pi = gremlinElement.properties(attributeKey);
        ArrayList<Object> values = new ArrayList<Object>();
        while(pi.hasNext()) {
          Property p = (Property)pi.next();
          Object val = p.value();
          values.add(val);
        }
        gremlinAttributes.put(attributeKey, values);
      }
    }

    //System.out.println("### " + gremlinIdStr + " " + gremlinLabel);

    if (gremlinElement instanceof Vertex) {
      Vertex gremlinVertex = (Vertex)gremlinElement;
      NodeDraft gephiNode = GremlinDatabaseImporter.this.containerLoader.getNode(gremlinIdStr);
      if (gephiNode == null) {
        gephiNode = factory.newNodeDraft(gremlinIdStr);
        GremlinDatabaseImporter.this.containerLoader.addNode(gephiNode);
      }
      setLabel(gephiNode, gremlinLabel);
      setAttributes(gephiNode, gremlinAttributes);
      gephiElement = gephiNode;
    } else if (gremlinElement instanceof Edge) {
      Edge gremlinEdge = (Edge)gremlinElement;
      EdgeDraft gephiEdge = GremlinDatabaseImporter.this.containerLoader.getEdge(gremlinIdStr);
      if (gephiEdge == null) {
        Vertex outGremlinVertex = gremlinEdge.outVertex();
        Object outId = null;
        if (outGremlinVertex != null) {
          handleElement(outGremlinVertex);
          outId = outGremlinVertex.id();
        }
        Vertex inGremlinVertex = gremlinEdge.inVertex();
        Object inId = null;
        if (inGremlinVertex != null) {
          handleElement(inGremlinVertex);
          inId = inGremlinVertex.id();
        }
        if (outId != null && inId != null) {
          NodeDraft gephiNodeOut = GremlinDatabaseImporter.this.containerLoader.getNode(outId.toString());
          NodeDraft gephiNodeIn = GremlinDatabaseImporter.this.containerLoader.getNode(inId.toString());
          if (gephiNodeOut != null && gephiNodeIn != null) {
            gephiEdge = factory.newEdgeDraft(gremlinIdStr);
            gephiEdge.setSource(gephiNodeOut);gephiEdge.setTarget(gephiNodeIn);
            setLabel(gephiEdge, gremlinLabel);
            setAttributes(gephiEdge, gremlinAttributes);
            GremlinDatabaseImporter.this.containerLoader.addEdge(gephiEdge);
          } else {
            if (gephiNodeOut == null) {
              System.out.println("### Source Node not found " + outId.toString());
            }
            if (gephiNodeIn == null) {
              System.out.println("### Target Node not found " + inId.toString());
            }
          }
        } else {
          if (outId == null) {
            System.out.println("### Source Node id is null");
          }
          if (inId == null) {
            System.out.println("### Target Node id is null");
          }
        }
      }
      gephiElement = gephiEdge;
    } else {
      System.out.println("### unknown gremlin element type " + gremlinElement.getClass().getName());
      gephiElement = null;
    }
    return gephiElement;
  }
  
  private static void setAttributes(ElementDraft gephiElement, Map<String, Object> gremlinAttributes) {
    if (gremlinAttributes != null) {
      for (Entry<String, Object> entry : gremlinAttributes.entrySet()) {
        gephiElement.setValue(entry.getKey(), entry.getValue().toString());
      }
    }
  }
  
  private static void setLabel(ElementDraft gephiElement, String label) {
    gephiElement.setLabel(label);
  }
  
  private void handlePath(Path gremlinPath) {
    List<Object> objects = gremlinPath.objects();
    for(Object obj : objects) {
      if (obj instanceof Element) {
        handleElement((Element)obj);
      } else {
        System.out.println("### unknown object from Path " + obj.getClass().getName());
      }
    } 
  }

  public ContainerLoader getContainer() {
    return containerLoader;
  }

  public Report getReport() {
    return new Report();
  }
  
}
