package com.rocketsoft.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
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
    final ElementDraft.Factory factory = cl.factory();
    
    Cluster cluster = Cluster.build("127.0.0.1").port(8182).create();
    Client client = cluster.connect();
    
    final Map<Long, NodeDraft> nodes = new HashMap<Long, NodeDraft>();
    /*try {
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
    }*/
    
    ResultSet resultSet = client.submit(query);
    resultSet.forEach(new Consumer<Result>() {
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
    });
    return true;
  }
  
  private void handleElement(Element gremlinElement) {
    ElementDraft.Factory factory = containerLoader.factory();
    
    Object gremlinId = gremlinElement.id();
    String gremlinIdStr = gremlinId.toString();
    String gremlinLabel = gremlinElement.label();
    Map<String, Object> gremlinAttributes = null;
    Set<String> attributeKeys = gremlinElement.keys();
    if (attributeKeys != null && !attributeKeys.isEmpty()) {
      gremlinAttributes = new HashMap<String, Object>();
      for (String attributeKey : attributeKeys) {
        Iterator pi = gremlinElement.properties(attributeKey);
        int i = 0;
        while(pi.hasNext()) {
          Property p = (Property)pi.next();
          Object val = p.value();
          gremlinAttributes.put(attributeKey + "." + i, val);//TODO find out how to show vectorproperty better
          i++;
        }
      }
    }

    //System.out.println("### " + gremlinIdStr + " " + gremlinLabel);

    if (gremlinElement instanceof Vertex) {
      Vertex gremlinVertex = (Vertex)gremlinElement;
      NodeDraft gephiNode = factory.newNodeDraft(gremlinIdStr);
      setLabel(gephiNode, gremlinLabel);
      setAttributes(gephiNode, gremlinAttributes);
      GremlinDatabaseImporter.this.containerLoader.addNode(gephiNode);
    } else if (gremlinElement instanceof Edge) {
      Edge gremlinEdge = (Edge)gremlinElement;
      Vertex outGremlinVertex = gremlinEdge.outVertex();Object outId = outGremlinVertex == null ? -1 : outGremlinVertex.id();
      Vertex inGremlinVertex = gremlinEdge.inVertex();Object inId = inGremlinVertex == null ? -1 : inGremlinVertex.id();
      if (outId != null && inId != null) {
        NodeDraft gephiNodeOut = GremlinDatabaseImporter.this.containerLoader.getNode(outId.toString());
        NodeDraft gephiNodeIn = GremlinDatabaseImporter.this.containerLoader.getNode(inId.toString());
        if (gephiNodeOut != null && gephiNodeIn != null) {
          EdgeDraft gephiEdge = factory.newEdgeDraft(gremlinIdStr);
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
    } else {
      System.out.println("### unknown gremlin element type " + gremlinElement.getClass().getName());
    }
  }
  
  private static void setAttributes(ElementDraft gephiElement, Map<String, Object> gremlinAttributes) {
    if (gremlinAttributes != null) {
      for (Entry<String, Object> entry : gremlinAttributes.entrySet()) {
        gephiElement. setValue(entry.getKey(), entry.getValue());
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
