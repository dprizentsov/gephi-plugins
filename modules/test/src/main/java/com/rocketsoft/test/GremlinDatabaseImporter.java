package com.rocketsoft.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
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
import org.json.JSONArray;
import org.json.JSONObject;
import sun.net.www.http.HttpClient;

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
  public String serviceUrl;
  
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
    
    
    importNewton();
    return true;
  }
  
  private void importNewton() {
    try {
      importNewton("g.V().toList()");
      importNewton("g.E().toList()");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void importNewton(String query) throws ProtocolException, MalformedURLException, IOException {
    URL url = new URL(serviceUrl + "?" + query);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    con.connect();
    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuilder content = new StringBuilder();
    while ((inputLine = in.readLine()) != null) {
      content.append(inputLine);
    }
    in.close();
    //System.out.println(content);
    JSONObject contectJson = new JSONObject(content.toString());
    JSONObject statusJson = contectJson.getJSONObject("status");
    if (statusJson != null && statusJson.getInt("code") == 200) {
      JSONObject resultJson = contectJson.getJSONObject("result");
      if (resultJson != null) {
        JSONArray dataJsonArray = resultJson.getJSONArray("data");
        for (int i = 0; i < dataJsonArray.length(); i++) {
          JSONObject graphElementJson = dataJsonArray.getJSONObject(i);
          handleGraphElementJson(graphElementJson);
        }
      } else {
        throw new RuntimeException("Empty result");
      }
    } else {
      throw new RuntimeException("status: " + statusJson);
    }
  }
  
  private void handleGraphElementJson(JSONObject graphElementJson) {
    ElementDraft gephiElement;
    int gtaphObjectId = graphElementJson.getInt("id");
    String gtaphObjectIdString = Integer.toString(gtaphObjectId);
    String gtaphObjectLabel = graphElementJson.getString("label");
    String gtaphObjectType = graphElementJson.getString("type");
    Map<String, Object> propertiesMap = null;
    if (graphElementJson.has("properties")) {
      propertiesMap = new HashMap<String, Object>();
      JSONObject gtaphObjectProperties = graphElementJson.getJSONObject("properties");
      for (String propertyName : gtaphObjectProperties.keySet()) {
        Object propertyValue = gtaphObjectProperties.getJSONArray(propertyName).getJSONObject(0).get("value");
        propertiesMap.put(propertyName, propertyValue);
      }
    }
    if ("vertex".equals(gtaphObjectType)) {
      //System.out.println("vertex " + gtaphObjectLabel);
      NodeDraft gephiNode = GremlinDatabaseImporter.this.containerLoader.getNode(gtaphObjectIdString);
      if (gephiNode == null) {
        gephiNode = factory.newNodeDraft(gtaphObjectIdString);
        GremlinDatabaseImporter.this.containerLoader.addNode(gephiNode);
      }
      setLabel(gephiNode, gtaphObjectLabel);
      setAttributes(gephiNode, propertiesMap);
      gephiElement = gephiNode;
    } else if ("edge".equals(gtaphObjectType)) {
      //System.out.println("edge " + gtaphObjectLabel);
      EdgeDraft gephiEdge = GremlinDatabaseImporter.this.containerLoader.getEdge(gtaphObjectIdString);
      if (gephiEdge == null) {
        int outVertexId = graphElementJson.getInt("outV");
        int inVertexId = graphElementJson.getInt("inV");
        String outVertexIdString = Integer.toString(outVertexId);
        String inVertexIdString = Integer.toString(inVertexId);
        NodeDraft gephiNodeOut = GremlinDatabaseImporter.this.containerLoader.getNode(outVertexIdString);
        NodeDraft gephiNodeIn = GremlinDatabaseImporter.this.containerLoader.getNode(inVertexIdString);
        if (gephiNodeOut != null && gephiNodeIn != null) {
          gephiEdge = factory.newEdgeDraft(gtaphObjectIdString);
          gephiEdge.setSource(gephiNodeOut);gephiEdge.setTarget(gephiNodeIn);
          setLabel(gephiEdge, gtaphObjectLabel);
          setAttributes(gephiEdge, propertiesMap);
          GremlinDatabaseImporter.this.containerLoader.addEdge(gephiEdge);
        } else {
          if (gephiNodeOut == null) {
            System.out.println("### Source Node not found " + outVertexIdString);
          }
          if (gephiNodeIn == null) {
            System.out.println("### Target Node not found " + inVertexIdString);
          }
        }
      }
      gephiElement = gephiEdge;
    } else {
      System.out.println("### Unknown type of object  " + gtaphObjectType);
    }
  }
  
  private void importGremlin() {
    Cluster.Builder builder = Cluster.build(address);
    builder.port(Integer.parseInt(port));
    if (username != null && !username.isEmpty() && password != null) {
      builder.credentials(username, password);
    }
    Cluster cluster = builder.create();
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
