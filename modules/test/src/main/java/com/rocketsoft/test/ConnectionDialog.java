package com.rocketsoft.test;

import java.util.Arrays;
import javax.swing.JPanel;
import javax.swing.JTextField;
import org.gephi.io.importer.spi.Importer;
import org.gephi.io.importer.spi.ImporterUI;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author dprizentsov
 */
@ServiceProvider(service = ImporterUI.class)
public class ConnectionDialog implements ImporterUI {

  //getPanel
  //setup
  //unsetup
  //importer.execute
  
  GremlinDatabaseImporter importer;
  public JTextField queryTextField;
  
  public void setup(Importer[] imprtrs) {
    System.out.println("setup " + Arrays.asList(imprtrs));//setup [com.rocketsoft.test.GremlinDatabaseImporter@7c8eff57]
    //TODO get default values from importer put them on dialog
    importer = (GremlinDatabaseImporter)imprtrs[0];
  }

  public JPanel getPanel() {
    System.out.println("getPanel");
    queryTextField = new JTextField();
    queryTextField.setText("g.E()");
    JPanel panel = new JPanel();
    panel.add(queryTextField);
    return panel;//TODO add address:poer, user/password, request
  }

  public void unsetup(boolean bln) {
    System.out.println("unsetup " + bln);//Ok == true, Cancel == false
    //TODO put user values to importer
    if (bln) {
      importer.query = queryTextField.getText();
    }
  }

  public String getDisplayName() {
    return "Connect to gremlin server...";
  }

  public boolean isUIForImporter(Importer imprtr) {
    return imprtr instanceof GremlinDatabaseImporter;
  }
  
}
