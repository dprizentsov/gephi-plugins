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
  
  private javax.swing.JTextField addressText;
  private javax.swing.JLabel jLabel1;
  private javax.swing.JLabel jLabel2;
  private javax.swing.JLabel jLabel3;
  private javax.swing.JLabel jLabel4;
  private javax.swing.JScrollPane jScrollPane1;
  private javax.swing.JPasswordField passwordText;
  private javax.swing.JTextField portText;
  private javax.swing.JEditorPane queryText;
  private javax.swing.JTextField usernameText;
  
  private void initComponents(final JPanel that) {

    addressText = new javax.swing.JTextField();
    jLabel1 = new javax.swing.JLabel();
    portText = new javax.swing.JTextField();
    passwordText = new javax.swing.JPasswordField();
    usernameText = new javax.swing.JTextField();
    jLabel2 = new javax.swing.JLabel();
    jLabel3 = new javax.swing.JLabel();
    jLabel4 = new javax.swing.JLabel();
    jScrollPane1 = new javax.swing.JScrollPane();
    queryText = new javax.swing.JEditorPane();

    addressText.setText("127.0.0.1");

    jLabel1.setText("Address");

    portText.setText("8182");

    passwordText.setText("password");

    usernameText.setText("administrator");

    jLabel2.setText("Username");

    jLabel3.setText("Password");

    jLabel4.setText("Query");

    queryText.setText("g.V()");
    jScrollPane1.setViewportView(queryText);

    javax.swing.GroupLayout layout = new javax.swing.GroupLayout(that);
    that.setLayout(layout);
    layout.setHorizontalGroup(
      layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
      .addGroup(layout.createSequentialGroup()
        .addContainerGap()
        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
          .addComponent(jLabel1)
          .addComponent(jLabel2)
          .addComponent(jLabel3)
          .addComponent(jLabel4))
        .addGap(55, 55, 55)
        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
          .addGroup(layout.createSequentialGroup()
            .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
              .addComponent(usernameText, javax.swing.GroupLayout.DEFAULT_SIZE, 127, Short.MAX_VALUE)
              .addComponent(passwordText)
              .addComponent(addressText))
            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
            .addComponent(portText, javax.swing.GroupLayout.PREFERRED_SIZE, 67, javax.swing.GroupLayout.PREFERRED_SIZE))
          .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 300, javax.swing.GroupLayout.PREFERRED_SIZE))
        .addContainerGap(24, Short.MAX_VALUE))
    );
    layout.setVerticalGroup(
      layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
      .addGroup(layout.createSequentialGroup()
        .addGap(22, 22, 22)
        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
          .addComponent(addressText, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
          .addComponent(jLabel1)
          .addComponent(portText, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
          .addComponent(usernameText, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
          .addComponent(jLabel2))
        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
          .addComponent(passwordText, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
          .addComponent(jLabel3))
        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
          .addComponent(jLabel4)
          .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 110, javax.swing.GroupLayout.PREFERRED_SIZE))
        .addContainerGap(30, Short.MAX_VALUE))
    );
  }

  //getPanel
  //setup
  //unsetup
  //importer.execute
  
  GremlinDatabaseImporter importer;
  
  public void setup(Importer[] imprtrs) {
    System.out.println("setup " + Arrays.asList(imprtrs));//setup [com.rocketsoft.test.GremlinDatabaseImporter@7c8eff57]
    //TODO get default values from importer put them on dialog
    importer = (GremlinDatabaseImporter)imprtrs[0];
  }

  public JPanel getPanel() {
    System.out.println("getPanel");
    JPanel panel = new JPanel();
    initComponents(panel);
    return panel;//TODO add address:poer, user/password, request
  }

  public void unsetup(boolean bln) {
    System.out.println("unsetup " + bln);//Ok == true, Cancel == false
    //TODO put user values to importer
    if (bln) {
      importer.address = addressText.getText();
      importer.port = portText.getText();
      importer.username = usernameText.getText();
      importer.password = passwordText.getText();
      importer.query = queryText.getText();
    }
  }

  public String getDisplayName() {
    return "Connect to gremlin server...";
  }

  public boolean isUIForImporter(Importer imprtr) {
    return imprtr instanceof GremlinDatabaseImporter;
  }
  
}
