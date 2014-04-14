package org.apache.drill.exec.rpc.user;

import java.io.IOException;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.store.SchemaFactory;

public class UserSession {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSession.class);

  private DrillUser user;
  private String defaultSchema = "";

  public UserSession(UserCredentials credentials, SchemaFactory factory) throws IOException{
  }

  public DrillUser getUser(){
    return user;
  }


  /**
   * Update the schema path for the session.
   * @param fullPath The desired path to set to.
   * @param schema The root schema to find this path within.
   * @return true if the path was set succesfully.  false if this path was unavailable.
   */
  public boolean setDefaultSchemaPath(String fullPath, SchemaPlus schema){
    SchemaPlus newDefault = getDefaultSchema(schema);
    if(newDefault == null) return false;
    this.defaultSchema = fullPath;
    return true;
  }

  public SchemaPlus getDefaultSchema(SchemaPlus rootSchema){
    String[] paths = defaultSchema.split("\\.");
    SchemaPlus schema = rootSchema;
    for(String p : paths){
      schema = schema.getSubSchema(p);
      if(schema == null) break;
    }
    return schema;
  }
}
