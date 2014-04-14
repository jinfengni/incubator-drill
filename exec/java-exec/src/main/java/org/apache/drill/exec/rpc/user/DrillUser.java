package org.apache.drill.exec.rpc.user;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;

public class DrillUser {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillUser.class);

  private UserGroupInformation hadoopUser;

  public DrillUser(String userName) throws IOException {
    this.hadoopUser = UserGroupInformation.createProxyUser(userName, UserGroupInformation.getCurrentUser());
  }

  public UserGroupInformation getHadoopUser(){
    return hadoopUser;
  }
}
