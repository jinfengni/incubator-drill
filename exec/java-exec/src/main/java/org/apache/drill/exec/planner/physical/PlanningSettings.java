package org.apache.drill.exec.planner.physical;

public class PlanningSettings {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlanningSettings.class);

  private static ThreadLocal<PlanningSettings> settings = new ThreadLocal<>();

  private boolean singleMode;

  public static PlanningSettings get(){
    PlanningSettings s = settings.get();
    if(s == null){
      s = new PlanningSettings();
      settings.set(s);
    }

    return s;
  }

  public boolean isSingleMode() {
    return singleMode;
  }

  public void setSingleMode(boolean singleMode) {
    this.singleMode = singleMode;
  }




}
