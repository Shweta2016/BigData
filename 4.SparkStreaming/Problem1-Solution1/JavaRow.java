package nu1;

import scala.Tuple2;

public class JavaRow implements java.io.Serializable {
	  private String time;
	  private String octate;
	  public String getTime() {
		return time;
	}

	public void setTime(Tuple2<String, String> word2) {
		this.time = word2._1;
	}

	public String getOctate() {
		return octate;
	}

	public void setOctate(Tuple2<String, String> word2) {
		this.octate = word2._2;
	}

	}
