package model;

import lombok.Data;

@Data
public class HostLog {
    private Long createdTime;
    private String metricItem;
    private Integer metricValue;

}
