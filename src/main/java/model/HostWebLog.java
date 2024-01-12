package model;

import lombok.Data;

@Data
public class HostWebLog {
    private Long id;
    private Long createdTime;
    private String metricItem;
    private Integer metricValue;
    private String log;

}
