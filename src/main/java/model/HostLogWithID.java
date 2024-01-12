package model;

import lombok.Data;

@Data
public class HostLogWithID {
    private Long id;
    private Long createdTime;
    private String metricItem;
    private Integer metricValue;

}
