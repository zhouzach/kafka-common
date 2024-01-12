package model;

import lombok.Data;

@Data
public class WebLog {
    private Long id;
    private String log;
    private Long createdTime;
}
