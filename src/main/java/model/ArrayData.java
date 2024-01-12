package model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ArrayData {
    private Long id;
    private List<Map<String, Object>> cpus;
    private Map<String, Object> people;
}
