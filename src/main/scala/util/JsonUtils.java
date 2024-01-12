package util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;


public class JsonUtils {

    private static ObjectMapper objectMapper = null;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));

        //忽略NULL值
        //objectMapper.setDefaultPropertyInclusion(
        //        JsonInclude.Value.construct(JsonInclude.Include.ALWAYS, JsonInclude.Include.NON_NULL));
        //objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    }

    public static String writeValueAsString(Object obj) throws JsonProcessingException {
        return objectMapper.writeValueAsString(obj);
    }

    public static void write(File file, Object obj) throws IOException {
        objectMapper.writeValue(file, obj);
    }

    public static <T> T parse(File file, Class<T> clazz) throws IOException {
        if (!file.exists() || file.isDirectory()) {
            return null;
        }

        return objectMapper.readValue(file, clazz);
    }

    public static <E> List<E> convertList(List list, Class<E> clazz) {
        JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, clazz);
        return objectMapper.convertValue(list, type);
    }


    public static <T> T parse(String json, Class<T> clazz) throws IOException {
        return objectMapper.readValue(json, clazz);
    }

    public static <T> T parse(InputStream input, Class<T> clazz) throws IOException {
        return objectMapper.readValue(input, clazz);
    }

    public static <E> List<E> parseList(String json, Class<E> clazz) throws Exception {
        JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, clazz);
        return objectMapper.readValue(json, type);
    }

    public static <T> T convert(Map map, Class<T> clazz) {
        return objectMapper.convertValue(map, clazz);
    }

//    public static <T> T parse(String json, TypeReference reference) throws IOException {
//        if (json == null || json.length() == 0) {
//            return null;
//        }
//
//        return objectMapper.readValue(json, reference);
//    }


}
