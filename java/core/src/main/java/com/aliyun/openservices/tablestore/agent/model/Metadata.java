package com.aliyun.openservices.tablestore.agent.model;

import static com.aliyun.openservices.tablestore.agent.util.Exceptions.illegalArgument;
import static com.aliyun.openservices.tablestore.agent.util.Exceptions.runtime;
import static com.aliyun.openservices.tablestore.agent.util.ValidationUtils.ensureNotBlank;
import static com.aliyun.openservices.tablestore.agent.util.ValidationUtils.ensureNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

public class Metadata {

    private static final Set<Class<?>> SUPPORTED_VALUE_TYPES = new LinkedHashSet<>();

    static {
        SUPPORTED_VALUE_TYPES.add(String.class);

        SUPPORTED_VALUE_TYPES.add(int.class);
        SUPPORTED_VALUE_TYPES.add(Integer.class);

        SUPPORTED_VALUE_TYPES.add(long.class);
        SUPPORTED_VALUE_TYPES.add(Long.class);

        SUPPORTED_VALUE_TYPES.add(float.class);
        SUPPORTED_VALUE_TYPES.add(Float.class);

        SUPPORTED_VALUE_TYPES.add(double.class);
        SUPPORTED_VALUE_TYPES.add(Double.class);

        SUPPORTED_VALUE_TYPES.add(short.class);
        SUPPORTED_VALUE_TYPES.add(Short.class);

        SUPPORTED_VALUE_TYPES.add(boolean.class);
        SUPPORTED_VALUE_TYPES.add(Boolean.class);

        SUPPORTED_VALUE_TYPES.add(byte[].class);
    }

    private final Map<String, Object> metadata;

    public Metadata() {
        this.metadata = new HashMap<>();
    }

    public Metadata(Map<String, ?> metadata) {
        ensureNotNull(metadata, "metadata").forEach((key, value) -> {
            validate(key, value);
            checkSupportedValueTypes(key, value);
        });
        this.metadata = new HashMap<>(metadata);
    }

    private void checkSupportedValueTypes(String key, Object value) {
        if (!SUPPORTED_VALUE_TYPES.contains(value.getClass())) {
            throw illegalArgument(
                "The metadata key '%s' has the value '%s', which is of the unsupported type '%s'. " + "Currently, the supported types are: %s",
                key,
                value,
                value.getClass().getName(),
                SUPPORTED_VALUE_TYPES
            );
        }
    }

    private static void validate(String key, Object value) {
        ensureNotBlank(key, "The metadata key with the value '" + value + "'");
        ensureNotNull(value, "The metadata value for the key '" + key + "'");
    }

    @Nullable
    public Object get(String key) {
        return metadata.get(key);
    }

    @Nullable
    public String getString(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof String) {
            return (String) value;
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as a String.",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public Boolean getBoolean(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as a Boolean.",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public byte[] getBytes(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as a byte[].",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public Integer getInteger(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof String) {
            return Integer.parseInt(value.toString());
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as an Integer.",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public Short getShort(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof String) {
            return Short.parseShort(value.toString());
        } else if (value instanceof Number) {
            return ((Number) value).shortValue();
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as an Short.",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public Long getLong(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof String) {
            return Long.parseLong(value.toString());
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as a Long.",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public Float getFloat(String key) {
        if (!containsKey(key)) {
            return null;
        }

        final Object value = metadata.get(key);
        if (value instanceof String) {
            return Float.parseFloat(value.toString());
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as a Float.",
            key,
            value,
            value.getClass().getName()
        );
    }

    @Nullable
    public Double getDouble(String key) {
        if (!containsKey(key)) {
            return null;
        }

        Object value = metadata.get(key);
        if (value instanceof String) {
            return Double.parseDouble(value.toString());
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        throw runtime(
            "Metadata entry with the key '%s' has a value of '%s' and type '%s'. " + "It cannot be returned as a Double.",
            key,
            value,
            value.getClass().getName()
        );
    }

    public boolean containsKey(String key) {
        return metadata.containsKey(key);
    }

    public Metadata put(String key, String value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata put(String key, boolean value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata put(String key, byte[] value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata put(String key, int value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata put(String key, long value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata put(String key, float value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata put(String key, double value) {
        validate(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata putObject(String key, Object value) {
        validate(key, value);
        checkSupportedValueTypes(key, value);
        this.metadata.put(key, value);
        return this;
    }

    public Metadata remove(String key) {
        this.metadata.remove(key);
        return this;
    }

    public Metadata copy() {
        return new Metadata(metadata);
    }

    public int size() {
        return metadata.size();
    }

    public Map<String, Object> toMap() {
        return new HashMap<>(metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Metadata that = (Metadata) o;
        return Objects.equals(this.metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata);
    }

    @Override
    public String toString() {
        return "Metadata {" + metadata + "}";
    }

    public static Metadata from(String key, String value) {
        return new Metadata().put(key, value);
    }

    public static Metadata from(Map<String, ?> metadata) {
        return new Metadata(metadata);
    }

    public static Metadata metadata(String key, String value) {
        return from(key, value);
    }

    public Metadata merge(@Nullable Metadata another) {
        if (another == null || another.metadata.isEmpty()) {
            return this.copy();
        }
        final Map<String, Object> thisMap = this.toMap();
        final Map<String, Object> anotherMap = another.toMap();
        final Set<String> commonKeys = new HashSet<>(thisMap.keySet());
        commonKeys.retainAll(anotherMap.keySet());
        if (!commonKeys.isEmpty()) {
            throw illegalArgument("Metadata keys are not unique. Common keys: %s", commonKeys);
        }
        final Map<String, Object> mergedMap = new HashMap<>(thisMap);
        mergedMap.putAll(anotherMap);
        return Metadata.from(mergedMap);
    }
}
