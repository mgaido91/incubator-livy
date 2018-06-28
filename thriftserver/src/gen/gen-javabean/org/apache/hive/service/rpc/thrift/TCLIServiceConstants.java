/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.rpc.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TCLIServiceConstants {

  public static final java.util.Set<TTypeId> PRIMITIVE_TYPES = new java.util.HashSet<TTypeId>();
  static {
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.BOOLEAN_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.TINYINT_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.SMALLINT_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.INT_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.BIGINT_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.FLOAT_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.DOUBLE_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.STRING_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.TIMESTAMP_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.BINARY_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.DECIMAL_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.NULL_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.DATE_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.VARCHAR_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.CHAR_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.INTERVAL_YEAR_MONTH_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.INTERVAL_DAY_TIME_TYPE);
    PRIMITIVE_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.TIMESTAMPLOCALTZ_TYPE);
  }

  public static final java.util.Set<TTypeId> COMPLEX_TYPES = new java.util.HashSet<TTypeId>();
  static {
    COMPLEX_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.ARRAY_TYPE);
    COMPLEX_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.MAP_TYPE);
    COMPLEX_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.STRUCT_TYPE);
    COMPLEX_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.UNION_TYPE);
    COMPLEX_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.USER_DEFINED_TYPE);
  }

  public static final java.util.Set<TTypeId> COLLECTION_TYPES = new java.util.HashSet<TTypeId>();
  static {
    COLLECTION_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.ARRAY_TYPE);
    COLLECTION_TYPES.add(org.apache.hive.service.rpc.thrift.TTypeId.MAP_TYPE);
  }

  public static final java.util.Map<TTypeId,java.lang.String> TYPE_NAMES = new java.util.HashMap<TTypeId,java.lang.String>();
  static {
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.BOOLEAN_TYPE, "BOOLEAN");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.TINYINT_TYPE, "TINYINT");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.SMALLINT_TYPE, "SMALLINT");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.INT_TYPE, "INT");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.BIGINT_TYPE, "BIGINT");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.FLOAT_TYPE, "FLOAT");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.DOUBLE_TYPE, "DOUBLE");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.STRING_TYPE, "STRING");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.TIMESTAMP_TYPE, "TIMESTAMP");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.BINARY_TYPE, "BINARY");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.ARRAY_TYPE, "ARRAY");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.MAP_TYPE, "MAP");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.STRUCT_TYPE, "STRUCT");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.UNION_TYPE, "UNIONTYPE");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.DECIMAL_TYPE, "DECIMAL");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.NULL_TYPE, "NULL");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.DATE_TYPE, "DATE");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.VARCHAR_TYPE, "VARCHAR");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.CHAR_TYPE, "CHAR");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.INTERVAL_YEAR_MONTH_TYPE, "INTERVAL_YEAR_MONTH");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.INTERVAL_DAY_TIME_TYPE, "INTERVAL_DAY_TIME");
    TYPE_NAMES.put(org.apache.hive.service.rpc.thrift.TTypeId.TIMESTAMPLOCALTZ_TYPE, "TIMESTAMP WITH LOCAL TIME ZONE");
  }

  public static final java.lang.String CHARACTER_MAXIMUM_LENGTH = "characterMaximumLength";

  public static final java.lang.String PRECISION = "precision";

  public static final java.lang.String SCALE = "scale";

}
