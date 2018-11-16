package com.alibaba.datax.plugin.writer.geomesahbasewriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by zhangqiang.wei on 18-10-23.
 */
public enum GeoMesaHBaseWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("GeoMesaHBaseWriter-00", "您缺失了必须填写的参数值."),
    DATASET_NUMBER("GeoMesaHBaseWriter-01", "您配置的数据集数量错误."),
    FIELDS_VALUE("GeoMesaHBaseWriter-02", "您缺失了字段信息的参数值."),
    BATCH_NUMBER_ERROR("GeoMesaHBaseWriter-03", "您配置的批量提交条数错误."),
    DATASOURCE_NAME_VALUE("GeoMesaHBaseWriter-04", "您缺失了数据源名称的参数值."),
    DATASET_NAME_VALUE("GeoMesaHBaseWriter-05", "您缺失了数据集名称的参数值."),
    DATASET_BOUNDS_ERROR("GeoMesaHBaseWriter-06", "您配置的数据集范围错误."),
    Z_SHARD_ERROR("GeoMesaHBaseWriter-07", "您配置的Z索引分区数错误."),
    GEOMETRY_NAME_VALUE("GeoMesaHBaseWriter-08", "您缺失了几何对象字段名称的参数值."),
    GEOMETRY_TYPE_ERROR("GeoMesaHBaseWriter-09", "您配置的几何对象类型错误."),
    GEOMETRY_SRID_ERROR("GeoMesaHBaseWriter-10", "您配置的几何对象SRID错误."),
    IGNORE_DTG_ERROR("GeoMesaHBaseWriter-11", "您配置的是否忽略Z3索引错误."),
    SPLITTER_OPTION_ERROR("GeoMesaHBaseWriter-12", "您配置的ID分区选项错误."),
    FIELD_SHARD_ERROR("GeoMesaHBaseWriter-13", "您配置的字段索引分区数错误."),
    FIELD_NAME_VALUE("GeoMesaHBaseWriter-14", "您缺失了字段名称的参数值."),
    FIELD_TYPE_ERROR("GeoMesaHBaseWriter-15", "您配置的字段类型错误."),
    WRITE_ERROR("GeoMesaHBaseWriter-16", "您配置的信息在写入时异常."),
    CONNECT_ERROR("GeoMesaHBaseWriter-17", "您配置的数据源连接异常."),
    SCHEMA_ERROR("GeoMesaHBaseWriter-18", "您配置的数据集信息错误."),
    FIELD_NUMBER("GeoMesaHBaseWriter-19", "您配置的字段个数错误."),
    GEOMETRY_ERROR("GeoMesaHBaseWriter-20", "您写入时获取几何对象异常.");

    private final String code;
    private final String description;

    private GeoMesaHBaseWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}
