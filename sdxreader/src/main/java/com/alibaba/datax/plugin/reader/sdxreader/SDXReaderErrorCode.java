package com.alibaba.datax.plugin.reader.sdxreader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by zhangqiang.wei on 18-10-23.
 */
public enum SDXReaderErrorCode implements ErrorCode {
	REQUIRED_VALUE("SDXReader-00", "您缺失了必须填写的参数值."),
	DATASOURCE_OPEN_FAIL("SDXReader-01", "您配置的数据源打开失败."),
	DATASET_NON_EXISTENT("SDXReader-02", "您配置的数据集不存在."),
	DATASET_INVALID("SDXReader-03","您配置的数据集无效."),
	FIELD_VALUE("SDXReader-04", "您缺失了字段名称的参数值."),
	FIELD_INVALID("SDXReader-05", "您配置的字段名称无效."),
    QUERY_FAIL("SDXReader-06", "您的数据集查询失败."),;

	private final String code;
	private final String description;

	private SDXReaderErrorCode(String code, String description) {
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
