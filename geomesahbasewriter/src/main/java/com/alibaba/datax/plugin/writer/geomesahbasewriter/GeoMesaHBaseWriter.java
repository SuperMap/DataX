package com.alibaba.datax.plugin.writer.geomesahbasewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by zhangqiang.wei on 18-10-23.
 */
public class GeoMesaHBaseWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration geomesahbaseWriterConfig = null;
        private List<Configuration> datasetConfigs = null;
        private long batchNumber;

        @Override
        public void init() {
            this.geomesahbaseWriterConfig = this.getPluginJobConf();
            this.batchNumber = this.geomesahbaseWriterConfig.getLong(Key.BATCHNUMBER);
            this.datasetConfigs = this.geomesahbaseWriterConfig.getListConfiguration(Key.DATASETINFOS);
            if (this.datasetConfigs == null || this.datasetConfigs.size() == 0) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.REQUIRED_VALUE,
                        "您需要指定待写入的数据集信息");
            }
        }

        @Override
        public void prepare() {
            LOG.debug("prepare() begin...");
            LOG.info(String.format("您即将写入的数据集总数为: [%d]", this.datasetConfigs.size()));
            LOG.debug("prepare() end...");
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.debug("split() begin...");
            if (mandatoryNumber != this.datasetConfigs.size()) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.DATASET_NUMBER,
                        "请您配置写入数据集与读取数据集一一对应");
            }
            for (int i = 0; i < mandatoryNumber; i++) {
                this.datasetConfigs.get(i).set(Key.BATCHNUMBER, this.batchNumber);
            }
            LOG.debug("split() end...");
            return this.datasetConfigs;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerDatasetConfig = null;
        private long nBatchNumber;
        private String strServer;
        private String strDsName;
        private String strDtName;
        private String strDtBounds;
        private Integer nZShard;
        private String strGeoName;
        private String strGeoType;
        private Integer nGeoSRID;
        private String strIgnoreDtg;
        private String strSplitterOption;
        private Integer nFieldShard;
        private List<String> strFieldNames = null;
        private List<String> strFieldTypes = null;
        private List<Boolean> bFieldIndexs = null;

        @Override
        public void init() {
            this.writerDatasetConfig = this.getPluginJobConf();
            this.nBatchNumber = this.writerDatasetConfig.getLong(Key.BATCHNUMBER);
            this.strServer = this.writerDatasetConfig.getString(Key.SERVER);
            this.strDsName = this.writerDatasetConfig.getString(Key.DATASOURCENAME);
            this.strDtName = this.writerDatasetConfig.getString(Key.DATASETNAME);
            this.strDtBounds = this.writerDatasetConfig.getString(Key.DATASETBOUNDS);
            this.nZShard = this.writerDatasetConfig.getInt(Key.ZSHARD);
            this.strGeoName = this.writerDatasetConfig.getString(Key.GEOMETRYNAME);
            this.strGeoType = this.writerDatasetConfig.getString(Key.GEOMETRYTYPE);
            this.nGeoSRID = this.writerDatasetConfig.getInt(Key.GEOMETRYSIRD);
            this.strIgnoreDtg = this.writerDatasetConfig.getString(Key.IGNOREDTG);
            this.strSplitterOption = this.writerDatasetConfig.getString(Key.SPLITTEROPTION);
            this.nFieldShard = this.writerDatasetConfig.getInt(Key.FIELDSHARD);
            List<Configuration> fieldConfigs = this.writerDatasetConfig.getListConfiguration(Key.FIELDS);
            if (fieldConfigs == null) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.FIELDS_VALUE,
                        "请您配置正确的字段信息");
            }
            this.strFieldNames = new ArrayList<String>();
            this.strFieldTypes = new ArrayList<String>();
            this.bFieldIndexs = new ArrayList<Boolean>();
            for (Configuration fieldConfig : fieldConfigs) {
                this.strFieldNames.add(fieldConfig.getString(Key.FIELDNAME));
                this.strFieldTypes.add(fieldConfig.getString(Key.FIELDTYPE));
                this.bFieldIndexs.add(fieldConfig.getBool(Key.FIELDINDEX));
            }
            validateParameter();
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.debug("begin do write...");
            Map<String, String> params = new HashMap<String, String>();
            if (this.strServer.isEmpty()) {
                params.put("hbase.catalog", this.strDsName);
            } else {
                params.put("hbase.catalog", this.strDsName);
                params.put("hbase.zookeepers", this.strServer);
            }

            try {
                DataStore ds = DataStoreFinder.getDataStore(params);
                if (ds == null) {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.CONNECT_ERROR,
                            "请您检查HBase连接是否可用");
                }

                SimpleFeatureType sft = ds.getSchema(this.strDtName);
                if (sft == null) {
                    SimpleFeatureType sftTemp = createSFT();
                    ds.createSchema(sftTemp);
                }
                sft = ds.getSchema(this.strDtName);
                if (sft == null) {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.SCHEMA_ERROR,
                            "请您检查数据集信息配置");
                }

                Transaction transaction = new DefaultTransaction("Add Data");
                SimpleFeatureStore featureStore = (SimpleFeatureStore)ds.getFeatureSource(this.strDtName);
                featureStore.setTransaction(transaction);
                SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(sft);
                List<SimpleFeature> featureList = new ArrayList<SimpleFeature>();
                WKBReader wkbReader = new WKBReader();
                long lCount = 0L;
                Record record;
                int nRecColumn;
                Boolean bIsAttribute;
                if (this.strGeoType.equals("Attribute")) {
                    nRecColumn = this.strFieldNames.size();
                    bIsAttribute = true;
                } else {
                    nRecColumn = this.strFieldNames.size() + 1;
                    bIsAttribute = false;
                }
                while ((record = lineReceiver.getFromReader()) != null) {
                    int nRecordLength = record.getColumnNumber();
                    if (nRecordLength != nRecColumn) {
                        throw DataXException.asDataXException(
                                GeoMesaHBaseWriterErrorCode.FIELD_NUMBER,
                                "请您配置写入字段等于读取字段个数");
                    }
                    featureBuilder.reset();

                    for (int i=0; i<nRecordLength-1; i++) {
                        if (this.strFieldTypes.get(i).equals("String")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asString());
                        } else if (this.strFieldTypes.get(i).equals("Integer")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asLong());
                        } else if (this.strFieldTypes.get(i).equals("Long")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asLong());
                        } else if (this.strFieldTypes.get(i).equals("Double")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asDouble());
                        } else if (this.strFieldTypes.get(i).equals("Date")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asDate());
                        } else if (this.strFieldTypes.get(i).equals("Float")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asDouble());
                        } else if (this.strFieldTypes.get(i).equals("Boolean")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asBoolean());
                        } else if (this.strFieldTypes.get(i).equals("Short")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asLong());
                        } else if (this.strFieldTypes.get(i).equals("Character")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asString());
                        } else if (this.strFieldTypes.get(i).equals("Byte")) {
                            featureBuilder.set(this.strFieldNames.get(i), record.getColumn(i).asLong());
                        }
                    }

                    if (!bIsAttribute) {
                        try {
                            Geometry geo = wkbReader.read(record.getColumn(nRecordLength-1).asBytes());
                            featureBuilder.set(this.strGeoName, geo);
                        } catch (ParseException e1) {
                            throw DataXException.asDataXException(
                                    GeoMesaHBaseWriterErrorCode.GEOMETRY_ERROR,
                                    "请您检查读取数据的几何对象合法性");
                        }
                    }

                    SimpleFeature simFeature = featureBuilder.buildFeature(java.util.UUID.randomUUID().toString());
                    if (featureList.add(simFeature)) {
                        lCount++;
                    }

                    if (lCount == this.nBatchNumber) {
                        SimpleFeatureCollection featureCollection = new ListFeatureCollection(sft, featureList);
                        try {
                            featureStore.addFeatures(featureCollection);
                            transaction.commit();
                        } catch (Exception e2) {
                            transaction.rollback();
                        }
                        lCount = 0L;
                        featureList.clear();
                    }
                }

                if (lCount != 0L) {
                    SimpleFeatureCollection featureCollection = new ListFeatureCollection(sft, featureList);
                    try {
                        featureStore.addFeatures(featureCollection);
                        transaction.commit();
                    } catch (Exception e3) {
                        transaction.rollback();
                    }
                    featureList.clear();
                }
                transaction.close();
            } catch (IOException e) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.WRITE_ERROR,
                        "请您检查配置信息");
            }
            LOG.debug("end do write");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

        private void validateParameter() {
            if (this.nBatchNumber <= 0) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.BATCH_NUMBER_ERROR,
                        "请您配置批量提交条数大于等于零");
            }
            
            if (this.strDsName.isEmpty()) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.DATASOURCE_NAME_VALUE,
                        "请您配置数据源名称");
            }

            if (this.strDtName.isEmpty()) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.DATASET_NAME_VALUE,
                        "请您配置数据集名称");
            }

            if (!this.strDtBounds.isEmpty()) {
                String strTemp = this.strDtBounds.replace(",", "");
                if (this.strDtBounds.length() - strTemp.length() != 3) {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.DATASET_BOUNDS_ERROR,
                            "请您配置左右下上逗号分隔的范围");
                }
            }

            if (this.nZShard < 4) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.Z_SHARD_ERROR,
                        "请您配置至少4个Z索引分区");
            }

            if (this.strGeoName.isEmpty() && !this.strGeoType.equals("Attribute")) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.GEOMETRY_NAME_VALUE,
                        "请您配置几何对象字段名称");
            }

            if (this.strGeoType.isEmpty() ||
                    (!this.strGeoType.equals("Attribute") &&
                            !this.strGeoType.equals("Point") &&
                            !this.strGeoType.equals("MultiLineString") &&
                            !this.strGeoType.equals("MultiPolygon"))) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.GEOMETRY_TYPE_ERROR,
                        "请您配置正确的几何对象类型");
            }

            if (this.nGeoSRID <= 0) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.GEOMETRY_SRID_ERROR,
                        "请您配置正确的几何对象SRID");
            }

            if (!this.strIgnoreDtg.isEmpty() &&
                    !this.strIgnoreDtg.equals("false") &&
                    !this.strIgnoreDtg.equals("true")) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.IGNORE_DTG_ERROR,
                        "请您配置字符串型的true or false");
            }

            if (!this.strSplitterOption.isEmpty() &&
                    !this.strSplitterOption.startsWith("id.pattern:")) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.SPLITTER_OPTION_ERROR,
                        "请您配置正确的ID分区策略");
            }

            if (this.nFieldShard < 4) {
                throw DataXException.asDataXException(
                        GeoMesaHBaseWriterErrorCode.FIELD_SHARD_ERROR,
                        "请您配置至少4个字段索引分区");
            }

            for (int nFieldIndex=0; nFieldIndex<this.strFieldNames.size(); nFieldIndex++) {
                if (this.strFieldNames.get(nFieldIndex).isEmpty()) {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.FIELD_NAME_VALUE,
                            "请您配置字段名称");
                }
                String strFieldType = this.strFieldTypes.get(nFieldIndex);
                if (strFieldType.isEmpty() ||
                        (!strFieldType.equals("String") &&
                                !strFieldType.equals("Integer") &&
                                !strFieldType.equals("Long") &&
                                !strFieldType.equals("Double") &&
                                !strFieldType.equals("Date") &&
                                !strFieldType.equals("Float") &&
                                !strFieldType.equals("Boolean") &&
                                !strFieldType.equals("Short") &&
                                !strFieldType.equals("Character") &&
                                !strFieldType.equals("Byte"))) {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.FIELD_TYPE_ERROR,
                            "请您配置正确的字段类型");
                }
            }
        }

        private SimpleFeatureType createSFT() {
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            for (int nFieldIndex=0; nFieldIndex<this.strFieldNames.size(); nFieldIndex++) {
                String strFieldType = this.strFieldTypes.get(nFieldIndex);
                if (strFieldType.equals("String")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), String.class);
                } else if (strFieldType.equals("Integer")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Integer.class);
                } else if (strFieldType.equals("Long")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Long.class);
                } else if (strFieldType.equals("Double")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Double.class);
                } else if (strFieldType.equals("Date")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Date.class);
                } else if (strFieldType.equals("Float")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Float.class);
                } else if (strFieldType.equals("Boolean")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Boolean.class);
                } else if (strFieldType.equals("Short")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Short.class);
                } else if (strFieldType.equals("Character")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Character.class);
                } else if (strFieldType.equals("Byte")) {
                    builder.add(this.strFieldNames.get(nFieldIndex), Byte.class);
                } else {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.FIELD_TYPE_ERROR,
                            "请您配置正确的字段类型");
                }
            }

            if (!this.strGeoType.equals("Attribute")) {
                if (this.strGeoType.equals("Point")) {
                    builder.add(this.strGeoName, Point.class);
                } else if (this.strGeoType.equals("MultiLineString")) {
                    builder.add(this.strGeoName, MultiLineString.class);
                } else if (this.strGeoType.equals("MultiPolygon")) {
                    builder.add(this.strGeoName, MultiPolygon.class);
                } else {
                    throw DataXException.asDataXException(
                            GeoMesaHBaseWriterErrorCode.GEOMETRY_TYPE_ERROR,
                            "请您配置正确的几何对象类型");
                }
            }

            builder.setName(this.strDtName);
            SimpleFeatureType sft = builder.buildFeatureType();

            if (!this.strGeoType.equals("Attribute")) {
                sft.getUserData().put("geomesa.srid", this.nGeoSRID.toString());
                if (!this.strDtBounds.isEmpty()) {
                    sft.getUserData().put("geomesa.z.bounds", this.strDtBounds);
                }
                sft.getUserData().put("geomesa.z.splits", this.nZShard.toString());
            }

            Boolean bIsResult = false;
            for (int nFieldIndex=0; nFieldIndex<this.strFieldNames.size(); nFieldIndex++) {
                if (this.bFieldIndexs.get(nFieldIndex)) {
                    bIsResult = true;
                    sft.getDescriptor(this.strFieldNames.get(nFieldIndex)).getUserData().put("index", "true");
                }
            }
            if (bIsResult) {
                sft.getUserData().put("geomesa.attr.splits", this.nFieldShard.toString());
            }

            if (this.strIgnoreDtg.equals("true")) {
                sft.getUserData().put("geomesa.ignore.dtg", this.strIgnoreDtg);
            }

            if (!this.strSplitterOption.isEmpty()) {
                sft.getUserData().put("table.splitter.options", this.strSplitterOption);
            }

            return sft;
        }
    }
}
