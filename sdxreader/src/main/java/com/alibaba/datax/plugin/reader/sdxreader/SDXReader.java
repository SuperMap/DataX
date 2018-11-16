package com.alibaba.datax.plugin.reader.sdxreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.supermap.data.*;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.OutStream;
import com.vividsolutions.jts.io.OutputStreamOutStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangqiang.wei on 18-10-23.
 */
public class SDXReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration sdxReaderConfig = null;

		private List<Configuration> datasetConfigs = null;

		@Override
		public void init() {
			this.sdxReaderConfig = this.getPluginJobConf();
			this.datasetConfigs = this.sdxReaderConfig.getListConfiguration(Key.DATASETINFOS);
			if (this.datasetConfigs == null || this.datasetConfigs.size() == 0) {
				throw DataXException.asDataXException(
						SDXReaderErrorCode.REQUIRED_VALUE,
						"您需要指定待读取的数据集信息");
			}
		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			LOG.info(String.format("您即将读取的数据集总数为: [%d]", this.datasetConfigs.size()));
			LOG.debug("prepare() end...");
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.debug("split() begin...");
			// 将每个单独的 dataset 作为一个 slice
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

	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration readerDatasetConfig;
		private String strServer;
		private Integer nEngineType;
		private String strDriver;
		private String strDatabase;
		private String strUserName;
		private String strPassword;
		private String strDatasetName;
		private List<String> strFieldNames;

		private int byteOrder;
		private int nSRID;
		private ByteArrayOutputStream byteArrayOS;
		private OutStream byteArrayOutStream;
		private byte[] buf;

		@Override
		public void init() {
			this.readerDatasetConfig = this.getPluginJobConf();
			this.strServer = this.readerDatasetConfig.getString(Key.SERVER);
			this.nEngineType = this.readerDatasetConfig.getInt(Key.ENGINETYPE);
			this.strDriver = this.readerDatasetConfig.getString(Key.DRIVER);
			this.strDatabase = this.readerDatasetConfig.getString(Key.DATABASE);
			this.strUserName = this.readerDatasetConfig.getString(Key.USERNAME);
			this.strPassword = this.readerDatasetConfig.getString(Key.PASSWORD);
			this.strDatasetName = this.readerDatasetConfig.getString(Key.DATASETNAME);
			this.strFieldNames = this.readerDatasetConfig.getList(Key.FIELDNAMES, String.class);

			this.byteOrder = 2;
			this.nSRID = 0;
			this.byteArrayOS = new ByteArrayOutputStream();
			this.byteArrayOutStream = new OutputStreamOutStream(this.byteArrayOS);
			this.buf = new byte[8];
		}

		@Override
		public void prepare() {

		}

		@Override
		public void startRead(RecordSender recordSender) {
			LOG.debug("start read dataset...");
			EngineType dsType = toEngineType(this.nEngineType);
			Datasource ds = new Datasource(dsType);
			DatasourceConnectionInfo info = new DatasourceConnectionInfo();
			info.setServer(this.strServer);
			info.setEngineType(dsType);
			info.setDriver(this.strDriver);
			info.setDatabase(this.strDatabase);
			info.setUser(this.strUserName);
			info.setPassword(this.strPassword);
			info.setReadOnly(true);
			boolean bIsOpen = ds.open(info);
			info.dispose();
			if (!bIsOpen) {
				throw DataXException.asDataXException(
						SDXReaderErrorCode.DATASOURCE_OPEN_FAIL,
						"请您配置正确的数据源连接信息");
			}

			Dataset dt = ds.getDatasets().get(this.strDatasetName);
			if (dt == null) {
				ds.close();
				throw DataXException.asDataXException(
						SDXReaderErrorCode.DATASET_NON_EXISTENT,
						"请您配置该数据源下存在的数据集");
			}
			if (!validateDataset(dt)) {
				dt.close();
				ds.close();
				throw DataXException.asDataXException(
						SDXReaderErrorCode.DATASET_INVALID,
						"请您配置二维点线面属性表数据集");
			}
			DatasetVector dtv = (DatasetVector)dt;
            this.nSRID = dtv.getPrjCoordSys().toEPSGCode();
            
			if (this.strFieldNames == null || this.strFieldNames.size() == 0) {
				dtv.close();
				ds.close();
				throw DataXException.asDataXException(
						SDXReaderErrorCode.FIELD_VALUE,
						"请您配置正确的字段名称");
			}
			for (String strFieldName : this.strFieldNames) {
				if (strFieldName.startsWith("Sm")) {
					dtv.close();
					ds.close();
					throw DataXException.asDataXException(
							SDXReaderErrorCode.FIELD_INVALID,
							"请您配置非系统字段");
				}
			}

			QueryParameter par = new QueryParameter();
			if (this.strFieldNames.size() != 1 || !this.strFieldNames.get(0).equals("*")) {
				par.setResultFields(this.strFieldNames.toArray(new String[0]));
			}
			par.setCursorType(CursorType.STATIC);

			Recordset rc = dtv.query(par);
			if (rc == null) {
				par.dispose();
				dtv.close();
				ds.close();
				throw DataXException.asDataXException(
						SDXReaderErrorCode.QUERY_FAIL,
						"请您检查是否配置了不存在的字段名称");
			}
			par.dispose();

			FieldInfos fieldinfos = rc.getFieldInfos();
			List<Integer> nFieldIndexs = new ArrayList<Integer>();
			for (int nIndex=0; nIndex<fieldinfos.getCount(); nIndex++) {
				if (!fieldinfos.get(nIndex).getName().startsWith("Sm")) {
					nFieldIndexs.add(nIndex);
				}
			}

			rc.moveFirst();
			while (!rc.isEOF()) {
				transforOneRecord(recordSender, rc, fieldinfos, nFieldIndexs);
				rc.moveNext();
			}

			rc.dispose();
			dtv.close();
			ds.close();
			LOG.debug("end read dataset...");
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {

		}

		private EngineType toEngineType(int nEngineType) {
			switch (nEngineType) {
				case 4:
					return EngineType.SDE;
				case 5:
					return EngineType.IMAGEPLUGINS;
				case 10:
					return EngineType.ORACLESPATIAL;
				case 12:
					return EngineType.ORACLEPLUS;
				case 16:
					return EngineType.SQLPLUS;
				case 17:
					return EngineType.DM;
				case 18:
					return EngineType.DB2;
				case 19:
					return EngineType.KINGBASE;
				case 20:
					return EngineType.MEMORY;
				case 23:
					return EngineType.OGC;
				case 32:
					return EngineType.MYSQL;
				case 101:
					return EngineType.VECTORFILE;
				case 219:
					return EngineType.UDB;
				case 221:
					return EngineType.POSTGRESQL;
				case 223:
					return EngineType.GOOGLEMAPS;
				case 224:
					return EngineType.SUPERMAPCLOUD;
				case 225:
					return EngineType.ISERVERREST;
				case 227:
					return EngineType.BAIDUMAPS;
				case 228:
					return EngineType.OPENSTREETMAPS;
				case 229:
					return EngineType.SCV;
				case 230:
					return EngineType.BINGMAPS;
				case 401:
					return EngineType.MONGODB;
				case 1001:
					return EngineType.MDB;
				case 2001:
					return EngineType.BEYONDB;
				case 2002:
					return EngineType.GBASE;
				case 2003:
					return EngineType.HIGHGODB;
				case 2004:
					return EngineType.ALTIBASE;
				case 2005:
					return EngineType.KDB;
				case 2006:
					return EngineType.SRDB;
				case 2007:
					return EngineType.MYSQLPlus;
				case 2008:
					return EngineType.DRDS;
				case 2009:
					return EngineType.GBASE8T;
				case 2010:
					return EngineType.KADB;
				case 2011:
					return EngineType.ES;
				case 2012:
					return EngineType.PGGIS;
				case 2013:
					return EngineType.SQLSPATIAL;
				case 2014:
					return EngineType.TIBERO;
				case 2050:
					return EngineType.SINODB;
				case 2051:
					return EngineType.DATASERVER;
				case 2052:
					return EngineType.GREENPLUM;
				case 2054:
					return EngineType.UDBX;
				default:
					return EngineType.UDB;
			}
		}

		private boolean validateDataset(Dataset dt) {
			DatasetType dtType = dt.getType();
			return (dtType == DatasetType.POINT ||
					dtType == DatasetType.LINE ||
					dtType == DatasetType.REGION ||
					dtType == DatasetType.TABULAR);
		}

		private void transforOneRecord(RecordSender recordSender, Recordset rc, FieldInfos fieldinfos, List<Integer> nFieldIndexs) {
			Record record = recordSender.createRecord();
			for (Integer nIndex : nFieldIndexs) {
				FieldType fieldType = fieldinfos.get(nIndex).getType();
				if (fieldType == FieldType.CHAR  ||
					fieldType == FieldType.TEXT  ||
					fieldType == FieldType.WTEXT ||
					fieldType == FieldType.JSONB) {
					record.addColumn(new StringColumn(rc.getString(nIndex)));
				} else if (fieldType == FieldType.BYTE  ||
						fieldType == FieldType.INT16 ||
						fieldType == FieldType.INT32 ||
						fieldType == FieldType.INT64) {
					record.addColumn(new LongColumn(rc.getInt64(nIndex)));
				} else if (fieldType == FieldType.BOOLEAN) {
					record.addColumn(new BoolColumn(rc.getBoolean(nIndex)));
				} else if (fieldType == FieldType.SINGLE ||
						fieldType == FieldType.DOUBLE) {
					record.addColumn(new DoubleColumn(rc.getDouble(nIndex)));
				} else if (fieldType == FieldType.DATETIME) {
					record.addColumn(new DateColumn(rc.getDateTime(nIndex)));
				} else if (fieldType == FieldType.LONGBINARY) {
					record.addColumn(new BytesColumn(rc.getLongBinary(nIndex)));
				}
			}

			Geometry geometry = rc.getGeometry();
			if (geometry != null) {
				try {
					this.byteArrayOS.reset();
					this.writeGeometry(geometry, this.byteArrayOutStream);
					geometry.dispose();
					record.addColumn(new BytesColumn(this.byteArrayOS.toByteArray()));
					recordSender.sendToWriter(record);
				} catch (IOException var3) {
					geometry.dispose();
				}
			} else {
				recordSender.sendToWriter(record);
			}
		}

		private void writeGeometry(Geometry geom, OutStream os) throws IOException {
			if (geom.getType() == GeometryType.GEOPOINT) {
				GeoPoint geoP = (GeoPoint) geom;
				Point2D pt2D = new Point2D();
				pt2D.setX(geoP.getX());
				pt2D.setY(geoP.getY());
				writePoint2D(pt2D, os);
			} else if (geom.getType() == GeometryType.GEOLINE) {
				GeoLine geoL = (GeoLine) geom;
				int nCount = geoL.getPartCount();
				if (nCount == 1) {
					writeLine2D(geoL.getPart(0), os);
				} else {
					this.writeByteOrder(os);
					this.writeGeometryType(5, os);
					this.writeInt(nCount, os);
					for(int i = 0; i < nCount; ++i) {
						writeLine2D(geoL.getPart(i), os);
					}
				}
			} else if (geom.getType() == GeometryType.GEOREGION) {
				GeoRegion geoR = (GeoRegion) geom;
				int nCount = geoR.getPartCount();
				if (nCount == 1) {
					writePolygon2D(geoR.getPart(0), os);
				} else {
					this.writeByteOrder(os);
					this.writeGeometryType(6, os);
					this.writeInt(nCount, os);
					for(int i = 0; i < nCount; ++i) {
						writePolygon2D(geoR.getPart(i), os);
					}
				}
			} else {
			    throw new IOException("Unknown Geometry type");
            }
		}

		private void writePoint2D(Point2D pt2D, OutStream os) throws IOException {
			this.writeByteOrder(os);
			this.writeGeometryType(1, os);
			this.writeCoordinate(pt2D, os);
		}

		private void writeLine2D(Point2Ds pt2Ds, OutStream os) throws IOException {
			this.writeByteOrder(os);
			this.writeGeometryType(2, os);
			this.writeCoordinates(pt2Ds, os);
		}

		private void writePolygon2D(Point2Ds pt2Ds, OutStream os) throws IOException {
			this.writeByteOrder(os);
			this.writeGeometryType(3, os);
			this.writeInt(1, os);
			this.writeCoordinates(pt2Ds, os);
		}

		private void writeByteOrder(OutStream os) throws IOException {
			if (this.byteOrder == 2) {
				this.buf[0] = 1;
			} else {
				this.buf[0] = 0;
			}
			os.write(this.buf, 1);
		}

		private void writeGeometryType(int geometryType, OutStream os) throws IOException {
			int flag3D = 0;
			int typeInt = geometryType | flag3D;
			typeInt |= this.nSRID > 0 ? 536870912 : 0;
			this.writeInt(typeInt, os);
			if (this.nSRID > 0) {
				this.writeInt(this.nSRID, os);
			}
		}

		private void writeInt(int intValue, OutStream os) throws IOException {
			ByteOrderValues.putInt(intValue, this.buf, this.byteOrder);
			os.write(this.buf, 4);
		}

		private void writeCoordinates(Point2Ds pt2Ds, OutStream os) throws IOException {
			int nCount = pt2Ds.getCount();
			this.writeInt(nCount, os);
			for(int i = 0; i < nCount; ++i) {
				this.writeCoordinate(pt2Ds.getItem(i), os);
			}
		}

		private void writeCoordinate(Point2D pt2D, OutStream os) throws IOException {
			ByteOrderValues.putDouble(pt2D.getX(), this.buf, this.byteOrder);
			os.write(this.buf, 8);
			ByteOrderValues.putDouble(pt2D.getY(), this.buf, this.byteOrder);
			os.write(this.buf, 8);
		}

	}
}
