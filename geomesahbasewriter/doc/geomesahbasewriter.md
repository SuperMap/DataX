# GeoMesaHBaseWriter 插件文档

___

## 1 快速介绍

GeoMesaHBaseWriter 插件实现了从向GeoMesaHBase中写取数据。在底层实现上，GeoMesaHBaseWriter 通过 GeoMesa 连接远程 HBase 服务，并写入Hbase。

## 2 实现原理

简而言之，GeoMesaHBaseWriter 通过 GeoMesa 运用GeoTools API，将从上游Reader读取的数据写入HBase，并表现为GeoMesa的表结构形式。

## 3 功能说明

### 3.1 配置样例

* 配置一个从SuperMap传统引擎同步抽取数据到GeomesaHBase:

```
{
    "job": {
        "setting": {
            "speed": {
                // channel 表示通道数量
            	"channel": 3
            }
        },
        "content": [
            {
                "reader": {
                    "name": "sdxreader",
                    "parameter": {
                        "datasetinfos": [
                            {
                                "database": "",
                                "datasetname": "test1",
                                "driver": "",
                                "enginetype": 219,
                                "fieldnames": [
                                    "*"
                                ],
                                "password": "",
                                "server": "E:/Data/test.udb",
                                "username": ""
                            },
                            {
                                "database": "",
                                "datasetname": "test2",
                                "driver": "",
                                "enginetype": 219,
                                "fieldnames": [
                                    "Field1","Field2"
                                ],
                                "password": "",
                                "server": "E:/Data/test.udb",
                                "username": ""
                            },
                            {
                                "database": "",
                                "datasetname": "test3",
                                "driver": "",
                                "enginetype": 219,
                                "fieldnames": [
                                    "*"
                                ],
                                "password": "",
                                "server": "E:/Data/test.udb",
                                "username": ""
                            },
                            {
                                "database": "",
                                "datasetname": "test4",
                                "driver": "",
                                "enginetype": 219,
                                "fieldnames": [
                                    "*"
                                ],
                                "password": "",
                                "server": "E:/Data/test.udb",
                                "username": ""
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "geomesahbasewriter",
                    "parameter": {
                        "batchnumber": 1000,
                        "datasetinfos": [
                            {
                                "datasetbounds": "",
                                "datasetname": "test1",
                                "datasourcename": "wei",
                                "fields": [
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field1",
                                        "fieldtype": "Double"
                                    }
                                ],
                                "fieldshard": 4,
                                "geometryname": "geom",
                                "geometrysrid": 4326,
                                "geometrytype": "Point",
                                "ignoredtg": "false",
                                "server": "",
                                "splitteroption": "",
                                "zshard": 4
                            },
                            {
                                "datasetbounds": "",
                                "datasetname": "test2",
                                "datasourcename": "wei",
                                "fields": [
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field1",
                                        "fieldtype": "Long"
                                    },
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field2",
                                        "fieldtype": "Date"
                                    }
                                ],
                                "fieldshard": 4,
                                "geometryname": "geom",
                                "geometrysrid": 4326,
                                "geometrytype": "MultiLineString",
                                "ignoredtg": "false",
                                "server": "",
                                "splitteroption": "",
                                "zshard": 4
                            },
                            {
                                "datasetbounds": "",
                                "datasetname": "test3",
                                "datasourcename": "zhang",
                                "fields": [
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field1",
                                        "fieldtype": "Integer"
                                    },
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field2",
                                        "fieldtype": "String"
                                    }
                                ],
                                "fieldshard": 4,
                                "geometryname": "geom",
                                "geometrysrid": 4326,
                                "geometrytype": "MultiPolygon",
                                "ignoredtg": "false",
                                "server": "",
                                "splitteroption": "",
                                "zshard": 4
                            },
                            {
                                "datasetbounds": "",
                                "datasetname": "test4",
                                "datasourcename": "zhang",
                                "fields": [
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field1",
                                        "fieldtype": "Float"
                                    },
                                    {
                                        "fieldindex": false,
                                        "fieldname": "Field2",
                                        "fieldtype": "Boolean"
                                    }
                                ],
                                "fieldshard": 4,
                                "geometryname": "",
                                "geometrysrid": 4326,
                                "geometrytype": "Attribute",
                                "ignoredtg": "false",
                                "server": "",
                                "splitteroption": "",
                                "zshard": 4
                            }
                        ]
                    }
                }
            }
        ]
    }
}
```


### 3.2 参数说明

* **batchnumber**

	* 描述：HBase批量提交的条数。<br />

	* 必选：是 <br />

	* 默认值：1000 <br />

* **datasetinfos**

	* 描述：数据集信息配置，以确定哪些数据集将被写入。<br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **server**

	* 描述：HBase连接信息中的zookeeper。<br />
 
	* 必选：否，对于本地的HBase则不需要zookeeper都能连接 <br />
 
	* 默认值：无 <br />
	
* **datasourcename**
 
	* 描述：HBase连接信息中的catalog。 <br />
 
	* 必选：是 <br />
 
	* 默认值：无 <br />

* **datasetname**

	* 描述：HBase的Schema。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **datasetbounds**

	* 描述：HBase写入数据的范围，顺序为左右下上，中间以逗号分隔，属性表时不起作用。 <br />

	* 必选：不一定，对于非经纬度数据则必选，经纬度数据可不选 <br />
 
	* 默认值：无 <br />

* **zshard**

	* 描述：GeoMesaHBase Z索引分区数，大于等于4，属性表时不起作用。 <br />

	* 必选：是<br />
 
	* 默认值：4 <br />

* **geometryname**

	* 描述：GeoMesaHBase 几何对象字段名称。 <br />

	* 必选：不一定，对于点线面则必选，属性表可不选 <br />

	* 默认值：geom <br />

* **geometrytype**

	* 描述：GeoMesaHBase 几何对象类型，Point、MultiLineString、MultiPolygon、Attribute四者之一。 <br />

	* 必选：是 <br />
 
	* 默认值：无 <br />

	请注意:

    * `MultiLineString中能存LineString，MultiPolygon能存Polygon，所以配置MultiLineString和MultiPolygon即可`。

* **geometrysrid**

	* 描述：GeoMesaHBase 几何对象投影的SRID，属性表时不起作用。 <br />
	  
	* 必选：是 <br />
 
	* 默认值：4326 <br />

* **ignoredtg**

	* 描述：GeoMesaHBase 是否忽略Z3索引，也就是日期索引。 <br />
	  
	* 必选：否 <br />
 
	* 默认值：false <br />

* **splitteroption**

	* 描述：GeoMesaHBase ID索引分区策略，决定ID分区。 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **fieldshard**

	* 描述：GeoMesaHBase 字段索引分区数，没有字段索引时不起作用。 <br />

	* 必选：是 <br />

	* 默认值：4 <br />

* **fields**

	* 描述：GeoMesaHBase 待写入的字段描述。 <br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **fieldname**

	* 描述：GeoMesaHBase 字段描述中的字段名称。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fieldtype**

	* 描述：GeoMesaHBase 字段描述中的字段类型。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fieldindex**

	* 描述：GeoMesaHBase 字段描述中的字段索引。 <br />

	* 必选：是 <br />

	* 默认值：false <br />


### 3.3 HBase支持的列类型
* Boolean
* Short
* Integer
* Long
* Float
* Double
* String
* Date
* Character
* Byte

请注意:

* `除上述罗列字段类型外，其他类型均不支持`。

## 4 性能报告

略

## 5 约束限制

略

## 6 FAQ

略

***