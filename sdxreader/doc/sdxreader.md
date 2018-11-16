
# SDXReader 插件文档


___


## 1 快速介绍

SDXReader插件实现了从SuperMap数据源中的数据集读取数据。在底层实现上，SDXReader通过SuperMap组件连接数据源，获取其中数据集的数据，并转换为DataX传输协议传递给Writer。

## 2 实现原理

简而言之，SDXReader通过SuperMap组件连接数据源，并根据用户配置的数据集获取非系统字段值和几何对象，并将获取的结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置server、enginetype、driver、database、username、password的信息，SDXReader将其构造为数据源链接信息再连接到数据库；对于用户配置datasetname、fieldnames信息，数据源会获取相应数据集的相应字段信息。


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

* **datasetinfos**

    * 描述：数据集信息配置，以确定哪些数据集将被读取。<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **server**

	* 描述：数据库服务器名、文件名或服务地址。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **enginetype**

	* 描述：数据源连接的引擎类型。 <br />

	* 必选：是 <br />

	* 默认值：219 SuperMap的UDB引擎 <br />

* **driver**

	* 描述：数据源连接所需的驱动名称。<br />

	* 必选：不一定，对于需要驱动的数据库而言必须选，其他数据库则不选 <br />

	* 默认值：无 <br />

* **database**

	* 描述：数据源连接的数据库名。

	* 必选：不一定，对于需要数据库名的数据库而言必须选，其他数据库则不选 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据库的用户名。

	* 必选：不一定，对于需要用户名的数据库而言必须选，其他数据库则不选 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源连接的数据库或文件的密码。<br />

	* 必选：不一定，对于需要密码的数据库或文件而言必须选，其他数据库则不选 <br />

	* 默认值：无 <br />

* **datasetname**

	* 描述：数据源中数据集的名称，二维点线面或属性表数据集的名称 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **fieldnames**

	* 描述：数据源中数据集的字段名称数组。<br />

	* 必选：是 <br />

	* 默认值：*表示全部字段 <br />


### 3.3 类型转换

目前SDXReader支持大部分SuperMap字段类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出SDXReader针对SuperMap字段类型转换列表:


| DataX 内部类型| SuperMap 字段类型    |
| -------- | -----  |
| Long     |BYTE,INT16,INT32,INT64|
| Double   |SINGLE,DOUBLE|
| String   |CHAR,TEXT,WTEXT,JSONB|
| Date     |DATETIME|
| Boolean  |BOOLEAN|
| Bytes    |LONGBINARY,GEOMETRY|


请注意:

* `除上述罗列字段类型外，其他类型均不支持`。


## 4 性能报告

略

## 5 约束限制

略

## 6 FAQ

略
