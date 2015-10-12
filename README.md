#HBase api 练习

## maven
        <maven.compiler.source>1.7</maven.compiler.source>
        <maven.compiler.target>1.7</maven.compiler.target>
## 注意事项
*   开发环境IntelliJ IDEA 14.1.5 需要设置语言级别 7.0 - Diamonds, ARM, multi-catch, etc.
*   JDK 1.7.+
*   maven 需要指定

## 参考API地址

        http://apix.cn/services/show/19
        http://apix.cn/services/show/20
## hbase 数据模型
        手机归属地查询
        表名称: phone ; rowkey: 电话号码前7位; 列簇： cf; 列：province, city, operator
        根据城市名称查询天气
        表名称: weatherjson; rowkey: 城市名称+“；”+日期; 列簇: def; 列：wind_power, post_code, high_temp, temp, time, weather, low_temp, wind_direction, city_code
                        列簇: cf; 列： json
        具体调用细节请参考WebAPIServer类        
   
## 技术细节
        对json编码与解码是时使用的是org.json库，JSONHelper可以直接将json字符串转换为对象。
