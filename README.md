说明：
1.评价标签生成有很多方法：人工运营，机器抽取等，其中机器抽取需要比较复杂的nlp技术，此处假定每条评价的标签已经生成完成
2.在业务上展示评价标签时，需要通过对该商家的所有用户的评价标签进行聚合，然后设置一定的策略进行展示，此处仅仅通过标签的频次进行聚合
3.评价标签存储为json格式，需要进行解析，此处解析使用java脚本
4.运行时请把temptags.txt文件上传到hdfs上，并修改scala脚本中的path路径