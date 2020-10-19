# NCHU.BigData

## 作業1
### K-means(MapReduce)

* centroid (要先預設放4個中心點)
```shell=
hadoop fs -put pm25.cluster.center.conf.txt pm25.cluster.center.conf.txt
```
* 上傳PM2.5數據集
```shell=
hadoop fs -put pm25.txt pm25.txt
```
* 執行運算
參數1(數據集) : pm25.txt
參數2(各群集中心點) : pm25.cluster.center.conf.txt
參數3(分群後結果) : kmeans
參數4(迭代5次) : 5

```shell=
hadoop jar output.jar Kmeans pm25.txt pm25.cluster.center.conf.txt kmeans 5
```