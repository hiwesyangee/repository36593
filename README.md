# repository36593
1.实现HBase评分推荐表基于现有测试数据集的推荐结果初始化。
2.经过固定每2秒访问一次网址，向集群发送一次带用户行为数据的文件，并通过监控目录，收集生成文件中的所有数据进行计算和分析，实现向HBase评分推荐表中更新数据
3.设置固定参数进行建模，测试计算时间，暂未加入最佳参数计算函数。
4.暂未测试数据库查询速度。
5.基于本地化进行计算，基本实现准实时更新（30s收集数据并进行计算）。
