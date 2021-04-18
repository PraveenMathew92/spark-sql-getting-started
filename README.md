# OUTPUT

> dataframe.show();

+------+--------------------+------+------+----------+-----+-----+--------------------+
|errors|           histogram|layout|length|lessonType|speed| time|           timeStamp|
+------+--------------------+------+------+----------+-----+-----+--------------------+
|    10|[[32, 23, 0, 352]...|    us|   127|      auto|  141|53900|2021-01-14T16:32:...|
|    12|[[32, 22, 2, 295]...|    us|   122|      auto|  128|57245|2021-01-14T16:33:...|
|    17|[[32, 19, 0, 427]...|    us|   120|      auto|   90|80081|2021-01-14T17:24:...|
|    10|[[32, 22, 0, 373]...|    us|   126|      auto|  150|50365|2021-01-14T17:32:...|
|    18|[[32, 21, 0, 381]...|    us|   122|      auto|  120|60985|2021-01-14T17:33:...|
|    18|[[32, 23, 1, 295]...|    us|   123|      auto|  135|54638|2021-01-14T17:34:...|
|    18|[[32, 18, 1, 249]...|    us|   119|      auto|  134|53438|2021-01-14T17:35:...|
|    17|[[32, 22, 1, 200]...|    us|   122|      auto|  145|50485|2021-01-14T17:36:...|
|    19|[[32, 23, 2, 246]...|    us|   124|      auto|  143|52021|2021-01-14T17:37:...|
|    20|[[32, 24, 1, 223]...|    us|   124|      auto|  144|51820|2021-01-14T17:40:...|
|    16|[[32, 22, 1, 299]...|    us|   125|      auto|  133|56189|2021-01-14T17:40:...|
|    13|[[32, 19, 0, 233]...|    us|   124|      auto|  142|52311|2021-01-14T17:41:...|
|    11|[[32, 22, 0, 252]...|    us|   122|      auto|  171|42702|2021-01-14T17:42:...|
|    12|[[32, 20, 0, 200]...|    us|   121|      auto|  174|41678|2021-01-14T17:43:...|
|    15|[[32, 20, 2, 194]...|    us|   123|      auto|  162|45535|2021-01-14T17:44:...|
|    22|[[32, 20, 2, 222]...|    us|   120|      auto|  133|54254|2021-01-14T17:45:...|
|    18|[[32, 22, 1, 226]...|    us|   122|      auto|  150|48854|2021-01-14T17:45:...|
|    15|[[32, 22, 1, 369]...|    us|   124|      auto|  131|56762|2021-01-15T13:51:...|
|    13|[[32, 21, 0, 317]...|    us|   129|      auto|  142|54413|2021-01-15T13:52:...|
|    11|[[32, 21, 1, 365]...|    us|   121|      auto|  151|48094|2021-01-15T13:53:...|
+------+--------------------+------+------+----------+-----+-----+--------------------+
only showing top 20 rows

> dataframe.select("histogram").show();

+--------------------+
|           histogram|
+--------------------+
|[[32, 23, 0, 352]...|
|[[32, 22, 2, 295]...|
|[[32, 19, 0, 427]...|
|[[32, 22, 0, 373]...|
|[[32, 21, 0, 381]...|
|[[32, 23, 1, 295]...|
|[[32, 18, 1, 249]...|
|[[32, 22, 1, 200]...|
|[[32, 23, 2, 246]...|
|[[32, 24, 1, 223]...|
|[[32, 22, 1, 299]...|
|[[32, 19, 0, 233]...|
|[[32, 22, 0, 252]...|
|[[32, 20, 0, 200]...|
|[[32, 20, 2, 194]...|
|[[32, 20, 2, 222]...|
|[[32, 22, 1, 226]...|
|[[32, 22, 1, 369]...|
|[[32, 21, 0, 317]...|
|[[32, 21, 1, 365]...|
+--------------------+
only showing top 20 rows

> dataframe.printSchema();

root
|-- errors: long (nullable = true)
|-- histogram: array (nullable = true)
|    |-- element: struct (containsNull = true)
|    |    |-- charCode: long (nullable = true)
|    |    |-- hitCount: long (nullable = true)
|    |    |-- missCount: long (nullable = true)
|    |    |-- timeToType: long (nullable = true)
|-- layout: string (nullable = true)
|-- length: long (nullable = true)
|-- lessonType: string (nullable = true)
|-- speed: long (nullable = true)
|-- time: long (nullable = true)
|-- timeStamp: string (nullable = true)

> dataframe.createOrReplaceTempView("dataframeView");
> sparkSession.sql("SELECT speed FROM dataframeView").show();

+-----+
|speed|
+-----+
|  141|
|  128|
|   90|
|  150|
|  120|
|  135|
|  134|
|  145|
|  143|
|  144|
|  133|
|  142|
|  171|
|  174|
|  162|
|  133|
|  150|
|  131|
|  142|
|  151|
+-----+
only showing top 20 rows

> dataframe.select(explode(col("histogram"))).show();

+-----------------+
|              col|
+-----------------+
| [32, 23, 0, 352]|
|[101, 39, 3, 328]|
|[105, 13, 1, 392]|
|[108, 10, 1, 305]|
|[110, 16, 2, 343]|
|[114, 10, 1, 401]|
|[116, 16, 2, 598]|
| [32, 22, 2, 295]|
|[101, 27, 2, 383]|
|[105, 15, 3, 383]|
|[108, 10, 0, 369]|
|[110, 12, 0, 226]|
| [114, 6, 3, 622]|
|[116, 30, 2, 328]|
| [32, 19, 0, 427]|
|[101, 25, 2, 446]|
|[105, 19, 2, 412]|
| [108, 7, 2, 206]|
|[110, 13, 1, 150]|
|[114, 6, 1, 1020]|
+-----------------+
only showing top 20 rows
