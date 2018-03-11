# create row number partitioned by group
from pyspark.sql.window import *
zip9_order = hou_1617_zip8.groupby('date', 'driver', 'zip9').agg(sf.min('time').alias('min_time')) \
            .withColumn("order", sf.row_number().over(Window.partitionBy('date', 'driver').orderBy('min_time')))
            
# autocorrelation
tmp['order'].autocorr(lag=1)

# pandas column correlation
order_pd['day1'].corr(order_pd['day2'], method='pearson')

# pandas ols (auto regression here)
import pandas as pd
import statsmodels.formula.api as sm
day1 = order_new[:len(order_new)-1]
day2 = order_new[1:]
order_pd = pd.DataFrame({'day1' : day1, 'day2' : day2})
result = sm.ols(formula="day2 ~ day1", data=order_pd).fit()
result.summary()

# get pandas column to array
order_new = order_filtered['order'].values.reshape(1, 375)[0]

# pandas drop first row
order = order.drop(order.index[0])

#pyspark day_of_year
.withColumn('dt_dayofy', sf.dayofyear('date'))

# jupyter plot
tmp_order = zip9_order.where(col('zip9') == '77070-4053').select('order', 'date')
tmp_order.registerTempTable('tmp_order')
---------------------------------------------------------------------------------
%%sql -o tmp_order
select date, order from tmp_order
---------------------------------------------------------------------------------
%matplotlib inline
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.plot(tmp_order['date'], tmp_order['order'])
ax.set(xlabel='date', ylabel='order',
       title='77070-4053')
plt.show()
