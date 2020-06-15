# Title



As part of a project evaluating the effect of Haifa's low emission zone (LEZ) using the synthetic control method (coming soon!), I ran a short exploration of the data. It's always beneficial to have a thorough look at the data before starting to model it. I think it's safe to say that any investment of time in exploratory data analysis will be returned at a later stage in terms of better modelling insights, less bugs and valid conclusions.

Disclaimer: I have no expertise in air quality analysis, in fact this is the first time I look at data collected from an air pollution monitoring system.

This post was generated from a Jupyter notebook, which can be downloaded from [here](https://github.com/tom-beer/Air-Pollution). The notebook goes through the following steps:
- Dataset cleaning and tidying
- Handling of missing values
- Analyzing trends of time series (daily, weekly, annual etc)
- Finding correlations between pairs of variables
- Data fusion with meteorological dataset

At a later stage, these sections will be added:
- Simple air pollution modelling
- Validation of the inference conducted for the ministry regarding the effect of Haifa's LEZ.

### Dataset sources
- Air quality monitoring data was downloaded from the website of Israel's Ministry of Environmental Protection. The interface can be found [here](https://www.svivaaqm.net/).
- Temperature data was downloaded from Israel Meteorological Service's website. The interface can be found [here](https://ims.data.gov.il/ims/1).

The raw data looks like this:

```python
df_aqm = pd.read_csv('Data/Haifa-Atzmaut.csv')
df_aqm
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>תחנה:עצמאות חיפה -</th>
      <th>SO2</th>
      <th>Benzene</th>
      <th>PM2.5</th>
      <th>PM10</th>
      <th>CO</th>
      <th>NO2</th>
      <th>NOX</th>
      <th>NO</th>
      <th>תאריך  \ שעה</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td></td>
      <td>ppb</td>
      <td>ppb</td>
      <td>µg/m³</td>
      <td>µg/m³</td>
      <td>ppm</td>
      <td>ppb</td>
      <td>ppb</td>
      <td>ppb</td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td></td>
      <td></td>
      <td></td>
      <td>29.7</td>
      <td>44.4</td>
      <td>0.6</td>
      <td>41.2</td>
      <td>114.8</td>
      <td>73.8</td>
      <td>31/12/2011 24:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td></td>
      <td></td>
      <td></td>
      <td>30.6</td>
      <td>45.5</td>
      <td>0.6</td>
      <td>35.6</td>
      <td>81.1</td>
      <td>45.6</td>
      <td>01/01/2012 00:05</td>
    </tr>
    <tr>
      <th>3</th>
      <td></td>
      <td></td>
      <td></td>
      <td>30.6</td>
      <td>45.5</td>
      <td>0.5</td>
      <td>40.7</td>
      <td>107.9</td>
      <td>67.0</td>
      <td>01/01/2012 00:10</td>
    </tr>
    <tr>
      <th>4</th>
      <td></td>
      <td></td>
      <td></td>
      <td>30.2</td>
      <td>45.1</td>
      <td>0.5</td>
      <td>41.7</td>
      <td>127.9</td>
      <td>86.2</td>
      <td>01/01/2012 00:15</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>883289</th>
      <td></td>
      <td>23/04/2020 02:15</td>
      <td>31/03/2014 22:00</td>
      <td>02/03/2014 17:00</td>
      <td>07/02/2012 14:30</td>
      <td>06/01/2020 13:40</td>
      <td>18/01/2017 13:05</td>
      <td>22/01/2015 07:30</td>
      <td>22/01/2015 07:30</td>
      <td>תאריך מקסימלי</td>
    </tr>
    <tr>
      <th>883290</th>
      <td></td>
      <td>1.7</td>
      <td>0.24</td>
      <td>19.8</td>
      <td>49.7</td>
      <td>0.4</td>
      <td>20.8</td>
      <td>42.6</td>
      <td>21.8</td>
      <td>Avg</td>
    </tr>
    <tr>
      <th>883291</th>
      <td></td>
      <td>237295</td>
      <td>602180</td>
      <td>800202</td>
      <td>97432</td>
      <td>844796</td>
      <td>827176</td>
      <td>826981</td>
      <td>826945</td>
      <td>כמות ערכים</td>
    </tr>
    <tr>
      <th>883292</th>
      <td></td>
      <td>27</td>
      <td>68</td>
      <td>91</td>
      <td>11</td>
      <td>96</td>
      <td>94</td>
      <td>94</td>
      <td>94</td>
      <td>נתון[%]</td>
    </tr>
    <tr>
      <th>883293</th>
      <td></td>
      <td>2.8</td>
      <td>0.3</td>
      <td>17.8</td>
      <td>50.0</td>
      <td>0.2</td>
      <td>16.3</td>
      <td>58.6</td>
      <td>46.0</td>
      <td>סטיית תקן</td>
    </tr>
  </tbody>
</table>
<p>883294 rows × 10 columns</p>
</div>



### Dataset Cleaning 

The following steps are needed to clean the dataset:
- Delete the first column, which contains only the station's name
- Strip the column names of leading and trailing whitespaces
- Translate the Hebrew date/time column
- Replace the representation of midnight from 24:00 to 00:00 to comply with Python's DateTime
- Drop the first row, which contains the units of measurements for the variables
- Drop the last 8 rows, which contain summary statistics
- Numerify the measurements, setting missing values to NaN
- Set the datetime variable as index

```python
station_name = df_aqm.columns[0]
df_aqm.drop(columns=[station_name], inplace=True)
df_aqm.rename(columns={colname: colname.strip() for colname in df_aqm.columns}, inplace=True)
df_aqm.rename(columns={df_aqm.columns[-1]: 'DateTime'}, inplace=True)
df_aqm['DateTime'] = df_aqm['DateTime'].apply(lambda x: x.strip().replace('24:00', '00:00'))
pollutants = list(df_aqm)
pollutants.remove('DateTime')
units = {colname: df_aqm.loc[0, colname].strip() for colname in pollutants}
df_aqm = df_aqm.iloc[1:-8]
df_aqm[pollutants] = df_aqm[pollutants].apply(pd.to_numeric, errors='coerce')
df_aqm['DateTime'] = pd.to_datetime(df_aqm['DateTime'],infer_datetime_format=True)
df_aqm = df_aqm.set_index('DateTime')

print(f'The dataset spans {len(np.unique(df_aqm.index.date))} days,' 
      f'from {df_aqm.index[0].date()} to {df_aqm.index[-1].date()}')
```

    The dataset spans 3068 days,from 2011-12-31 to 2020-05-24


Let's have a look at a 2-month slice of the cleaned dataset:

```python
df_aqm.loc["2018-04":"2018-05"]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SO2</th>
      <th>Benzene</th>
      <th>PM2.5</th>
      <th>PM10</th>
      <th>CO</th>
      <th>NO2</th>
      <th>NOX</th>
      <th>NO</th>
    </tr>
    <tr>
      <th>DateTime</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2018-04-01 00:05:00</th>
      <td>1.8</td>
      <td>0.18</td>
      <td>7.4</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>22.2</td>
      <td>23.6</td>
      <td>1.4</td>
    </tr>
    <tr>
      <th>2018-04-01 00:10:00</th>
      <td>2.0</td>
      <td>0.18</td>
      <td>9.5</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>26.3</td>
      <td>27.9</td>
      <td>1.5</td>
    </tr>
    <tr>
      <th>2018-04-01 00:15:00</th>
      <td>NaN</td>
      <td>0.18</td>
      <td>13.7</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>24.6</td>
      <td>25.2</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>2018-04-01 00:20:00</th>
      <td>NaN</td>
      <td>0.18</td>
      <td>16.8</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>25.0</td>
      <td>25.9</td>
      <td>0.9</td>
    </tr>
    <tr>
      <th>2018-04-01 00:25:00</th>
      <td>NaN</td>
      <td>0.18</td>
      <td>17.4</td>
      <td>NaN</td>
      <td>0.4</td>
      <td>24.2</td>
      <td>24.7</td>
      <td>0.6</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2018-05-31 23:40:00</th>
      <td>0.0</td>
      <td>0.08</td>
      <td>8.0</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>7.2</td>
      <td>7.5</td>
      <td>0.5</td>
    </tr>
    <tr>
      <th>2018-05-31 23:45:00</th>
      <td>0.0</td>
      <td>0.08</td>
      <td>7.5</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>6.7</td>
      <td>7.1</td>
      <td>0.2</td>
    </tr>
    <tr>
      <th>2018-05-31 23:50:00</th>
      <td>0.0</td>
      <td>0.08</td>
      <td>6.4</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>10.2</td>
      <td>11.2</td>
      <td>1.2</td>
    </tr>
    <tr>
      <th>2018-05-31 23:55:00</th>
      <td>0.0</td>
      <td>0.08</td>
      <td>6.6</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>9.1</td>
      <td>10.1</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>2018-05-31 00:00:00</th>
      <td>0.0</td>
      <td>0.08</td>
      <td>9.8</td>
      <td>NaN</td>
      <td>0.3</td>
      <td>7.7</td>
      <td>7.4</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
<p>17568 rows × 8 columns</p>
</div>



Not bad! Nice and tidy. 

**Some notes about the data**
- Measurements are recorded at 5-minute intervals. That's a very high resolution, much more than needed to answer the research question. But it'll be very helpful - it will allow for aggressive smoothing of the noisy measurements. In total, we have almost 1 million measurements of each pollutant.
- From the slice above we can suspect that the measurement resolution for CO (Carbon Monoxide) might be small compared to its distribution. It's easy to check:

```python
df_aqm['CO'].value_counts(normalize=True).head(7)
```




    0.3    0.338305
    0.2    0.186482
    0.4    0.182812
    0.5    0.073091
    0.1    0.051982
    0.6    0.039140
    0.0    0.035468
    Name: CO, dtype: float64



And indeed more than 70% of the CO measurements fall in one of 3 distinct values. This implies that the CO measurement is not accurate enough in capturing this pollutant's distribution. We should keep that in mind when drawing inferences based on CO.

Moving on to a general overview of the data, let's use the convenient 'describe' method:

```python
df_aqm.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SO2</th>
      <th>Benzene</th>
      <th>PM2.5</th>
      <th>PM10</th>
      <th>CO</th>
      <th>NO2</th>
      <th>NOX</th>
      <th>NO</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>237295.000000</td>
      <td>602180.000000</td>
      <td>800202.000000</td>
      <td>97432.000000</td>
      <td>844796.000000</td>
      <td>827176.000000</td>
      <td>826981.000000</td>
      <td>826945.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>1.718825</td>
      <td>0.242009</td>
      <td>19.774215</td>
      <td>49.704957</td>
      <td>0.366657</td>
      <td>20.821325</td>
      <td>42.579953</td>
      <td>21.816400</td>
    </tr>
    <tr>
      <th>std</th>
      <td>2.763984</td>
      <td>0.300786</td>
      <td>17.774355</td>
      <td>49.961126</td>
      <td>0.249257</td>
      <td>16.310098</td>
      <td>58.644492</td>
      <td>46.070798</td>
    </tr>
    <tr>
      <th>min</th>
      <td>-0.800000</td>
      <td>-0.010000</td>
      <td>-29.500000</td>
      <td>-10.100000</td>
      <td>-0.400000</td>
      <td>-4.800000</td>
      <td>-4.600000</td>
      <td>-5.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>0.100000</td>
      <td>0.060000</td>
      <td>10.800000</td>
      <td>24.000000</td>
      <td>0.200000</td>
      <td>7.700000</td>
      <td>8.500000</td>
      <td>0.300000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>0.800000</td>
      <td>0.150000</td>
      <td>17.200000</td>
      <td>37.500000</td>
      <td>0.300000</td>
      <td>15.400000</td>
      <td>19.300000</td>
      <td>3.300000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>2.400000</td>
      <td>0.320000</td>
      <td>25.400000</td>
      <td>59.400000</td>
      <td>0.400000</td>
      <td>31.900000</td>
      <td>51.300000</td>
      <td>18.400000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>136.200000</td>
      <td>12.700000</td>
      <td>562.000000</td>
      <td>810.900000</td>
      <td>23.400000</td>
      <td>262.400000</td>
      <td>865.500000</td>
      <td>763.800000</td>
    </tr>
  </tbody>
</table>
</div>



We can immediately notice negative measurements for all pollutants. This is not a good sign, because while this can be easily addressed and fixed, it increases our suspicion of more serious problems in the dataset that would be difficult to identify.

But that will be addressed later. Now we have to solve the issue of negative measurements. Let's first understand the extent of the problem:

```python
percent_negative = 100*(df_aqm[pollutants] < 0).mean(axis=0).values
print(f'{pollutants[percent_negative.argmax()]} has {percent_negative.max():.1f}% negative values')
```

    NO has 8.8% negative values


That's quite bad. Two of the simplest ways to fix this issue are:
- Clip the negative values to 0
- Replace them with NaNs

However, these two approaches may or may not be the best thing to do, depending on:
- The mechanism that has caused this issue
- The quality of data required to answer the research question

Since I don't have a substantiated answer to any of these questions, I tried to find out what domain practitioners recommend on doing. [Here](https://www.researchgate.net/post/How_can_I_deal_with_negative_and_zero_concentrations_of_PM25_PM10_and_gas_data) is a thread with some opinions and insights on the matter. The authors propose to differentiate between two situations, and act accordingly:
- If the datapoints exhibit sudden short drops below 0, this probably indicates an instrument malfunction. In this case, it is advised to replace the invalid value with an average of some neighborhood.
- If consecutive values below 0 are recorded for long times compared to the instrument's integration time, this is probably rooted in the true air conditions. In this case, negative values imply that the real value was below the instrument's detection limit. The suggested approach would be to replace the negative values with the known detection limit, or 0 if it isn't known (like in our case). More justifications for this approach can be found in [Polissar et al, 1998](https://agupubs.onlinelibrary.wiley.com/doi/pdf/10.1029/98JD01212)

If we want to follow these guidelines, we need to decide for each negative measurement if it the former case or the latter. This will be coded at a later stage.

### Checking for systematic data collection flaws
It is important to make sure that our dataset does not have any systematic flaws in the data collection process.
For example, later we would like to demonstrate the fluctuations of pollutants as function of the weekday.
In this case, if some days are recorded significantly less often than other days, any conclusion we would like to state as for the weekday trend might be invalidated.

An easy way to check this is to print the value counts of the weekdays:

```python
df_aqm['Weekday Name'] = df_aqm.index.day_name()
df_aqm['Weekday Name'].value_counts()
```




    Sunday       126420
    Saturday     126145
    Friday       126144
    Monday       126144
    Wednesday    126144
    Thursday     126144
    Tuesday      126144
    Name: Weekday Name, dtype: int64



We see that the dataset is pretty much balanced in terms of weekday logging, and that is reassuring.
However, it could also be that the logging system simply adds an entry for every 5-minute interval, while the measurement instruments malfunction with some systematic (i.e not at random) pattern. A complete examination would be:

```python
df_aqm[pollutants].isna().groupby(df_aqm['Weekday Name']).mean()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SO2</th>
      <th>Benzene</th>
      <th>PM2.5</th>
      <th>PM10</th>
      <th>CO</th>
      <th>NO2</th>
      <th>NOX</th>
      <th>NO</th>
    </tr>
    <tr>
      <th>Weekday Name</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Friday</th>
      <td>0.731030</td>
      <td>0.271008</td>
      <td>0.100235</td>
      <td>0.892718</td>
      <td>0.040160</td>
      <td>0.061295</td>
      <td>0.061430</td>
      <td>0.061430</td>
    </tr>
    <tr>
      <th>Monday</th>
      <td>0.731204</td>
      <td>0.394097</td>
      <td>0.094646</td>
      <td>0.887097</td>
      <td>0.046114</td>
      <td>0.067835</td>
      <td>0.068121</td>
      <td>0.068398</td>
    </tr>
    <tr>
      <th>Saturday</th>
      <td>0.730992</td>
      <td>0.344128</td>
      <td>0.100820</td>
      <td>0.894621</td>
      <td>0.043846</td>
      <td>0.064632</td>
      <td>0.064767</td>
      <td>0.064767</td>
    </tr>
    <tr>
      <th>Sunday</th>
      <td>0.734813</td>
      <td>0.313724</td>
      <td>0.100791</td>
      <td>0.884710</td>
      <td>0.046757</td>
      <td>0.060141</td>
      <td>0.060354</td>
      <td>0.060354</td>
    </tr>
    <tr>
      <th>Thursday</th>
      <td>0.728350</td>
      <td>0.297026</td>
      <td>0.093805</td>
      <td>0.892662</td>
      <td>0.043442</td>
      <td>0.062254</td>
      <td>0.062571</td>
      <td>0.062579</td>
    </tr>
    <tr>
      <th>Tuesday</th>
      <td>0.733376</td>
      <td>0.305429</td>
      <td>0.084047</td>
      <td>0.886312</td>
      <td>0.043363</td>
      <td>0.069159</td>
      <td>0.069413</td>
      <td>0.069413</td>
    </tr>
    <tr>
      <th>Wednesday</th>
      <td>0.729674</td>
      <td>0.302345</td>
      <td>0.084071</td>
      <td>0.889745</td>
      <td>0.041334</td>
      <td>0.059353</td>
      <td>0.059559</td>
      <td>0.059559</td>
    </tr>
  </tbody>
</table>
</div>



We see that the fraction of missing data changes significantly from one pollutant to another. In addition, for each pollutant, the fraction of missing values changes per weekday, but for now it's hard to say if these differences are consequential. Meanwhile, it's good to keep this table in mind, and we might return to it depending on the analysis and inference that we would like to conduct.

So we can conclude this step with the reassurance that the data does not exhibit major systematic data collection flaws, and we can move on in to the analysis. Let's plot the data in its most raw form - just a scatterplot of every datapoint:

```python
axes=df_aqm[pollutants].plot(marker='.', alpha=0.5, linestyle='None', figsize=(11, 11), subplots=True, colormap='Dark2');
```


![png](Exploration_files/output_19_0.png)


We can notice a few things from this plot:
- PM10 and SO2 are not continuously monitored along the 8 year period. Plots and analyses in the following will not ignore them, as we can still draw some conclusions using these limited measurements
- We can see the first hint of a seasonal trend: it seems like NOx and NO follow an annual trend. Because of the scatterplot's density it could also be that only a few outliers paint a picture that looks like an annual trend, while in reality they do not contribute a significant amount to the total trend. Therefore a more detailed analysis is required to validate this claim
- It seems like NO2 and NOx are identical (or almost identical measurements). Reading about [NOx](https://en.wikipedia.org/wiki/NOx) it seems rather amusing, as NOx is defined as the sum the nitrogen oxides NO and NO2. But it is the visualization that is misleading, and a closer look reveals that NOx is always higher than NO2.

Let's see if on average there is a real annual trend for some substances:

```python
log_interval = 5  # minutes
samples_per_week = int(60 / log_interval * 24 * 7)
window = samples_per_week*4
overlap = int(2*samples_per_week)
df_aqm.rolling(
    window=window, center=False, min_periods=samples_per_week).mean()[samples_per_week*2::overlap].plot(
    marker='.', alpha=0.5, linestyle='None', figsize=(11, 11), subplots=True, colormap='Dark2');
```


![png](Exploration_files/output_21_0.png)


This plot shows rolling averages of the measurements, with windows of 4 weeks and an overlap of 2 weeks. A similar result could be obtained using a simple groupby with a monthly frequency:

`df_aqm.groupby(pd.Grouper(freq='M')).mean()`

But the overlapping windows add a degree of smoothing which is (arguably) preferable in this case.

If you are disappointed too with pandas' lack of an overlap parameter in the rolling function, you can contribute to the discussion [here](https://github.com/pandas-dev/pandas/issues/15354).

Anyway, enough with the ranting. The above plot shows that all nitrogen oxide measurements (and also benzene to a lesser extent) follow a clear annual trend peaking at winter. It is still not clear what's the cause of this modulation, and we will revisit it later in the analysis. 

The same trends could be demonstrated using a different visualization, plotting the 8 year daily averages of every pollutants.

```python
df_aqm.groupby(df_aqm.index.dayofyear).mean().plot(marker='.', alpha=0.5, linestyle='None', figsize=(11, 11), subplots=True, colormap='Dark2');
plt.xlabel('Day of the Year');
```


![png](Exploration_files/output_23_0.png)


The annual trend of NO, NO2, NOx and benzene is validated through this plot. In addition, we can notice a peculiar drop in CO values around day 240. A closer look reveals that this drop is at day 243. This day is August 31st, the last day of summer vacation for 1.5 million kids. Coincidence? I don't know. What do you think?

Let's move now to a weekly analysis:

```python
ax = sns.boxplot(data=df_aqm, x='Weekday Name', y='NOX', 
                 order=['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'], 
                 palette=sns.cubehelix_palette(7, start=.5, rot=-.75, dark=0.3, reverse=True)).set_ylim([0,70]);
```


![png](Exploration_files/output_25_0.png)


For NOx, there is a clear weekly trend in the amount of air pollution measured. Note that the outliers have been cropped out of the plot. For other pollutants, the trend is not as impressive:

```python
ax = sns.boxplot(data=df_aqm, x='Weekday Name', y='CO', 
                 order=['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'], 
                 palette=sns.cubehelix_palette(7, start=.5, rot=-.75, dark=0.3, reverse=True)).set_ylim([0,1]);
```


![png](Exploration_files/output_27_0.png)


And we also see the discrete nature of the CO measurements, as noted earlier. Another way to look at the weekly flactations would be to plot the raw measurements for a specific period of time:

```python
df_sub = df_aqm.loc['2019-07':'2019-09'] 
fig, ax = plt.subplots(figsize=(20,5))
ax.plot(df_sub.groupby(df_sub.index.date).median()['NOX'], marker='o', linestyle='-')
ax.set_ylabel('NOx [ppb]')
ax.set_title('')
ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.SATURDAY))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'));
```


![png](Exploration_files/output_29_0.png)


For this 3-month period, the lowest measured values were always on Saturdays, agreeing with the boxplot above.

The third and last seasonal trend to be analyzed is the dependence on the hour of the day.

```python
df_aqm.groupby(df_aqm.index.time).mean().plot(marker='.', alpha=0.5, linestyle='None', figsize=(11, 11), subplots=True, colormap='Dark2');
plt.xticks(["08:00","12:00", "16:00", "20:00", "00:00", "04:00"]);
```


![png](Exploration_files/output_31_0.png)


Some notes about this plot:
- Most pollutants peak in the morning. Some right at the rush hour (CO, NO, NO2, NOX, Benzene) and some a bit later (SO2, PM10)
- It doesn't seem like PM2.5 adheres to any logical pattern.
- CO peaks around midnight. It seems as it has a slow buildup process, followed by a slow decreasing phase.
- If nitrogen oxides are caused mainly by transportation, then the rush hour attribution above could make sense. In this case, we could also attribute the slow increase and plateau in the afternoon/evening to the fact (or assumption?) that commute back home is spread on longer hours, compared to the narrow rush hour.
- I'm no expert on the sources of each pollutant, be it transportation, industry, the port or natural factors. So I don't know if any of these hypotheses make any sort of sense.


## Air pollution and temperatures

The above trends show a clear correlation between the season/month and the average measured pollutants (at least for nitrogen oxides and benzene). But what is the cause of this modulation - is it the time itself? Or maybe it is the temperature? There could be many potential causes, and estimating each of these effects is a difficult problem, according to the identification strategies of causal inference. Instead, we can choose to stay in the realm of mere correlations, and while they do not imply causation, we could speculate what is a reasonable claim and what is not.

A simple examination would be to evaluate how much of this modulation is governed by the change in temperature (disregarding the day of the year). To this end, I downloaded a second dataset, this time from Israeli Meteorological Service. It includes daily measurements (low and high temperatures) from the meteorological station closest to the air quality monitoring station, situated at BAZAN Group's plant in Haifa.

A straightforward cleaning yields a dataset that looks like this:

```python
df_temp = pd.read_csv('Data/ims_data.csv', sep=",", encoding="ISO-8859-8")
# translate df from Hebrew
df_temp = df_temp.rename(columns={'משתנה': 'variable', 'תאריך': 'DateTime', 'חיפה בתי זיקוק                                    (600)': 'value'})
df_temp.loc[df_temp['variable'] == 'טמפרטורת מינימום(C°)', 'variable'] = 'min temp'
df_temp.loc[df_temp['variable'] == 'טמפרטורת מקסימום(C°)', 'variable'] = 'max temp'
df_temp.loc[df_temp['variable'] == 'טמפרטורת מינימום ליד הקרקע(C°)', 'variable'] = 'ground temp'

# Delete the record 'ground temp' (first make sure it's empty)
assert(df_temp.loc[df_temp['variable'] == 'ground temp','value'].values == '-').all()
df_temp = df_temp.drop(df_temp[df_temp['variable'] == 'ground temp'].index)

# Convert to datetime
df_temp['DateTime'] = pd.to_datetime(df_temp['DateTime'],infer_datetime_format=True)

df_temp['value'] = df_temp['value'].apply(pd.to_numeric, errors='coerce')

df_temp.tail()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>variable</th>
      <th>DateTime</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>8638</th>
      <td>min temp</td>
      <td>2020-04-27</td>
      <td>11.6</td>
    </tr>
    <tr>
      <th>8640</th>
      <td>max temp</td>
      <td>2020-04-28</td>
      <td>23.2</td>
    </tr>
    <tr>
      <th>8641</th>
      <td>min temp</td>
      <td>2020-04-28</td>
      <td>11.0</td>
    </tr>
    <tr>
      <th>8643</th>
      <td>max temp</td>
      <td>2020-04-29</td>
      <td>22.9</td>
    </tr>
    <tr>
      <th>8644</th>
      <td>min temp</td>
      <td>2020-04-29</td>
      <td>17.1</td>
    </tr>
  </tbody>
</table>
</div>



Let's plot a histogram of the daily lows and highs, just to see that the dataset makes sense (it does):

```python
g = sns.FacetGrid(df_temp, hue='variable', height=6)
g.map(sns.distplot, 'value', bins=30, hist=True, kde=False, hist_kws={"linewidth": 3, "alpha": 0.7})
g.add_legend(title=' ')
g.axes[0,0].set_title("Distribution of minimal and maximal temperatures")
g.axes[0,0].set_xlabel('Temperature [Celsius]');

```


![png](Exploration_files/output_36_0.png)


To merge (fuse?) the datasets, we need to calculate an aggregate value for the pollutants for each day (because temperatures are measured only once a day). Averaging the measurements of each day is the simplest thing to do.

```python
df_pol_day = df_aqm.groupby(df_aqm.index.date).mean()
df_pol_day.index = pd.to_datetime(df_pol_day.index)

df_max = df_temp.loc[df_temp['variable'] == 'max temp', ['DateTime', 'value']]
df_max.rename(columns={'value': 'max temp'}, inplace=True)
df_max.set_index('DateTime', inplace=True, drop=True)

df_min = df_temp.loc[df_temp['variable'] == 'min temp', ['DateTime', 'value']]
df_min.rename(columns={'value': 'min temp'}, inplace=True)
df_min.set_index('DateTime', inplace=True, drop=True)

df_pol_temp = df_pol_day.merge(right=df_max, how='inner', left_index=True, right_index=True)
df_pol_temp = df_pol_temp.merge(right=df_min, how='inner', left_index=True, right_index=True)
df_pol_temp.tail()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>SO2</th>
      <th>Benzene</th>
      <th>PM2.5</th>
      <th>PM10</th>
      <th>CO</th>
      <th>NO2</th>
      <th>NOX</th>
      <th>NO</th>
      <th>max temp</th>
      <th>min temp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2020-04-25</th>
      <td>0.031802</td>
      <td>0.049213</td>
      <td>10.824653</td>
      <td>NaN</td>
      <td>0.279152</td>
      <td>1.761702</td>
      <td>1.509220</td>
      <td>0.010284</td>
      <td>22.3</td>
      <td>17.1</td>
    </tr>
    <tr>
      <th>2020-04-26</th>
      <td>0.089753</td>
      <td>0.036250</td>
      <td>9.450694</td>
      <td>NaN</td>
      <td>0.295406</td>
      <td>6.074823</td>
      <td>7.617376</td>
      <td>1.686170</td>
      <td>23.0</td>
      <td>16.1</td>
    </tr>
    <tr>
      <th>2020-04-27</th>
      <td>0.217314</td>
      <td>0.079306</td>
      <td>12.359028</td>
      <td>NaN</td>
      <td>0.310247</td>
      <td>18.504255</td>
      <td>28.706383</td>
      <td>10.386525</td>
      <td>23.0</td>
      <td>11.6</td>
    </tr>
    <tr>
      <th>2020-04-28</th>
      <td>0.595760</td>
      <td>0.091632</td>
      <td>13.841319</td>
      <td>NaN</td>
      <td>0.332155</td>
      <td>17.207801</td>
      <td>26.422340</td>
      <td>9.412057</td>
      <td>23.2</td>
      <td>11.0</td>
    </tr>
    <tr>
      <th>2020-04-29</th>
      <td>0.102827</td>
      <td>0.020035</td>
      <td>10.708333</td>
      <td>NaN</td>
      <td>0.260777</td>
      <td>1.787943</td>
      <td>1.271986</td>
      <td>0.000000</td>
      <td>22.9</td>
      <td>17.1</td>
    </tr>
  </tbody>
</table>
</div>



```python
sns.jointplot(x='min temp', y='NOX', data=df_pol_temp, kind="reg", stat_func=corr_coef);
warnings.filterwarnings(action='default')
```


![png](Exploration_files/output_39_0.png)


With a significant correlation of -0.58, the measured NOx values can be explained to some degree by the temperatures. It would be interesting to see if other meteorological factors, like humidity and wind, can explain even more of the NOx fluctuations.

## Closing remarks
This notebook described a preliminary exploration of the air quality monitoring data in Haifa. It is by no means an exhaustive analysis, it is just a set of steps used to increase the understanding of the dataset. If you have any comment - an analysis that you think should be done, a peculiar result that should be examined or anything else - I would love to know!

This is not a conclusion, as it is only the start of the analysis. In the next chapter, I will evaluate the causal effect of Haifa's new low emission zone using the synthetic control method. Stay tuned!
