# Air Quality Kafka Pipeline Project
Real-time data streaming with Apache Kafka and environmental time series analysis

### Data Preprocessing
The Air Quality dataset contained several preprocessing steps. Below is a summary of actions taken to clean and prepare the data:

1. Loading & Cleaning Raw Data
- The dataset was loaded using only the first 15 columns to exclude empty columns. 
- Columns "Date" and "Time" were merged into a single "Datetime" column and converted to proper datetime format. 
- The "NMHC(GT)" column was dropped due to a high proportion of missing values (greater than 90%), making it unreliable for future model training. 

2. Handling Missing Values (-200)
- All instances of -200 were replaced with np.nan to mark them as missing. 
- Long periods of missing data (greater than three consecutive missing rows with any missing values) were interpreted as likely sensor downtime and were removed from the dataset to avoid training on unreliable chunks of data. Shorter gaps (1-3 consecutive missing rows with any missing values) were kept and interpolated using time-based linear interpolation. 

3. Standardizing Data
- Column names were cleaned and standardized to lowercase, underscore-separated formats for readability. 
- All numerical values (float64 type) were standardized to scalar type. 

### Feature Engineering
To prepare the air quality dataset for time series modeling, I engineered features that capture both temporal patterns and historical trends. I extracted time-based features including Hour, Day, Month, and Year to help models learn daily and seasonal cycles. To incorporate temporal dependencies, I created lagged features for CO, NOx, and Benzene concentrations at 1-hour, 24-hour (daily), and 168-hour (weekly) intervals. Additionally, I calculated rolling statistics — mean and standard deviation over 3-hour, 24-hour, and 168-hour windows — to represent recent trends and variability. These features provide temporal context and pollutant history, enhancing model performance in forecasting tasks.

### Challenges to Kakfa Setup
While setting up Apache Kafka on Windows, I encountered several challenges related to script execution, file paths, and command-line limitations. PowerShell did not recognize Kafka’s .bat startup scripts (zookeeper-server-start.bat, kafka-server-start.bat) unless they were explicitly prefixed with .backwards slash. Additionally, long directory paths caused syntax errors and “input line is too long” issues. To resolve this issue, I relocated Kafka to a simpler path (C:\kafka) and adjusted configuration files accordingly. These adjustments allowed me to successfully start ZooKeeper and Kafka, create topics, and verify message flow between producer and consumer terminals.

My Kafka topic is called "air-qual-test."

### Kafka & Model Integration
For real-time prediction, I integrated a trained Random Forest model into the Kafka-based data pipeline. The setup begins with the Kafka consumer script, where processed data is passed to the Random Forest model for prediction. After each prediction is generated, the result is combined with the original input data and stored in a list of records. These records are continuously written to a CSV file using pandas. This happens inside the message loop, so the file is updated in real time as new messages are consumed from Kafka. Each row in the CSV includes the original air quality data along with the predicted values, allowing for easy tracking, evaluation, and future analysis. 
