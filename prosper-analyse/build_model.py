from autogluon.tabular import TabularDataset, TabularPredictor
from pandas import read_csv
from os import path
import logging

logger = logging.getLogger(__file__)

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s", level=logging.INFO
)

logger.info("Loading data...")
data_path = path.join(path.dirname(__file__), "..", "data", "results.csv")
df = read_csv(data_path,encoding='latin-1') #, index_col=['loan_origination_date','amount_funded','borrower_rate','listing_term'],header=0)

logger.info("Splitting data into train and test sets...")
train=df.sample(frac=0.8,random_state=200)
test=df.drop(train.index)
test_by_rating = test.groupby("prosper_rating")

logger.info("Importing data into autogluon...")
train_data = TabularDataset(train)

logger.info(f"First 5 rows: \n{train_data.head()}")

logger.info("Training model...")
predictor = TabularPredictor(label='scaled_return_rate', ).fit(train_data, time_limit=6000, )

logger.info("Evaluating model...")
report = predictor.evaluate(test, detailed_report=True)
logger.info(f"Report: \n{report}")

for rating, test_data in test_by_rating:
    logger.info(f"Rating: {rating}")
    report = predictor.evaluate(test_data, detailed_report=True)
    logger.info(f"Report: \n{report}\n")

