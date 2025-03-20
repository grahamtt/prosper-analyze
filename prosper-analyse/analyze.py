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

logger.info("Loading model...")
predictor = TabularPredictor.load(path.join(path.dirname(__file__), "..", "AutogluonModels", "ag-20250318_232623"))

logger.info("Evaluating model...")
report = predictor.evaluate(test, detailed_report=True)
logger.info(f"Report: \n{report}")

for rating, test_data in test_by_rating:
    logger.info(f"Rating: {rating}")
    report = predictor.evaluate(test_data, detailed_report=True)
    logger.info(f"Report: \n{report}\n")

